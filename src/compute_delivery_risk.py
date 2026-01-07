import json
import sqlite3
from severity import classify_score
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "efct.db"


def now_local_iso(tz_offset_hours: int) -> str:
    # tz_offset_hours: e.g. -5 for Peru
    tz = timezone(datetime.now().astimezone().utcoffset())
    return datetime.now(tz).isoformat()


def load_cfg(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def in_window(local_hhmm: str, start: str, end: str) -> bool:
    # assumes same-day window; your window 16:00–23:00
    return start <= local_hhmm <= end


def get_latest_traffic(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        """
        SELECT ts_utc, current_speed_kmh, freeflow_speed_kmh
        FROM traffic_flow_snapshots
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    ts_utc, cur, ff = row
    if cur is None or ff is None or ff <= 0:
        return None
    ratio = cur / ff  # lower ratio => worse traffic
    return {"ts_utc": ts_utc, "current_speed_kmh": cur, "freeflow_speed_kmh": ff, "ratio": ratio}


def get_latest_weather(conn: sqlite3.Connection) -> dict | None:
    # Your table currently has raw_json (not payload_json)
    row = conn.execute(
        """
        SELECT ts_utc, raw_json
        FROM weather_snapshots
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    ts_utc, raw_json = row
    try:
        payload = json.loads(raw_json)
    except Exception:
        return None
    return {"ts_utc": ts_utc, "payload": payload}


def score_delivery_risk(traffic_ratio: float | None, rain_pop_3h: float | None) -> tuple[int, dict]:
    """
    score: 0 (best) -> 100 (worst)
    """
    score = 10
    reasons = {}

    # Traffic component
    if traffic_ratio is None:
        score += 10
        reasons["traffic"] = "Traffic missing; adding uncertainty."
    else:
        # ratio ~1.0 is freeflow, ~0.6 is very slow
        if traffic_ratio >= 0.90:
            score += 5
            reasons["traffic"] = "Traffic near freeflow."
        elif traffic_ratio >= 0.75:
            score += 15
            reasons["traffic"] = "Traffic moderately slower than freeflow."
        elif traffic_ratio >= 0.60:
            score += 30
            reasons["traffic"] = "Traffic significantly slower than freeflow."
        else:
            score += 45
            reasons["traffic"] = "Traffic heavily congested."

    # Rain component (probability of precipitation next ~3h)
    if rain_pop_3h is None:
        score += 5
        reasons["rain"] = "Rain outlook missing; adding uncertainty."
    else:
        if rain_pop_3h >= 0.60:
            score += 25
            reasons["rain"] = "High rain probability can slow last-mile delivery."
        elif rain_pop_3h >= 0.30:
            score += 15
            reasons["rain"] = "Moderate rain probability can increase delivery variability."
        elif rain_pop_3h >= 0.10:
            score += 5
            reasons["rain"] = "Low rain probability."
        else:
            reasons["rain"] = "Very low rain probability."

    score = max(0, min(100, score))
    return score, reasons




def main() -> None:
    import sys
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python compute_delivery_risk.py <config_path> <output_path>")

    cfg_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    cfg = load_cfg(cfg_path)

    site_id = cfg.get("site_id", "unknown")
    site_name = cfg.get("site_name", "Unknown")
    service = cfg.get("service_window_local", {"start": "16:00", "end": "23:00"})
    start = service.get("start", "16:00")
    end = service.get("end", "23:00")

    # local time label only (Peru)
    local_now = datetime.now().astimezone()
    local_hhmm = local_now.strftime("%H:%M")
    is_in_service = in_window(local_hhmm, start, end)

    with sqlite3.connect(DB_PATH) as conn:
        traffic = get_latest_traffic(conn)
        weather = get_latest_weather(conn)

    # Extract a simple “rain outlook next 3h” from OpenWeather raw payload if present.
    pop_3h = None
    if weather and isinstance(weather.get("payload"), dict):
        payload = weather["payload"]
        # If you used One Call 3.0 hourly: payload["hourly"][i]["pop"]
        hourly = payload.get("hourly")
        if isinstance(hourly, list) and hourly:
            vals = []
            for h in hourly[:3]:
                pop = h.get("pop")
                if isinstance(pop, (int, float)):
                    vals.append(float(pop))
            if vals:
                pop_3h = max(vals)

    traffic_ratio = traffic["ratio"] if traffic else None
    score, score_reasons = score_delivery_risk(traffic_ratio, pop_3h)
    sev = classify_score(score)
    level, icon = sev.level, sev.icon


    # Confidence
    have_traffic = traffic_ratio is not None
    have_weather = pop_3h is not None
    if have_traffic and have_weather:
        confidence = "Medium"
        confidence_reason = "Traffic and short-term rain outlook are available."
    elif have_traffic or have_weather:
        confidence = "Low"
        confidence_reason = "One of traffic or weather context is missing; variability may be higher than shown."
    else:
        confidence = "Low"
        confidence_reason = "Traffic and weather context missing; this is mostly a placeholder."

    subtitle = "Delivery variability vs typical conditions"
    if not is_in_service:
        subtitle = f"Off-hours: informational snapshot (service window {start}–{end})"

    # Drivers list (UI-friendly)
    drivers = [
        {"label": "Traffic vs freeflow", "impact": "High" if (traffic_ratio is not None and traffic_ratio < 0.75) else "Low"},
        {"label": "Rain probability next 3h", "impact": "High" if (pop_3h is not None and pop_3h >= 0.30) else "Low"},
        {"label": "Last-mile variability", "impact": "Medium"},
    ]

    card = {
        "schema_version": "ui-ready-v1",
        "site_id": site_id,
        "site_name": site_name,
        "generated_at_local": local_now.isoformat(),
        "service_window_local": {"start": start, "end": end},
        "insight": {
            "id": "delivery_risk",
            "title": "Delivery Risk",
            "category": "Delivery",
            "time_horizon": "0–3h",
            "status": {
                "level": level,
                "icon": icon,
                "score_0_100": score,
                "subtitle": subtitle,
                "confidence": confidence,
                "confidence_reason": confidence_reason,
            },
            "summary": "External conditions suggest delivery timing may vary with traffic and short-term rain risk.",
            "drivers": drivers,
            "implications": [
                "Higher delivery ETA variability can increase customer frustration and remake risk.",
                "If conditions worsen, batching deliveries may trade speed for reliability.",
            ],
            "supported_considerations": [
                "Consider slightly longer quoted ETAs when risk is Elevated/High.",
                "Prioritize closer zones if conditions deteriorate.",
                "If rain probability rises, expect slower rider availability and curbside delays.",
            ],
            "suggested_actions": [
                {
                    "action": "Add a small buffer to quoted delivery ETA",
                    "when": "If score ≥ 45 (Elevated/High)",
                    "why": "Reduces late-delivery complaints when external variability is high.",
                    "effort": "Low",
                    "tradeoff": "Slightly longer ETA shown",
                },
                {
                    "action": "Prioritize nearby delivery zones first",
                    "when": "If traffic ratio < 0.75 or rain pop ≥ 0.30",
                    "why": "Shorter distances are less sensitive to external slowdowns.",
                    "effort": "Low",
                    "tradeoff": "May delay farther zones",
                },
            ],
            "outlook": "Outlook (0–3h): Conditions can shift quickly; re-run the pipeline closer to service.",
            "trust_note": "This insight uses only external conditions (traffic/weather). No internal order data is used.",
        },
        "_internal": {
            "traffic": traffic,
            "weather": {"max_pop_next_3h": pop_3h},
            "score_reasons": score_reasons,
            "flags": {"is_in_service_window": is_in_service},
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")
    print("✅ Delivery card JSON generated (UI-ready v1).")
    print(f"✅ Wrote: {out_path}")


if __name__ == "__main__":
    main()
