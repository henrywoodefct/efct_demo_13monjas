import json
import sqlite3
from severity import classify_score
from pathlib import Path
from datetime import datetime
from dateutil import tz

from db import ensure_columns, table_exists

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "efct.db"


def load_cfg(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def ensure_schema(conn: sqlite3.Connection) -> None:
    if table_exists(conn, "weather_snapshots"):
        ensure_columns(conn, "weather_snapshots", {"raw_json": "TEXT"})
    if table_exists(conn, "traffic_flow_snapshots"):
        ensure_columns(
            conn,
            "traffic_flow_snapshots",
            {
                "current_speed_kmh": "REAL",
                "freeflow_speed_kmh": "REAL",
                "raw_json": "TEXT",
            },
        )


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
    ratio = None
    if cur is not None and ff not in (None, 0):
        ratio = cur / ff
    return {"ts_utc": ts_utc, "ratio": ratio}


def get_recent_traffic_ratios(conn: sqlite3.Connection, minutes: int = 60) -> list[float]:
    rows = conn.execute(
        """
        SELECT current_speed_kmh, freeflow_speed_kmh
        FROM traffic_flow_snapshots
        WHERE ts_utc >= datetime('now', ?)
        ORDER BY ts_utc ASC
        """,
        (f"-{minutes} minutes",),
    ).fetchall()

    ratios: list[float] = []
    for cur, ff in rows:
        if cur is None or ff is None or ff <= 0:
            continue
        ratios.append(cur / ff)
    return ratios


def get_latest_weather_payload(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        """
        SELECT raw_json
        FROM weather_snapshots
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def outlook_next_3h(payload: dict | None) -> dict | None:
    if not payload:
        return None
    hourly = payload.get("hourly") or []
    if not hourly:
        return None

    hourly = hourly[:3]
    max_pop = 0.0
    rain_likely = False

    for h in hourly:
        pop = float(h.get("pop") or 0.0)
        max_pop = max(max_pop, pop)

        rainmm = 0.0
        if isinstance(h.get("rain"), dict):
            rainmm = float(h["rain"].get("1h") or 0.0)

        wx = (h.get("weather") or [{}])[0] or {}
        main = (wx.get("main") or "").lower()

        if pop >= 0.5 or rainmm >= 0.2 or "rain" in main:
            rain_likely = True

    return {"rain_likely": rain_likely, "max_pop": max_pop}





def score_0_100(raw_score: float) -> int:
    scaled = (raw_score / 2.5) * 100.0
    return int(max(0, min(100, round(scaled))))


def impact_label(value: float, low: float, med: float) -> str:
    if value >= med:
        return "High"
    if value >= low:
        return "Medium"
    return "Low"


def hhmm_from_hour(h: int) -> str:
    return f"{int(h):02d}:00"


def main() -> None:
    import sys

    # CLI
    # Backwards compatible:
    #   python compute_late_arrival_risk.py config/site.json
    # Preferred (used by run_pipeline.py):
    #   python compute_late_arrival_risk.py config/site.json outputs/cards/late_arrival_risk.json
    if len(sys.argv) not in (2, 3):
        raise SystemExit(
            "Usage: python compute_late_arrival_risk.py <config_path> [output_path]"
        )

    cfg = load_cfg(Path(sys.argv[1]))
    tz_local = tz.gettz(cfg.get("timezone", "America/Lima"))

    service_start = int(cfg.get("service_window", {}).get("start_hour", 16))
    service_end = int(cfg.get("service_window", {}).get("end_hour", 23))
    peak_start = int(cfg.get("peak_window", {}).get("start_hour", 19))
    peak_end = int(cfg.get("peak_window", {}).get("end_hour", 22))

    out_path = (
        Path(sys.argv[2])
        if len(sys.argv) == 3
        else (ROOT / "outputs" / "cards" / "late_arrival_risk.json")
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        ensure_schema(conn)

        traffic = get_latest_traffic(conn)
        if not traffic:
            raise RuntimeError("No traffic data found. Run fetch_traffic_tomtom.py first.")

        dt_utc = datetime.fromisoformat(traffic["ts_utc"].replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(tz_local)

        in_service_window = (service_start <= dt_local.hour < service_end)
        is_peak = (peak_start <= dt_local.hour < peak_end)

        ratios = get_recent_traffic_ratios(conn, minutes=60)
        volatility = (max(ratios) - min(ratios)) if len(ratios) >= 4 else 0.0

        wx_payload = get_latest_weather_payload(conn)
        out = outlook_next_3h(wx_payload)
        rain_likely = bool(out and out.get("rain_likely"))

        ratio = traffic["ratio"]
        congestion = 0.0
        if ratio is not None:
            congestion = max(0.0, 1.0 - ratio)

        # Late arrival risk emphasizes volatility and rain more, plus peak sensitivity
        raw_score = 0.0
        raw_score += congestion * 1.8
        raw_score += volatility * 2.4
        raw_score += (0.7 if rain_likely else 0.0)
        raw_score += (0.6 if is_peak else 0.0)

        score = score_0_100(raw_score)
        sev = classify_score(score)
        level, icon = sev.level, sev.icon

        if not wx_payload:
            confidence = "Medium"
            conf_reason = "Traffic data is available; weather context is missing, so late-arrival amplification is uncertain."
        else:
            confidence = "High" if volatility < 0.10 else "Medium"
            conf_reason = "Traffic and weather outlook are available; confidence depends on short-term traffic volatility."

        subtitle = "Arrival punctuality risk for the next 0–3 hours"
        if not in_service_window:
            subtitle = "Off-hours: informational snapshot (service window 16:00–23:00)"

        drivers = [
            {"label": "Short-term traffic volatility (60m)", "impact": impact_label(volatility, 0.06, 0.12)},
            {"label": "Traffic congestion proxy", "impact": impact_label(congestion, 0.10, 0.25)},
            {"label": "Rain / precipitation outlook", "impact": "Medium" if rain_likely else "Low"},
            {"label": "Peak-hour sensitivity", "impact": "Medium" if is_peak else "Low"},
        ]

        summary = (
            "Arrival punctuality risk appears stable under current external conditions."
            if level == "Normal"
            else "Late arrivals may become more frequent due to volatile traffic and/or weather amplification."
        )

        implications = [
            "Higher probability of customers arriving late, causing table-sequencing friction.",
            "Increased variance in seating times can cause knock-on delays during busy periods.",
        ]

        suggested_actions = [
            {
                "action": "Send a soft check-in message shortly before reservation time",
                "when": "10–15 minutes before reservation time",
                "why": "Reduces uncertainty and allows re-sequencing if guests report delays.",
                "effort": "Low",
                "tradeoff": "Adds messaging workload",
            },
            {
                "action": "Use slightly wider buffers for groups of 5+ when volatility is high",
                "when": "When traffic volatility is Medium/High",
                "why": "Large groups amplify the operational cost of late arrivals.",
                "effort": "Low",
                "tradeoff": "Fewer tightly-packed slots",
            },
        ]

        if level != "Normal" or (is_peak and volatility >= 0.08):
            suggested_actions.append(
                {
                    "action": "Avoid scheduling back-to-back reservation start times during peak",
                    "when": f"During peak window ({hhmm_from_hour(peak_start)}–{hhmm_from_hour(peak_end)})",
                    "why": "Reduces cascading delays when arrivals cluster or slip.",
                    "effort": "Medium",
                    "tradeoff": "May reduce peak throughput, improves experience",
                }
            )

        outlook_text = "Outlook (0–3h): Limited weather outlook available."
        if out:
            outlook_text = (
                "Outlook (0–3h): Rain risk could increase late-arrival likelihood."
                if rain_likely
                else "Outlook (0–3h): Conditions are likely to remain similar in the near term."
            )

        result = {
            "schema_version": "ui-ready-v1",
            "site_id": cfg.get("site_id", "13monjas"),
            "site_name": cfg.get("site_name", "13 Monjas"),
            "generated_at_local": dt_local.isoformat(),
            "service_window_local": {"start": hhmm_from_hour(service_start), "end": hhmm_from_hour(service_end)},
            "insight": {
                "id": "late_arrival_risk",
                "title": "Late Arrival Risk",
                "category": "Reservations",
                "time_horizon": "0–3h",
                "status": {
                    "level": level,
                    "icon": icon,
                    "score_0_100": score,
                    "subtitle": subtitle,
                    "confidence": confidence,
                    "confidence_reason": conf_reason,
                },
                "summary": summary,
                "drivers": drivers,
                "implications": implications,
                "supported_considerations": [
                    "Traffic volatility affects punctuality more than steady congestion.",
                    "Rain risk can increase both travel friction and unpredictability.",
                ],
                "suggested_actions": suggested_actions,
                "outlook": outlook_text,
                "trust_note": "This insight is based entirely on external conditions (traffic, weather, timing, location). No internal reservation or customer data is used.",
            },
            "_internal": {
                "traffic": {"ratio": ratio, "congestion": congestion, "volatility_60m": volatility},
                "weather": {"rain_likely_next_3h": rain_likely, "max_pop_next_3h": float(out.get('max_pop')) if out else None},
                "flags": {"is_peak_window": is_peak, "is_in_service_window": in_service_window},
            },
        }

        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("✅ Card JSON generated (UI-ready v1).")
    print(f"✅ Wrote: {out_path}")


if __name__ == "__main__":
    main()
