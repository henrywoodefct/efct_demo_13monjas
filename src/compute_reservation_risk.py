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
    # These tables should already exist from fetch scripts, but we harden columns anyway.
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
    return {"ts_utc": ts_utc, "current_speed_kmh": cur, "freeflow_speed_kmh": ff}


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


def median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def bucket_15min(dt_local: datetime) -> int:
    return (dt_local.hour * 60 + dt_local.minute) // 15


def baseline_ratio(conn: sqlite3.Connection, weekday: int, bucket: int, tz_local) -> float | None:
    # lightweight baseline from recent history; becomes meaningful after a few days
    rows = conn.execute(
        """
        SELECT ts_utc, current_speed_kmh, freeflow_speed_kmh
        FROM traffic_flow_snapshots
        ORDER BY id DESC
        LIMIT 2000
        """
    ).fetchall()

    vals: list[float] = []
    for ts_utc, cur, ff in rows:
        if cur is None or ff is None or ff <= 0:
            continue
        dt_utc = datetime.fromisoformat(ts_utc.replace("Z", "+00:00"))
        dt_loc = dt_utc.astimezone(tz_local)
        if dt_loc.weekday() != weekday:
            continue
        if bucket_15min(dt_loc) != bucket:
            continue
        vals.append(cur / ff)

    return median(vals)


def rain_now_and_reason(payload: dict | None) -> tuple[bool, str]:
    if not payload:
        return False, "No weather data available."
    cur = payload.get("current") or {}
    wx = (cur.get("weather") or [{}])[0] or {}
    main = (wx.get("main") or "").lower()

    rain_mm = 0.0
    if isinstance(cur.get("rain"), dict):
        rain_mm = float(cur["rain"].get("1h") or 0.0)

    if "rain" in main or rain_mm >= 0.2:
        return True, "Rain detected."
    return False, "No rain detected."


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
    #   python compute_reservation_risk.py config/site.json
    # Preferred (used by run_pipeline.py):
    #   python compute_reservation_risk.py config/site.json outputs/cards/reservation_flow_risk.json
    if len(sys.argv) not in (2, 3):
        raise SystemExit(
            "Usage: python compute_reservation_risk.py <config_path> [output_path]"
        )

    cfg = load_cfg(Path(sys.argv[1]))
    tz_local = tz.gettz(cfg.get("timezone", "America/Lima"))

    service_start = int(cfg.get("service_window", {}).get("start_hour", 16))
    service_end = int(cfg.get("service_window", {}).get("end_hour", 23))
    peak_start = int(cfg.get("peak_window", {}).get("start_hour", 19))
    peak_end = int(cfg.get("peak_window", {}).get("end_hour", 22))

    # IMPORTANT: Never write the legacy reservation_flow.json
    # Default is the canonical risk card filename.
    out_path = (
        Path(sys.argv[2])
        if len(sys.argv) == 3
        else (ROOT / "outputs" / "cards" / "reservation_flow_risk.json")
    )

    # Guard: if caller passes the legacy name, force canonical.
    if out_path.name == "reservation_flow.json":
        out_path = out_path.with_name("reservation_flow_risk.json")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        ensure_schema(conn)

        latest = get_latest_traffic(conn)
        if not latest:
            raise RuntimeError("No traffic data found. Run fetch_traffic_tomtom.py first.")

        dt_utc = datetime.fromisoformat(latest["ts_utc"].replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(tz_local)

        # Traffic ratios (lower is worse)
        cur_ratio = None
        if latest["current_speed_kmh"] is not None and latest["freeflow_speed_kmh"] not in (None, 0):
            cur_ratio = latest["current_speed_kmh"] / latest["freeflow_speed_kmh"]

        wd = dt_local.weekday()
        bkt = bucket_15min(dt_local)
        base = baseline_ratio(conn, wd, bkt, tz_local)

        # Positive deviation means worse-than-normal (baseline higher ratio than current)
        deviation = 0.0
        if cur_ratio is not None and base is not None:
            deviation = max(0.0, base - cur_ratio)
        elif cur_ratio is not None and base is None:
            deviation = 0.2 * max(0.0, 1.0 - cur_ratio)

        recent = get_recent_traffic_ratios(conn, minutes=60)
        volatility = (max(recent) - min(recent)) if len(recent) >= 4 else 0.0

        weather_payload = get_latest_weather_payload(conn)
        rain_now, rain_reason = rain_now_and_reason(weather_payload)
        out = outlook_next_3h(weather_payload)

        is_peak = (peak_start <= dt_local.hour < peak_end)
        in_service_window = (service_start <= dt_local.hour < service_end)

        raw_score = 0.0
        raw_score += max(0.0, deviation) * 3.0
        raw_score += max(0.0, volatility) * 2.0
        if rain_now:
            raw_score += 0.6
        if is_peak:
            raw_score += 0.6

        score = score_0_100(raw_score)
        
        sev = classify_score(score)
        level, icon = sev.level, sev.icon

        # Confidence logic
        if base is None and not weather_payload:
            confidence = "Low"
            conf_reason = "Traffic baseline is not established yet and weather context is missing; insight is primarily heuristic."
        elif base is None:
            confidence = "Medium"
            conf_reason = "Traffic baseline is not established yet; comparison is a conservative proxy until more history accumulates."
        elif not weather_payload:
            confidence = "Medium"
            conf_reason = "Traffic baseline is available but weather context is missing; confidence is reduced."
        else:
            confidence = "High" if volatility < 0.08 else "Medium"
            conf_reason = "Traffic baseline and weather context are available; confidence depends on short-term volatility."

        drivers = [
            {"label": "Traffic vs baseline", "impact": impact_label(deviation, 0.02, 0.06)},
            {"label": "Short-term volatility (60m)", "impact": impact_label(volatility, 0.06, 0.12)},
            {"label": "Rain amplification risk", "impact": "Medium" if (rain_now or (out and out.get("rain_likely"))) else "Low"},
            {"label": "Peak-hour sensitivity", "impact": "Medium" if is_peak else "Low"},
            {"label": "Large-group sensitivity (>=5)", "impact": "Medium"},
        ]

        summary = (
            "Arrival times are less predictable than usual, increasing the risk of overlapping reservations during peak service hours."
            if level != "Normal"
            else "Arrival timing risk appears stable, but uncertainty can increase if traffic volatility or rain rises."
        )

        implications = [
            "Higher likelihood of reservation overlap and queue spillover during peak hours.",
            "Greater sensitivity to delays for larger groups.",
        ]

        considerations = [
            "Wider buffers for larger groups may reduce cascading delays.",
            "Greater flexibility during peak windows may be more valuable than usual.",
            "Proactive expectation-setting may reduce frustration if delays occur.",
        ]

        outlook_text = "Outlook (0–3h): No short-term outlook available."
        if out:
            if out["rain_likely"] and level != "Normal":
                outlook_text = "Outlook (0–3h): Elevated conditions may persist; rain risk could continue to amplify arrival variability."
            elif out["rain_likely"] and level == "Normal":
                outlook_text = "Outlook (0–3h): Conditions look normal, but rain risk could increase arrival variability later."
            else:
                outlook_text = "Outlook (0–3h): Conditions are likely to remain similar in the near term."

        suggested_actions = [
            {
                "action": "Add a buffer for groups of 5+ during the next 3 hours",
                "when": "Any booking in the next 3 hours for 5+ guests",
                "why": "Large groups create longer seating/ordering latency and amplify small arrival delays.",
                "effort": "Low",
                "tradeoff": "Slightly fewer slots, smoother flow",
            },
            {
                "action": "Proactively confirm late arrivals with a soft message",
                "when": "10–15 minutes before reservation time",
                "why": "Reduces uncertainty and helps resequence tables if someone is running late.",
                "effort": "Low",
                "tradeoff": "Adds messaging workload",
            },
        ]

        if volatility >= 0.08 or deviation >= 0.06 or level == "Elevated":
            suggested_actions.append(
                {
                    "action": "Avoid tight back-to-back reservations during peak window",
                    "when": f"During peak window ({hhmm_from_hour(peak_start)}–{hhmm_from_hour(peak_end)})",
                    "why": "When arrivals cluster, tighter sequencing increases queue spillover risk.",
                    "effort": "Medium",
                    "tradeoff": "May reduce peak throughput, improves experience",
                }
            )

        subtitle = "Compared to a normal evening" if base is not None else "Compared to recent external conditions"
        if not in_service_window:
            subtitle = "Off-hours: informational snapshot (service window 16:00–23:00)"

        result = {
            "schema_version": "ui-ready-v1",
            "site_id": cfg.get("site_id", "13monjas"),
            "site_name": cfg.get("site_name", "13 Monjas"),
            "generated_at_local": dt_local.isoformat(),
            "service_window_local": {"start": hhmm_from_hour(service_start), "end": hhmm_from_hour(service_end)},
            "insight": {
                "id": "reservation_flow_risk",
                "title": "Reservation Flow Risk",
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
                "supported_considerations": considerations,
                "suggested_actions": suggested_actions,
                "outlook": outlook_text,
                "trust_note": "This insight is based entirely on external conditions (traffic, weather, timing, location). No internal reservation or customer data is used.",
            },
            "_internal": {
                "traffic": {
                    "current_ratio": cur_ratio,
                    "baseline_ratio": base,
                    "deviation": deviation,
                    "volatility_60m": volatility,
                },
                "weather": {
                    "rain_now": rain_now,
                    "rain_reason": rain_reason,
                    "max_pop_next_3h": out.get("max_pop") if out else None,
                },
                "flags": {
                    "is_peak_window": is_peak,
                    "is_in_service_window": in_service_window,
                },
            },
        }

        out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    print("✅ Card JSON generated (UI-ready v1).")
    print(f"✅ Wrote: {out_path}")


if __name__ == "__main__":
    main()
