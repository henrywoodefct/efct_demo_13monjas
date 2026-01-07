import os
import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "efct.db"


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS traffic_flow_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            current_speed_kmh REAL,
            freeflow_speed_kmh REAL,
            current_travel_time_s REAL,
            freeflow_travel_time_s REAL,
            confidence REAL,
            raw_json TEXT NOT NULL
        );
        """
    )
    conn.commit()


def fetch_tomtom_flow(lat: float, lon: float, api_key: str) -> dict:
    # TomTom Flow Segment Data
    url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"
    params = {
        "key": api_key,
        "point": f"{lat},{lon}",
        "unit": "KMPH",
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def parse_flow(payload: dict) -> dict:
    fs = payload.get("flowSegmentData") or payload.get("flowSegmentDataResult") or {}
    return {
        "current_speed_kmh": fs.get("currentSpeed"),
        "freeflow_speed_kmh": fs.get("freeFlowSpeed"),
        "current_travel_time_s": fs.get("currentTravelTime"),
        "freeflow_travel_time_s": fs.get("freeFlowTravelTime"),
        "confidence": fs.get("confidence"),
    }


def main() -> None:
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python fetch_traffic_tomtom.py config/site.json")

    cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    lat, lon = float(cfg["lat"]), float(cfg["lon"])

    api_key = os.getenv("TOMTOM_API_KEY")
    if not api_key:
        raise RuntimeError("Missing TOMTOM_API_KEY")

    ts_utc = datetime.now(timezone.utc).isoformat()

    payload = fetch_tomtom_flow(lat, lon, api_key)
    parsed = parse_flow(payload)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT INTO traffic_flow_snapshots
            (ts_utc, lat, lon,
             current_speed_kmh, freeflow_speed_kmh,
             current_travel_time_s, freeflow_travel_time_s,
             confidence, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_utc,
                lat,
                lon,
                parsed["current_speed_kmh"],
                parsed["freeflow_speed_kmh"],
                parsed["current_travel_time_s"],
                parsed["freeflow_travel_time_s"],
                parsed["confidence"],
                json.dumps(payload),
            ),
        )
        conn.commit()

    print(f"✅ Traffic snapshot stored @ {ts_utc}")


if __name__ == "__main__":
    main()
