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
        CREATE TABLE IF NOT EXISTS weather_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_utc TEXT NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            rain_1h_mm REAL,
            weather_main TEXT,
            weather_desc TEXT,
            temp_c REAL,
            raw_json TEXT NOT NULL
        );
        """
    )
    conn.commit()


def fetch_current(lat: float, lon: float, api_key: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_forecast(lat: float, lon: float, api_key: str) -> dict:
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {"lat": lat, "lon": lon, "appid": api_key, "units": "metric"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def to_onecall_like_payload(current_json: dict, forecast_json: dict) -> dict:
    current = {
        "temp": (current_json.get("main") or {}).get("temp"),
        "rain": current_json.get("rain"),  # may be {"1h": ...}
        "weather": current_json.get("weather") or [],
    }

    hourly = []
    for item in (forecast_json.get("list") or [])[:3]:
        # forecast rain often in {"3h": ...}; approximate per-hour
        rain_1h = 0.0
        if isinstance(item.get("rain"), dict):
            r3 = float(item["rain"].get("3h") or 0.0)
            rain_1h = r3 / 3.0 if r3 > 0 else 0.0

        hourly.append(
            {
                "pop": float(item.get("pop") or 0.0),
                "rain": {"1h": rain_1h} if rain_1h > 0 else {},
                "weather": item.get("weather") or [],
            }
        )

    return {
        "current": current,
        "hourly": hourly,
        "_source": "openweather_2.5_weather+forecast",
        "_raw": {"current": current_json, "forecast": forecast_json},
    }


def parse_current(payload: dict) -> dict:
    cur = payload.get("current", {}) or {}
    wx = (cur.get("weather") or [{}])[0] or {}

    rain_1h = None
    if isinstance(cur.get("rain"), dict):
        rain_1h = cur["rain"].get("1h")

    return {
        "rain_1h_mm": rain_1h,
        "weather_main": wx.get("main"),
        "weather_desc": wx.get("description"),
        "temp_c": cur.get("temp"),
    }


def main() -> None:
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("Usage: python fetch_weather_openweather.py config/site.json")

    cfg = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    lat, lon = float(cfg["lat"]), float(cfg["lon"])

    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError("Missing OPENWEATHER_API_KEY")

    ts_utc = datetime.now(timezone.utc).isoformat()

    current_json = fetch_current(lat, lon, api_key)
    forecast_json = fetch_forecast(lat, lon, api_key)
    payload = to_onecall_like_payload(current_json, forecast_json)
    parsed = parse_current(payload)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        init_db(conn)
        conn.execute(
            """
            INSERT INTO weather_snapshots
            (ts_utc, lat, lon, rain_1h_mm, weather_main, weather_desc, temp_c, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_utc,
                lat,
                lon,
                parsed["rain_1h_mm"],
                parsed["weather_main"],
                parsed["weather_desc"],
                parsed["temp_c"],
                json.dumps(payload),
            ),
        )
        conn.commit()

    print(f"✅ Weather snapshot stored @ {ts_utc}")


if __name__ == "__main__":
    main()
