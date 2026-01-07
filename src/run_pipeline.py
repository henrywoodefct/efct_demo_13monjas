import os
import json
import sqlite3
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "site.json"
DB_PATH = ROOT / "data" / "efct.db"

OUTPUTS_DIR = ROOT / "outputs"
CARDS_DIR = OUTPUTS_DIR / "cards"
FEED_PATH = OUTPUTS_DIR / "feed.json"


def run(cmd: list[str]) -> None:
    print(">", " ".join(cmd))
    subprocess.check_call(cmd)


def has_any_rows(table: str) -> bool:
    if not DB_PATH.exists():
        return False
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            return row is not None
    except Exception:
        return False


def try_run(label: str, cmd: list[str], required_if_no_db: str | None = None) -> None:
    """
    Soft-fail runner:
    - If it fails and DB already has data for the relevant table -> warn and continue.
    - If it fails and DB has NO data -> raise (we can't compute anything meaningful).
    """
    try:
        run(cmd)
    except subprocess.CalledProcessError as e:
        if required_if_no_db and not has_any_rows(required_if_no_db):
            print(f"❌ {label} failed and there is no existing data in DB table '{required_if_no_db}'.")
            raise
        print(f"⚠️ {label} failed. Continuing using most recent DB snapshot (if available).")
        print("   ", e)


def main() -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    CARDS_DIR.mkdir(parents=True, exist_ok=True)

    # --- Decide if we can fetch (keys) vs rely on DB
    have_traffic_db = has_any_rows("traffic_flow_snapshots")
    have_weather_db = has_any_rows("weather_snapshots")
    have_bcrp_db = has_any_rows("bcrp_series_points")
    have_fuel_db = has_any_rows("fuel_price_aggregates")

    tomtom_key = os.getenv("TOMTOM_API_KEY")
    openweather_key = os.getenv("OPENWEATHER_API_KEY")

    # If keys missing but DB has snapshots, allow demo to proceed.
    # If keys missing AND DB empty for that source, we must stop.
    if not tomtom_key and not have_traffic_db:
        raise RuntimeError("Missing TOMTOM_API_KEY and no existing traffic snapshots in DB.")
    if not openweather_key and not have_weather_db:
        print("⚠️ OPENWEATHER_API_KEY missing and no existing weather snapshots in DB.")
        print("   Continuing (cards will degrade gracefully without weather where applicable).")

    # --- Fetch snapshots (soft-fail)
    if tomtom_key:
        try_run(
            "Traffic fetch",
            ["python", str(ROOT / "src" / "fetch_traffic_tomtom.py"), str(CONFIG_PATH)],
            required_if_no_db="traffic_flow_snapshots",
        )
    else:
        print("⚠️ TOMTOM_API_KEY missing, using existing traffic snapshots from DB.")

    if openweather_key:
        try_run(
            "Weather fetch",
            ["python", str(ROOT / "src" / "fetch_weather_openweather.py"), str(CONFIG_PATH)],
            required_if_no_db=None,  # weather is optional for demo; cards handle missing weather
        )
    else:
        print("⚠️ OPENWEATHER_API_KEY missing, using existing weather snapshots from DB (if any).")


    # --- Fetch Peru macro + logistics proxies (no API keys required)
    # These are optional for the demo; cards degrade gracefully if missing.
    try_run(
        "BCRP series fetch (food inflation + FX)",
        ["python", str(ROOT / "src" / "fetch_bcrp_series.py")],
        required_if_no_db=None,
    )

    # --- Compute cards (UI-ready v1)
    run([
        "python", str(ROOT / "src" / "compute_reservation_risk.py"),
        str(CONFIG_PATH),
        str(CARDS_DIR / "reservation_flow_risk.json"),
    ])
    run([
        "python", str(ROOT / "src" / "compute_late_arrival_risk.py"),
        str(CONFIG_PATH),
        str(CARDS_DIR / "late_arrival_risk.json"),
    ])
    run([
        "python", str(ROOT / "src" / "compute_delivery_risk.py"),
        str(CONFIG_PATH),
        str(CARDS_DIR / "delivery_risk.json"),
    ])

    run([
        "python", str(ROOT / "src" / "compute_logistics_cost_pressure_risk.py"),
        str(CONFIG_PATH),
        str(CARDS_DIR / "logistics_cost_pressure_risk.json"),
    ])

    # --- Build feed (with rollups + summary)
    run(["python", str(ROOT / "src" / "build_feed.py"), str(CONFIG_PATH), str(CARDS_DIR), str(FEED_PATH)])

    # --- Validate feed (hard gate)
    run(["python", str(ROOT / "src" / "validate_feed.py"), str(FEED_PATH)])

    # --- Print a clean “demo-ready” summary
    try:
        feed = json.loads(FEED_PATH.read_text(encoding="utf-8"))
        rollups = feed.get("rollups", {})
        overall = rollups.get("overall_status")
        urgency = rollups.get("urgency_summary")
        summary = rollups.get("summary")
        print("\n🧾 Demo Summary")
        print(f"• Overall:  {overall}")
        print(f"• Urgency:  {urgency}")
        print(f"• Summary:  {summary}")
    except Exception:
        pass

    print("\n✅ Pipeline complete.")
    print(f"✅ Cards dir: {CARDS_DIR}")
    print(f"✅ Feed:      {FEED_PATH}")


if __name__ == "__main__":
    main()
