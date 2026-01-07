import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from statistics import pstdev

from severity import classify_score

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "efct.db"


def clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x


def now_local(tz_offset_hours: int) -> datetime:
    return datetime.now(timezone(timedelta(hours=tz_offset_hours)))


def load_cfg(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def fetch_bcrp_latest(conn: sqlite3.Connection, series_code: str) -> tuple[str, float] | None:
    row = conn.execute(
        """
        SELECT period, value
        FROM bcrp_series_points
        WHERE series_code = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (series_code,),
    ).fetchone()
    if not row:
        return None
    return row[0], float(row[1])



def fetch_bcrp_recent_values(conn: sqlite3.Connection, series_code: str, limit: int) -> list[float]:
    rows = conn.execute(
        """
        SELECT value
        FROM bcrp_series_points
        WHERE series_code = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (series_code, limit),
    ).fetchall()
    return [float(r[0]) for r in rows if r and r[0] is not None]



def fetch_fx_volatility(
    conn: sqlite3.Connection,
    series_code: str = "PD04638PD",
    window_days: int = 30,
    baseline_days: int = 180,
) -> tuple[float, float] | None:
    """
    Computes:
    - vol_window: std dev of daily levels over last window_days
    - vol_baseline: std dev of daily levels over last baseline_days
    Uses recent points in DB (period is YYYY-MM-DD).
    """
    rows = conn.execute(
        """
        SELECT period, value
        FROM bcrp_series_points
        WHERE series_code = ?
        ORDER BY period DESC
        LIMIT ?
        """,
        (series_code, baseline_days),
    ).fetchall()
    if len(rows) < max(10, window_days // 2):
        return None

    values = [float(v) for _, v in rows if v is not None]
    vol_baseline = pstdev(values) if len(values) >= 2 else 0.0
    vol_window = pstdev(values[:window_days]) if len(values) >= window_days else pstdev(values)
    return vol_window, vol_baseline


def compute_food_pressure(food_yoy: float) -> float:
    """
    Map YOY food inflation into 0..1 pressure.
    Heuristic:
      ~2% = low pressure
      ~8% = high pressure
    """
    return clamp01((food_yoy - 2.0) / 6.0)


def compute_transport_pressure(transport_var: float) -> float:
    """
    Map transport inflation/variation into 0..1 pressure.
    Heuristic:
      ~1% = low pressure
      ~7% = high pressure
    (We keep it similar scale to food but slightly tighter.)
    """
    return clamp01((transport_var - 1.0) / 6.0)


def compute_fx_pressure(vol_window: float, vol_baseline: float) -> float:
    """
    Pressure increases when 30d volatility is meaningfully above baseline.
    """
    if vol_baseline <= 0:
        return 0.25 if vol_window > 0 else 0.0
    ratio = vol_window / vol_baseline
    # ratio 1.0 => normal, 1.8+ => high
    return clamp01((ratio - 1.0) / 0.8)


def main() -> None:
    import sys

    if len(sys.argv) < 3:
        raise SystemExit(
            "Usage: python compute_logistics_cost_pressure_risk.py <config/site.json> <output_path>"
        )

    cfg_path = Path(sys.argv[1])
    out_path = Path(sys.argv[2])

    cfg = load_cfg(cfg_path)
    site_id = cfg["site_id"]
    site_name = cfg["site_name"]
    tz_offset = -5  # keep consistent with other scripts; demo assumes Peru

    local_now = now_local(tz_offset)

    drivers: list[str] = []
    confidence_reasons: list[str] = []
    signals_available = 0

    # Series codes:
    FOOD_SERIES = "PN09822PM"      # IPC Alimentos y Bebidas (var% 12 meses)
    TRANSPORT_SERIES = "PN01310PM" # Transport inflation proxy (monthly)
    FX_SERIES = "PD04638PD"        # TC Interbancario - Venta (daily)

    with sqlite3.connect(DB_PATH) as conn:
        # --- FOOD inflation (monthly, YOY)
        food_latest = fetch_bcrp_latest(conn, FOOD_SERIES)
        food_pressure = None
        if food_latest:
            _, food_yoy = food_latest
            food_pressure = compute_food_pressure(food_yoy)
            signals_available += 1
            confidence_reasons.append("Food inflation series available (BCRPData).")
            if food_pressure >= 0.60:
                drivers.append("Food inflation pressure is elevated (12m trend).")
            elif food_pressure >= 0.35:
                drivers.append("Food inflation is above typical comfort zone.")
        else:
            confidence_reasons.append("Food inflation series missing; pressure estimate is partial.")

        # --- TRANSPORT inflation proxy (monthly)
        transport_latest = fetch_bcrp_latest(conn, TRANSPORT_SERIES)
        transport_pressure = None
        if transport_latest:
            _, tr_var = transport_latest
            transport_pressure = compute_transport_pressure(tr_var)
            signals_available += 1
            confidence_reasons.append("Transport cost proxy series available (BCRPData).")
            if transport_pressure >= 0.60:
                drivers.append("Transport cost pressure is elevated (recent inflation trend).")
            elif transport_pressure >= 0.35:
                drivers.append("Transport costs show moderate upward pressure.")
        else:
            confidence_reasons.append("Transport cost proxy missing; transport pressure estimate is partial.")

        # --- FX volatility (daily)
        fx_vol = fetch_fx_volatility(conn, FX_SERIES, window_days=30, baseline_days=180)
        fx_pressure = None
        if fx_vol:
            vol30, vol180 = fx_vol
            fx_pressure = compute_fx_pressure(vol30, vol180)
            signals_available += 1
            confidence_reasons.append("FX series available; volatility computed from recent daily data.")
            if fx_pressure >= 0.60:
                drivers.append("FX volatility is elevated; imported inputs may reprice faster.")
            elif fx_pressure >= 0.35:
                drivers.append("FX volatility is moderately higher than baseline.")
        else:
            confidence_reasons.append("FX series missing or too short; volatility estimate is partial.")
        # --- Ensure drivers reflect calm-but-valid conditions

    if not drivers and signals_available >= 2:
        drivers.append(
            "Macro cost indicators are within normal ranges; repricing risk appears low."
        )

    # --- Combine pressures
    # Default missing signals to neutral-low (not zero, to avoid false certainty)
    food_p = food_pressure if food_pressure is not None else 0.25
    transport_p = transport_pressure if transport_pressure is not None else 0.25
    fx_p = fx_pressure if fx_pressure is not None else 0.20

    # Locked weights for v1
    weighted = (0.45 * food_p) + (0.35 * transport_p) + (0.20 * fx_p)
    score = int(round(100 * clamp01(weighted)))

    if signals_available >= 2 and score < 5:
        score = 5
        
    sev = classify_score(score)
    level, icon = sev.level, sev.icon

    # Subtitle consistent tone
    if level == "Normal":
        subtitle = "External cost signals look stable."
    elif level == "Watch":
        subtitle = "Some upstream cost pressure is building."
    elif level == "Elevated":
        subtitle = "Upstream logistics costs are rising faster than usual."
    else:
        subtitle = "High cost pressure: expect faster repricing and tighter margins."

    # Confidence: based on how many signals we had
    # 1 signal => ~0.55, 2 => ~0.70, 3 => ~0.85
    confidence = round(min(0.90, 0.40 + 0.15 * signals_available), 2)
    if signals_available == 0:
        confidence = 0.35

    confidence_reason = " ".join(confidence_reasons)

    card = {
        "schema_version": "ui-ready-v1",
        "site_id": site_id,
        "site_name": site_name,
        "generated_at_local": local_now.isoformat(),
        "service_window_local": {
            "start": f"{cfg['service_window']['start_hour']:02d}:00",
            "end": f"{cfg['service_window']['end_hour']:02d}:00",
        },
        "insight": {
            "id": "logistics_cost_pressure_risk",
            "title": "Logistics Cost Pressure Risk",
            "category": "Logistics",
            "time_horizon": "7–30d",
            "status": {
                "level": level,
                "icon": icon,
                "score_0_100": score,
                "subtitle": subtitle,
                "confidence": confidence,
                "confidence_reason": confidence_reason,
            },
            "summary": "External indicators (food inflation, transport cost pressure, and FX volatility) suggest how likely suppliers are to reprice or tighten terms in the coming weeks.",
            "drivers": drivers[:5] if drivers else ["Limited signals available; using conservative defaults."],
            "implications": [
                "Higher cost pressure can reduce price-lock windows and increase quote variability.",
                "Margin sensitivity rises on imported or freight-heavy inputs during Elevated/Critical periods.",
            ],
            "supported_considerations": [
                "Focus on menu-margin awareness rather than predicting exact ingredient costs.",
                "Be cautious with promo commitments that depend on volatile inputs when risk is Elevated/Critical.",
                "Re-check supplier quotes closer to order time during higher-pressure periods.",
            ],
            "suggested_actions": [
                {
                    "action": "Review margin exposure on high-cost dishes",
                    "when": "If status is Elevated/Critical",
                    "why": "Cost pressure often hits a few key inputs first (proteins, dairy, imported items).",
                    "effort": "Low",
                    "tradeoff": "Requires quick menu-cost check",
                },
                {
                    "action": "Avoid locking large forward orders without price confirmation",
                    "when": "If score ≥ 50 (Elevated/Critical)",
                    "why": "Higher volatility can shorten supplier quote windows.",
                    "effort": "Low",
                    "tradeoff": "More frequent ordering/check-ins",
                },
                {
                    "action": "Delay promotions that rely on imported inputs",
                    "when": "If FX volatility driver is present",
                    "why": "FX-driven repricing can compress margins unexpectedly.",
                    "effort": "Low",
                    "tradeoff": "Fewer near-term promo options",
                },
            ],
            "trust_note": "This card uses Peru-wide macro indicators (BCRPData) as upstream proxies; it does not use restaurant purchase invoices.",
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(card, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Wrote {out_path}")


if __name__ == "__main__":
    main()
