import csv
import html
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional, Any

import requests

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "efct.db"

BCRP_API_BASE = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api"

DEFAULT_SERIES = [
    ("PN09822PM", "IPC Alimentos y Bebidas (var% 12 meses)", "monthly"),
    ("PN01310PM", "Inflación No Subyacente - Transportes (variación %)", "monthly"),
    ("PD04638PD", "TC Interbancario (S/ por US$) - Venta", "daily"),
]


@dataclass(frozen=True)
class SeriesPoint:
    series_code: str
    series_name: str
    frequency: str
    period: str
    value: float


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bcrp_series_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetched_at_utc TEXT NOT NULL,
            series_code TEXT NOT NULL,
            series_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            period TEXT NOT NULL,
            value REAL NOT NULL,
            UNIQUE(series_code, period)
        )
        """
    )
    conn.commit()


def _build_url(
    series_code: str,
    fmt: str = "csv",
    from_period: Optional[str] = None,
    to_period: Optional[str] = None,
    lang: str = "esp",
) -> str:
    parts = [BCRP_API_BASE, series_code, fmt]
    if from_period and to_period:
        parts += [from_period, to_period, lang]
    elif from_period and not to_period:
        parts += [from_period, lang]
    return "/".join(parts)


def _fetch_text(url: str) -> str:
    headers = {
        "User-Agent": "EFCT-Demo/1.0 (+https://efct.pe)",
        "Accept": "text/csv,application/json,text/plain;q=0.9,*/*;q=0.8",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text or ""


def _parse_float(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s:
        return None
    low = s.lower()
    if low in ("n.d.", "nd", "nan", "n/a", "na"):
        return None
    s = s.replace("\u00A0", "").replace(" ", "")
    # decimal comma
    if "," in s and "." not in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def _normalize_bcrp_csvish_text(raw: str) -> str:
    """
    BCRP 'csv' endpoint often returns HTML-ish content:
    ...<br>"Ene.2011","2.26"<br>...
    Convert it into real CSV text lines.
    """
    t = (raw or "").strip()
    if not t:
        return ""
    # convert <br> to newlines
    t = t.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    # unescape HTML entities (&ntilde;, &iacute;, etc.)
    t = html.unescape(t)
    return t.strip()


def _parse_csv_points(series_code: str, series_name: str, frequency: str, raw_text: str) -> list[SeriesPoint]:
    text = _normalize_bcrp_csvish_text(raw_text)
    if not text:
        return []

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        return []

    # detect delimiter
    first = lines[0]
    delimiter = ";" if (";" in first and "," not in first) else ","

    reader = csv.reader(lines, delimiter=delimiter)
    rows = list(reader)
    if len(rows) < 2:
        return []

    points: list[SeriesPoint] = []
    for r in rows[1:]:
        if not r or len(r) < 2:
            continue
        period = (r[0] or "").strip().strip('"')
        value = _parse_float((r[1] or "").strip().strip('"'))
        if not period or value is None:
            continue
        points.append(SeriesPoint(series_code, series_name, frequency, period, value))
    return points


def _dig_for_pairs(obj: Any) -> list[tuple[str, float]]:
    """
    Extract (period,value) pairs from a few common BCRP JSON shapes.
    We keep this tolerant because the API returns nested config/series/data structures.
    """
    pairs: list[tuple[str, float]] = []

    def maybe_add(p, v):
        if p is None or v is None:
            return
        ps = str(p).strip()
        vf = _parse_float(str(v))
        if ps and vf is not None:
            pairs.append((ps, vf))

    if isinstance(obj, dict):
        # common: {"periods":[{"name":"Ene.2011","value":"2.26"}, ...]}
        for key in ("periods", "data", "values", "results"):
            if key in obj and isinstance(obj[key], list):
                for item in obj[key]:
                    if isinstance(item, dict):
                        # try common keys
                        maybe_add(item.get("name") or item.get("period") or item.get("fecha") or item.get("Fecha"),
                                  item.get("value") or item.get("valor") or item.get("Valor") or item.get("v"))
        # common: {"series":[{"data":[["Ene.2011","2.26"], ...]}]}
        if "series" in obj and isinstance(obj["series"], list):
            for s in obj["series"]:
                if isinstance(s, dict) and "data" in s:
                    d = s["data"]
                    if isinstance(d, list):
                        for row in d:
                            if isinstance(row, list) and len(row) >= 2:
                                maybe_add(row[0], row[1])
                            elif isinstance(row, dict):
                                maybe_add(row.get("name") or row.get("period") or row.get("fecha") or row.get("Fecha"),
                                          row.get("value") or row.get("valor") or row.get("Valor"))
        # recurse into likely containers
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                pairs.extend(_dig_for_pairs(v))

    elif isinstance(obj, list):
        for item in obj:
            pairs.extend(_dig_for_pairs(item))

    return pairs


def _parse_json_points(series_code: str, series_name: str, frequency: str, raw_text: str) -> list[SeriesPoint]:
    t = (raw_text or "").strip()
    if not t:
        return []
    # JSON may contain html entities in titles but that's irrelevant; data is structured.
    try:
        payload = json.loads(t)
    except json.JSONDecodeError:
        return []

    pairs = _dig_for_pairs(payload)

    # de-dup while preserving order
    seen = set()
    points: list[SeriesPoint] = []
    for period, value in pairs:
        key = (series_code, period)
        if key in seen:
            continue
        seen.add(key)
        points.append(SeriesPoint(series_code, series_name, frequency, period, value))

    return points


def fetch_series_points(series_code: str, series_name: str, frequency: str, lookback: int = 120) -> list[SeriesPoint]:
    today = datetime.now(timezone.utc).date()

    def fmt_ymd(d):
        return f"{d.year}-{d.month:02d}-{d.day:02d}"

    def fmt_ym(y: int, m: int):
        # BCRP monthly periods: "YYYY-M" (no zero padding)
        return f"{y}-{m}"

    if frequency == "daily":
        from_date = today.fromordinal(today.toordinal() - lookback)
        from_period = fmt_ymd(from_date)
        to_period = fmt_ymd(today)
    else:
        total = today.year * 12 + (today.month - 1) - lookback
        from_year = total // 12
        from_month = (total % 12) + 1
        from_period = fmt_ym(from_year, from_month)
        to_period = fmt_ym(today.year, today.month)

    csv_url = _build_url(series_code, "csv", from_period, to_period, "esp")
    csv_text = _fetch_text(csv_url)
    pts = _parse_csv_points(series_code, series_name, frequency, csv_text)
    if pts:
        return pts

    json_url = _build_url(series_code, "json", from_period, to_period, "esp")
    json_text = _fetch_text(json_url)
    pts2 = _parse_json_points(series_code, series_name, frequency, json_text)
    return pts2


def insert_points(conn: sqlite3.Connection, points: Iterable[SeriesPoint]) -> int:
    fetched_at = utc_now_iso()
    cur = conn.cursor()
    n = 0
    for p in points:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO bcrp_series_points
                (fetched_at_utc, series_code, series_name, frequency, period, value)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (fetched_at, p.series_code, p.series_name, p.frequency, p.period, p.value),
            )
            if cur.rowcount:
                n += 1
        except sqlite3.Error:
            continue
    conn.commit()
    return n


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    total_new = 0
    for code, name, freq in DEFAULT_SERIES:
        pts = fetch_series_points(code, name, freq, lookback=180 if freq == "monthly" else 120)
        new_rows = insert_points(conn, pts)
        total_new += new_rows
        print(f"✅ BCRP fetched {len(pts)} points for {code} ({freq}); inserted {new_rows} new (total {total_new})")

    conn.close()
    print(f"✅ BCRP series fetch complete. New rows inserted: {total_new}")


if __name__ == "__main__":
    main()
