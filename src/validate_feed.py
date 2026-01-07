import json
import sys
from pathlib import Path
from datetime import datetime, timezone


ALLOWED_LEVELS = {"Normal", "Watch", "Elevated", "Critical"}


def _fail(errors: list[str]) -> None:
    print("❌ Feed validation failed:")
    for e in errors:
        print(f"- {e}")
    raise SystemExit(1)


def _warn(warnings: list[str]) -> None:
    for w in warnings:
        print(f"⚠️ {w}")


def _parse_dt(value: str) -> datetime | None:
    """
    Accepts ISO strings like:
      2026-01-06T19:12:34-05:00
      2026-01-06T19:12:34+00:00
      2026-01-06T19:12:34Z
    """
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def validate_feed(feed_path: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []

    if not feed_path.exists():
        errors.append(f"Feed file not found: {feed_path}")
        return errors, warnings

    try:
        feed = json.loads(feed_path.read_text(encoding="utf-8"))
    except Exception as e:
        errors.append(f"Feed is not valid JSON: {e}")
        return errors, warnings

    # --- Feed-level required fields ---
    for key in ("schema_version", "site_id", "site_name", "generated_at_local", "cards"):
        if key not in feed:
            errors.append(f"Missing top-level field: '{key}'")

    if errors:
        return errors, warnings

    if not isinstance(feed["cards"], list):
        errors.append("Top-level 'cards' must be a list.")
        return errors, warnings

    # Feed timestamp sanity (WARN only)
    feed_dt = _parse_dt(feed.get("generated_at_local"))
    if feed_dt is None:
        warnings.append("Top-level 'generated_at_local' is missing or not a valid ISO datetime.")
    else:
        now_utc = datetime.now(timezone.utc)
        feed_dt_utc = feed_dt.astimezone(timezone.utc) if feed_dt.tzinfo else feed_dt.replace(tzinfo=timezone.utc)
        if feed_dt_utc > now_utc.replace(microsecond=0) + timedelta_minutes(5):
            warnings.append("Top-level 'generated_at_local' appears to be >5 minutes in the future (clock drift?).")

    # --- Card-level checks ---
    seen_ids: set[str] = set()
    required_card_fields = [
        ("generated_at_local",),
        ("insight",),
        ("insight", "id"),
        ("insight", "title"),
        ("insight", "category"),
        ("insight", "status"),
        ("insight", "status", "level"),
    ]

    for i, card in enumerate(feed["cards"]):
        if not isinstance(card, dict):
            errors.append(f"Card[{i}] is not an object/dict.")
            continue

        # Required nested fields
        for path in required_card_fields:
            cur = card
            ok = True
            for p in path:
                if not isinstance(cur, dict) or p not in cur:
                    ok = False
                    break
                cur = cur[p]
            if not ok:
                errors.append(f"Card[{i}] missing field: {'.'.join(path)}")

        # Only proceed if insight exists
        insight = card.get("insight") if isinstance(card.get("insight"), dict) else None
        if not insight:
            continue

        insight_id = insight.get("id")
        if not isinstance(insight_id, str) or not insight_id.strip():
            errors.append(f"Card[{i}] insight.id must be a non-empty string.")
        else:
            if insight_id in seen_ids:
                errors.append(f"Duplicate insight.id in feed: '{insight_id}'")
            seen_ids.add(insight_id)

        # Status level normalization
        status = insight.get("status") if isinstance(insight.get("status"), dict) else None
        if status:
            level = status.get("level")
            if isinstance(level, str):
                if level not in ALLOWED_LEVELS:
                    errors.append(
                        f"Card[{i}] insight.status.level '{level}' is not allowed. Allowed: {sorted(ALLOWED_LEVELS)}"
                    )
            else:
                errors.append(f"Card[{i}] insight.status.level must be a string.")

            # Score sanity (optional)
            if "score_0_100" in status:
                score = status.get("score_0_100")
                if not isinstance(score, int):
                    errors.append(f"Card[{i}] insight.status.score_0_100 must be an integer (0–100).")
                elif score < 0 or score > 100:
                    errors.append(f"Card[{i}] insight.status.score_0_100 out of range (0–100): {score}")

        # Card timestamp sanity (WARN only)
        cdt = _parse_dt(card.get("generated_at_local"))
        if cdt is None:
            warnings.append(f"Card[{i}] generated_at_local missing or invalid ISO datetime.")
        else:
            now_utc = datetime.now(timezone.utc)
            cdt_utc = cdt.astimezone(timezone.utc) if cdt.tzinfo else cdt.replace(tzinfo=timezone.utc)
            if cdt_utc > now_utc.replace(microsecond=0) + timedelta_minutes(5):
                warnings.append(f"Card[{i}] generated_at_local appears >5 minutes in the future (clock drift?).")

    # Must have at least 1 card (you can tighten this later)
    if len(feed["cards"]) == 0:
        errors.append("Feed contains 0 cards.")

    return errors, warnings


def timedelta_minutes(n: int):
    # small helper to avoid importing timedelta everywhere
    from datetime import timedelta
    return timedelta(minutes=n)


def main() -> None:
    # Default feed path
    feed_path = Path("outputs") / "feed.json"
    if len(sys.argv) == 2:
        feed_path = Path(sys.argv[1])

    errors, warnings = validate_feed(feed_path)

    if warnings:
        _warn(warnings)

    if errors:
        _fail(errors)

    # PASS summary
    feed = json.loads(Path(feed_path).read_text(encoding="utf-8"))
    print("✅ Feed validation passed.")
    print(f"✅ site_id: {feed.get('site_id')} | cards: {len(feed.get('cards', []))}")
    raise SystemExit(0)


if __name__ == "__main__":
    main()
