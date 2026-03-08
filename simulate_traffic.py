from __future__ import annotations

import argparse
import json
import random
import time
import urllib.error
import urllib.request


def post_event(base_url: str, payload: dict, timeout: float = 5.0) -> tuple[bool, str]:
    url = f"{base_url.rstrip('/')}/track"
    request = urllib.request.Request(
        url=url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return True, body
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return False, f"HTTP {exc.code}: {body}"
    except Exception as exc:
        return False, str(exc)


def build_events(
    viewed_count: int,
    purchased_count: int,
    user_start: int,
    user_end: int,
    product_start: int,
    product_end: int,
) -> list[dict]:
    users = [f"user_{i}" for i in range(user_start, user_end + 1)]
    products = [f"product_{i}" for i in range(product_start, product_end + 1)]

    events: list[dict] = []
    for _ in range(viewed_count):
        events.append(
            {
                "user_id": random.choice(users),
                "product_id": random.choice(products),
                "action": "VIEWED",
            }
        )
    for _ in range(purchased_count):
        events.append(
            {
                "user_id": random.choice(users),
                "product_id": random.choice(products),
                "action": "PURCHASED",
            }
        )
    random.shuffle(events)
    return events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulate user interaction traffic.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--viewed", type=int, default=100)
    parser.add_argument("--purchased", type=int, default=20)
    parser.add_argument("--user-start", type=int, default=2)
    parser.add_argument("--user-end", type=int, default=10)
    parser.add_argument("--product-start", type=int, default=1)
    parser.add_argument("--product-end", type=int, default=50)
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=30,
        help="Optional pause between requests to keep logs readable.",
    )
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    events = build_events(
        viewed_count=args.viewed,
        purchased_count=args.purchased,
        user_start=args.user_start,
        user_end=args.user_end,
        product_start=args.product_start,
        product_end=args.product_end,
    )

    success = 0
    failed = 0

    print(
        f"Sending {len(events)} events to {args.base_url}/track "
        f"(VIEWED={args.viewed}, PURCHASED={args.purchased})"
    )
    for i, event in enumerate(events, start=1):
        ok, details = post_event(args.base_url, event)
        if ok:
            success += 1
        else:
            failed += 1
            print(f"[{i}/{len(events)}] failed {event} -> {details}")
        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    print(f"Done. Success={success}, Failed={failed}")


if __name__ == "__main__":
    main()
