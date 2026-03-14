#!/usr/bin/env python3
"""
Load test (traffic attack) for Sentinel AI API.
Simulates many clients using X-Client-ID so rate limiter is applied per client.
Run against localhost to see how much traffic the API can handle.
"""

import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("Install: pip install requests")
    raise

DEFAULT_URL = "http://localhost:8000"
DEFAULT_TASK_TYPE = "summarize"
DEFAULT_INPUT = "Load test sample text to summarize."


def post_task(base_url: str, client_id: str, task_type: str, input_text: str) -> tuple[int, float]:
    """POST /tasks with optional X-Client-ID. Returns (status_code, elapsed_sec)."""
    url = f"{base_url.rstrip('/')}/tasks"
    headers = {"Content-Type": "application/json"}
    if client_id:
        headers["X-Client-ID"] = client_id
    payload = {"task_type": task_type, "input_text": input_text}
    start = time.perf_counter()
    try:
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        elapsed = time.perf_counter() - start
        return r.status_code, elapsed
    except Exception as e:
        elapsed = time.perf_counter() - start
        return -1, elapsed  # -1 = connection/other error


def run_attack(
    base_url: str,
    num_clients: int,
    requests_per_client: int,
    concurrency: int,
    task_type: str,
    input_text: str,
) -> dict:
    """Run load test: num_clients virtual IPs, each sending requests_per_client requests."""
    total_requests = num_clients * requests_per_client
    results = {"200": 0, "429": 0, "other": 0, "errors": 0, "times": []}

    def one_request(client_idx: int, req_idx: int) -> tuple[int, float]:
        client_id = f"loadtest-client-{client_idx}"
        return post_task(base_url, client_id, task_type, input_text)

    start_wall = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        for c in range(num_clients):
            for r in range(requests_per_client):
                futures.append(ex.submit(one_request, c, r))
        for f in as_completed(futures):
            status, elapsed = f.result()
            results["times"].append(elapsed)
            if status == 200:
                results["200"] += 1
            elif status == 429:
                results["429"] += 1
            elif status < 0:
                results["errors"] += 1
            else:
                results["other"] += 1
    total_time = time.perf_counter() - start_wall
    results["total_time_sec"] = total_time
    results["rps"] = total_requests / total_time if total_time > 0 else 0
    return results


def main():
    p = argparse.ArgumentParser(description="Load test Sentinel API (use X-Client-ID per client)")
    p.add_argument("--url", default=DEFAULT_URL, help=f"API base URL (default: {DEFAULT_URL})")
    p.add_argument(
        "--num-clients",
        type=int,
        default=5,
        help="Number of virtual clients (different X-Client-ID = different rate-limit buckets)",
    )
    p.add_argument(
        "--requests-per-client",
        type=int,
        default=20,
        help="Requests to send per client",
    )
    p.add_argument(
        "--concurrent",
        type=int,
        default=25,
        help="Max concurrent requests",
    )
    p.add_argument("--task-type", default=DEFAULT_TASK_TYPE, help="task_type in payload")
    p.add_argument("--input", default=DEFAULT_INPUT, help="input_text in payload")
    args = p.parse_args()

    total = args.num_clients * args.requests_per_client
    print(f"Attack: {args.num_clients} clients x {args.requests_per_client} req = {total} requests")
    print(f"URL: {args.url}  concurrency: {args.concurrent}")
    print("Running...")

    r = run_attack(
        args.url,
        args.num_clients,
        args.requests_per_client,
        args.concurrent,
        args.task_type,
        args.input,
    )

    print("\n--- Results ---")
    print(f"  200 OK:        {r['200']}")
    print(f"  429 Limited:   {r['429']}")
    print(f"  Other status:  {r['other']}")
    print(f"  Errors:        {r['errors']}")
    print(f"  Total time:    {r['total_time_sec']:.2f}s")
    print(f"  RPS:           {r['rps']:.1f} req/s")
    if r["times"]:
        r["times"].sort()
        n = len(r["times"])
        print(f"  Latency p50:   {r['times'][n//2]*1000:.0f} ms")
        print(f"  Latency p95:   {r['times'][int(n*0.95)]*1000:.0f} ms")


if __name__ == "__main__":
    main()
