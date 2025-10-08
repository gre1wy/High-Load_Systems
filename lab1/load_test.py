# File: load_test.py
import requests
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
DEFAULT_BASE_URL = "http://localhost:8080"
DEFAULT_REQUESTS_PER_CLIENT = 10000
DEFAULT_CLIENT_LOADS = [1, 2, 5, 10]

def reset_counter(base_url):
    """Sends a request to /reset to reset the counter on the server."""
    try:
        response = requests.get(f"{base_url}/reset", timeout=5)
        response.raise_for_status()
        print("Counter successfully reset to 0.")
        return True
    except requests.RequestException as e:
        print(f"Failed to reset counter: {e}")
        return False

def make_requests(client_id, base_url, requests_per_client):
    """
    Thread function: performs a given number of /inc requests.
    Returns the number of errors that occurred during execution.
    """
    error_count = 0
    with requests.Session() as session:
        for _ in range(requests_per_client):
            try:
                # Set a short timeout to avoid waiting forever
                response = session.get(f"{base_url}/inc", timeout=2)
                response.raise_for_status()
            except requests.RequestException:
                error_count += 1
    return error_count

def get_final_count(base_url):
    """Gets the final value of /count"""
    try:
        response = requests.get(f"{base_url}/count", timeout=5)
        response.raise_for_status()
        return int(response.text.strip())
    except (requests.RequestException, ValueError) as e:
        print(f"Failed to get the final count: {e}")
        return "ERROR"

def run_test(num_clients, base_url, requests_per_client):
    """Runs a load test with N clients"""
    print(f"\n--- Test: {num_clients} Client(s) ---")
    
    start_time = time.time()
    total_errors = 0
    
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        # Create tasks for each client
        futures = [executor.submit(make_requests, i, base_url, requests_per_client) for i in range(num_clients)]
        
        # Collect results as they complete
        for future in as_completed(futures):
            total_errors += future.result()

    end_time = time.time()
    total_time = end_time - start_time
    total_requests = num_clients * requests_per_client
    
    throughput = total_requests / total_time
    final_count = get_final_count(base_url)
    
    print(f"Expected Count: {total_requests:,}")
    print(f"Actual Count:   {final_count:,}" if isinstance(final_count, int) else f"Actual Count:   {final_count}")
    print(f"Total Time:     {total_time:.4f} s")
    print(f"Throughput:     {throughput:,.2f} req/s")
    print(f"Total Errors:   {total_errors:,}")
    
    # Correctness check
    if isinstance(final_count, int) and final_count != total_requests:
         print("WARNING: Actual and expected counts do not match! Possible race condition on the server.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="A script for load testing a web counter.")
    parser.add_argument("--url", default=DEFAULT_BASE_URL, help="Server URL, e.g., http://localhost:8080")
    parser.add_argument("--reqs", type=int, default=DEFAULT_REQUESTS_PER_CLIENT, help="Number of requests per client")
    parser.add_argument("--clients", nargs='+', type=int, default=DEFAULT_CLIENT_LOADS, help="List of client loads, e.g., 1 5 10")
    args = parser.parse_args()

    print("--- Starting Load Test ---")
    print(f"Server: {args.url}")
    print(f"Requests per client: {args.reqs:,}")
    print(f"Load levels (clients): {args.clients}")

    for client_count in args.clients:
        # 1. Reset the counter before each test
        if not reset_counter(args.url):
            break # Abort if reset fails

        # 2. Brief pause to ensure the server is ready
        time.sleep(1)

        # 3. Run the test
        run_test(client_count, args.url, args.reqs)

    print("\n--- Testing Complete ---")