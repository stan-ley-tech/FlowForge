import sys

from flowforge import Client


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    items = sys.argv[2:] or ["widget", "gadget"]
    client = Client(base_url)
    run = client.start_run("order_pipeline", {"items": items})
    print(run["id"])


if __name__ == "__main__":
    main()
