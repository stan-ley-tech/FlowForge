import sys

from flowforge import Client, Worker
from order_pipeline import pipeline


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    client = Client(base_url)
    worker = Worker(client, pipeline, poll_interval=0.5)
    print(f"worker {worker.worker_id} polling {base_url} for {pipeline.name}")
    worker.run()


if __name__ == "__main__":
    main()
