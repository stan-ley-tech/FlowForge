import sys

from flowforge import Client
from order_pipeline import pipeline


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    client = Client(base_url)
    definition = client.register_workflow(pipeline)
    print(f"registered {definition['name']} v{definition['version']} ({len(definition['steps'])} steps)")


if __name__ == "__main__":
    main()
