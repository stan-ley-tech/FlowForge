# flowforge (Python SDK)

Client library for defining FlowForge workflows and running workers that
execute them. No dependencies beyond the standard library.

```python
from flowforge import Workflow, RetryPolicy

pipeline = Workflow("order_pipeline")

@pipeline.step("charge", retry=RetryPolicy(max_attempts=3))
def charge(ctx):
    return {"charged": True}

@pipeline.compensate("charge")
def refund(ctx):
    return {"refunded": True}
```

```python
from flowforge import Client, Worker

client = Client("http://localhost:8080")
client.register_workflow(pipeline)
client.start_run("order_pipeline", {"order_id": 42})

Worker(client, pipeline).run()
```

See [examples/order_pipeline.py](../examples/order_pipeline.py) for a
complete runnable workflow, and the top-level project README for how this
fits together with the engine.
