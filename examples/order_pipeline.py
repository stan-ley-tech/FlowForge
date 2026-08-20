"""A ten-step order fulfillment workflow, used both as a runnable example
and as the workflow behind the crash/resume demo (see README.md in this
directory). Every step just simulates work and returns a small result;
the interesting part is what happens to the run when a worker executing
one of these steps disappears.
"""

import os
import time

from flowforge import RetryPolicy, Workflow

pipeline = Workflow("order_pipeline", max_concurrent_runs=0)

# Set FLOWFORGE_DEMO_PAUSE_STEP to a step name and FLOWFORGE_DEMO_PAUSE_SECONDS
# to a duration to make that step sleep before returning - enough time to
# kill -9 the worker process while the step is in flight and lease-held.
_PAUSE_STEP = os.environ.get("FLOWFORGE_DEMO_PAUSE_STEP", "")
_PAUSE_SECONDS = float(os.environ.get("FLOWFORGE_DEMO_PAUSE_SECONDS", "0"))


def _maybe_pause(step_name):
    if step_name == _PAUSE_STEP and _PAUSE_SECONDS > 0:
        print(f"[{step_name}] pausing {_PAUSE_SECONDS}s - kill the worker now to test crash recovery")
        time.sleep(_PAUSE_SECONDS)


@pipeline.step("validate_order", retry=RetryPolicy(max_attempts=3))
def validate_order(ctx):
    _maybe_pause("validate_order")
    order = ctx.input
    if not order.get("items"):
        raise ValueError("order has no items")
    return {"valid": True, "item_count": len(order["items"])}


@pipeline.step("reserve_inventory", retry=RetryPolicy(max_attempts=3))
def reserve_inventory(ctx):
    _maybe_pause("reserve_inventory")
    return {"reserved": ctx.input["items"]}


@pipeline.compensate("reserve_inventory", name="release_inventory")
def release_inventory(ctx):
    _maybe_pause("release_inventory")
    return {"released": ctx.get("reserve_inventory", {}).get("reserved", [])}


@pipeline.step("authorize_payment", retry=RetryPolicy(max_attempts=3), timeout_seconds=20)
def authorize_payment(ctx):
    _maybe_pause("authorize_payment")
    return {"authorization_id": f"auth_{ctx.run_id[:8]}"}


@pipeline.compensate("authorize_payment", name="void_authorization")
def void_authorization(ctx):
    _maybe_pause("void_authorization")
    return {"voided": ctx.get("authorize_payment", {}).get("authorization_id")}


@pipeline.step("capture_payment", retry=RetryPolicy(max_attempts=3), timeout_seconds=20)
def capture_payment(ctx):
    _maybe_pause("capture_payment")
    return {"charged": True}


@pipeline.compensate("capture_payment", name="refund_payment")
def refund_payment(ctx):
    _maybe_pause("refund_payment")
    return {"refunded": True}


@pipeline.step("generate_invoice", retry=RetryPolicy(max_attempts=3))
def generate_invoice(ctx):
    _maybe_pause("generate_invoice")
    return {"invoice_id": f"inv_{ctx.run_id[:8]}"}


@pipeline.step("pack_shipment", retry=RetryPolicy(max_attempts=3), timeout_seconds=60)
def pack_shipment(ctx):
    _maybe_pause("pack_shipment")
    return {"packed": True}


@pipeline.step("dispatch_carrier", retry=RetryPolicy(max_attempts=3))
def dispatch_carrier(ctx):
    _maybe_pause("dispatch_carrier")
    return {"tracking_number": f"trk_{ctx.run_id[:8]}"}


@pipeline.step("notify_customer", retry=RetryPolicy(max_attempts=3))
def notify_customer(ctx):
    _maybe_pause("notify_customer")
    return {"notified": True}


@pipeline.step("update_analytics", retry=RetryPolicy(max_attempts=3))
def update_analytics(ctx):
    _maybe_pause("update_analytics")
    return {"recorded": True}


@pipeline.step("close_order", retry=RetryPolicy(max_attempts=3))
def close_order(ctx):
    _maybe_pause("close_order")
    return {"closed": True}
