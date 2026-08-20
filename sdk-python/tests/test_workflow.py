from flowforge import RetryPolicy, Workflow


def build_workflow():
    wf = Workflow("order_pipeline", max_concurrent_runs=5)

    @wf.step("validate", retry=RetryPolicy(max_attempts=2))
    def validate(ctx):
        return {"valid": True}

    @wf.step("charge", timeout_seconds=45)
    def charge(ctx):
        return {"charged": True}

    @wf.compensate("charge", name="refund")
    def refund(ctx):
        return {"refunded": True}

    return wf, validate, charge, refund


def test_steps_register_in_declaration_order():
    wf, validate, charge, refund = build_workflow()
    assert [s.name for s in wf.steps] == ["validate", "charge"]
    assert wf.steps[0].fn is validate
    assert wf.steps[1].fn is charge
    assert wf.steps[1].timeout_seconds == 45


def test_compensation_is_kept_separate_from_forward_steps():
    wf, *_ = build_workflow()
    assert [c.name for c in wf.compensations] == ["refund"]
    assert wf.compensations[0].compensation_of == "charge"


def test_handler_for_looks_up_by_name_and_kind():
    wf, validate, charge, refund = build_workflow()
    assert wf.handler_for("validate", is_compensation=False).fn is validate
    assert wf.handler_for("refund", is_compensation=True).fn is refund
    assert wf.handler_for("charge", is_compensation=False).fn is charge
    assert wf.handler_for("missing", is_compensation=False) is None


def test_to_definition_matches_engine_schema():
    wf, *_ = build_workflow()
    definition = wf.to_definition()

    assert definition["name"] == "order_pipeline"
    assert definition["max_concurrent_runs"] == 5
    assert definition["steps"][0]["name"] == "validate"
    assert definition["steps"][0]["retry"]["max_attempts"] == 2
    assert definition["compensations"][0]["compensation_of"] == "charge"
    assert "compensation_of" not in definition["steps"][0]
