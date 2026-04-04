# FILE: engine/core_billing/billing_processor.py
# DATE: 2026-04-04
# PURPOSE: Dedicated worker for billing-based audience task active recalculation.

from engine.common.worker import Worker
from engine.core_billing import billing

TASK_TIMEOUT_SEC = 120


def main() -> None:
    w = Worker(
        name="core_billing_processor",
        tick_sec=1,
        max_parallel=1,
    )

    w.register(
        name="billing_run_once",
        fn=billing.run_once,
        every_sec=5,
        timeout_sec=TASK_TIMEOUT_SEC,
        singleton=True,
        heavy=False,
        priority=10,
    )

    w.run_forever()


if __name__ == "__main__":
    main()
