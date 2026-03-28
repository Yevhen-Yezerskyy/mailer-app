# FILE: engine/core_crawler/processor.py
# DATE: 2026-03-26
# PURPOSE: Dedicated browser broker process for core_crawler.

from engine.core_crawler.browser.broker_server import run_browser_broker


def main() -> None:
    run_browser_broker()


if __name__ == "__main__":
    main()
