from __future__ import annotations

import os

from brokers.base import PaperBroker, PaperBrokerConfig


def get_paper_broker_name() -> str:
    return str(os.getenv("PAPER_BROKER", "alpaca")).strip().lower() or "alpaca"


def get_paper_broker_config() -> PaperBrokerConfig:
    return PaperBrokerConfig(
        broker_name=get_paper_broker_name(),
        shadow_mode_enabled=str(os.getenv("ENABLE_IBKR_SHADOW_MODE", "false")).strip().lower() in {"1", "true", "yes", "y", "on"},
        market_data_compare_enabled=str(os.getenv("ENABLE_IBKR_MARKET_DATA_COMPARE", "false")).strip().lower() in {"1", "true", "yes", "y", "on"},
    )


def get_paper_broker() -> PaperBroker:
    broker_name = get_paper_broker_name()
    if broker_name == "alpaca":
        from brokers.alpaca_adapter import AlpacaPaperBroker

        return AlpacaPaperBroker()
    if broker_name == "ibkr":
        from brokers.ibkr_adapter import IbkrPaperBroker

        return IbkrPaperBroker()
    raise ValueError(f"Unsupported PAPER_BROKER '{broker_name}'")
