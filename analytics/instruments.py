import json
from pathlib import Path


INSTRUMENTS_PATH = Path(__file__).with_name("instruments.json")
REQUIRED_MODES = ("primary", "secondary", "third", "fourth", "core_one", "core_two")


def _load_instrument_groups() -> dict[str, dict[str, dict]]:
    with INSTRUMENTS_PATH.open("r", encoding="utf-8") as handle:
        raw_data = json.load(handle)

    groups: dict[str, dict[str, dict]] = {}
    symbol_to_mode: dict[str, str] = {}

    for mode in REQUIRED_MODES:
        raw_group = raw_data.get(mode)
        if not isinstance(raw_group, dict):
            raise ValueError(f"Missing instrument group: {mode}")

        normalized_group: dict[str, dict] = {}
        for display_name, info in raw_group.items():
            if not isinstance(info, dict):
                raise ValueError(f"Instrument entry for {display_name!r} in {mode} must be an object")

            symbol = str(info.get("symbol", "")).strip().upper()
            instrument_type = str(info.get("type", "")).strip().lower()
            market = str(info.get("market", "")).strip().upper()
            priority = int(info.get("priority", 0))

            if not display_name or not symbol or not instrument_type or not market or priority <= 0:
                raise ValueError(f"Invalid instrument entry for {display_name!r} in {mode}")

            if symbol in symbol_to_mode:
                raise ValueError(
                    f"Duplicate symbol {symbol!r} found in both {symbol_to_mode[symbol]!r} and {mode!r}"
                )

            symbol_to_mode[symbol] = mode
            normalized_group[display_name] = {
                "symbol": symbol,
                "type": instrument_type,
                "priority": priority,
                "market": market,
            }

        groups[mode] = normalized_group

    return groups


INSTRUMENT_GROUPS = _load_instrument_groups()
PRIMARY_INSTRUMENTS = INSTRUMENT_GROUPS["primary"]
SECONDARY_INSTRUMENTS = INSTRUMENT_GROUPS["secondary"]
THIRD_INSTRUMENTS = INSTRUMENT_GROUPS["third"]
FOURTH_INSTRUMENTS = INSTRUMENT_GROUPS["fourth"]
CORE_ONE_INSTRUMENTS = INSTRUMENT_GROUPS["core_one"]
CORE_TWO_INSTRUMENTS = INSTRUMENT_GROUPS["core_two"]
