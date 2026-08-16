import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ESIOS_INDICATORS_CONFIG = PROJECT_ROOT / "config" / "esios_indicators.json"


def load_esios_indicators(group: str) -> dict[int, str]:
    with ESIOS_INDICATORS_CONFIG.open(
        "r",
        encoding="utf-8",
    ) as file:
        config = json.load(file)

    indicators = config.get(group)

    if not isinstance(indicators, dict):
        raise ValueError(
            f"Invalid or missing ESIOS indicator group: {group}"
        )

    return {
        int(indicator_id): dataset
        for indicator_id, dataset in indicators.items()
    }