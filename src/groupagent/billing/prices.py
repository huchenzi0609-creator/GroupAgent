"""Explicit conservative price snapshot, not an automatic spend authorization.

Official source accessed 2026-11-14:
https://api-docs.deepseek.com/zh-cn/quick_start/pricing/
Peak/cache-miss and undiscounted speech rates are used as upper estimates.
Actual provider billing may be lower; usage evidence and this snapshot are kept.
"""

from datetime import UTC, datetime
from decimal import Decimal

from .models import PriceSnapshot


def deepseek_20261114() -> tuple[PriceSnapshot, ...]:
    rates = {
        "text": {"input_tokens": "0.000002", "output_tokens": "0.000008"},
        "media.vision": {"input_tokens": "0.000002", "output_tokens": "0.000008"},
        "media.ocr": {"pages": "0.0015"},
        "media.asr": {"audio_seconds": "0.000216667"},
        "media.tts": {"characters": "0.0003"},
        "web.search": {"calls": "0.005"},
        "web.fetch": {"calls": "0.005"},
    }
    return tuple(
        PriceSnapshot(
            id="deepseek-20261114-" + operation,
            provider="deepseek",
            operation=operation,
            currency="CNY",
            rates={key: Decimal(value) for key, value in unit_rates.items()},
            source_url="https://api-docs.deepseek.com/zh-cn/quick_start/pricing/",
            captured_at=datetime(2026, 11, 13, 19, 48, 31, tzinfo=UTC),
            conservative=True,
        )
        for operation, unit_rates in rates.items()
    )
