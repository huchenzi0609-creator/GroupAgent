"""Synthetic credential boundaries; no real credentials or provider calls."""

import pytest
from pydantic import SecretStr

from groupagent.security import redact_credentials


@pytest.mark.parametrize("prefix", ["sk-", "ghp_", "github_pat_", "xoxb-", "xoxp-"])
@pytest.mark.parametrize(
    "before,after", [("凭据", "请隐藏"), ("", "中文"), ("中文", ""), (" ", " ")]
)
def test_synthetic_credential_is_redacted_when_touching_chinese(prefix, before, after):
    credential = prefix + "synthetic_only_" * 3
    assert redact_credentials(before + credential + after) == before + "[凭据已隐藏]" + after


def test_other_redaction_rules_and_ordinary_identifiers_are_preserved():
    assert redact_credentials("合成报告编号COURSE-17") == "合成报告编号COURSE-17"
    private_key = "-----BEGIN PRIVATE KEY-----\nsynthetic placeholder\n-----END PRIVATE KEY-----"
    assert redact_credentials(private_key) == "[凭据已隐藏]"
    assert redact_credentials("https://fixture:synthetic-pass@example.org") == (
        "https://[凭据已隐藏]example.org"
    )
    assert redact_credentials(
        "资料custom-fixture-value结束", (SecretStr("custom-fixture-value"),)
    ) == ("资料[凭据已隐藏]结束")
