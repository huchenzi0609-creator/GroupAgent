# Optional public research adapter

`DeepSeekResearchClient(api_key, client=None, resolver=...)` implements async
`search(SearchInput)` and `fetch(FetchInput)`. Credentials must be supplied
explicitly. Construction and `register_research_tools(gateway, client)` do not
make requests or expand a work's `allowed_tools`. Enable `web_search` and/or
`web_fetch` in a trusted work grant to use the read-only tools.

HTTP requests target only `https://api.deepseek.com/v1/tools/search` and
`https://api.deepseek.com/v1/tools/fetch`. Redirects are disabled, including when
an injected HTTP client enables them by default. Provider credentials never go
to the target page origin. The adapter does not perform a local target-page
HTTP request or follow target links.

Target URLs and explicit URL/site search operands must use public, standard-port
HTTP(S) addresses without credentials. Private/special IPs, local hostnames,
ambiguous numeric addresses, and DNS answers containing non-public addresses are
rejected before provider requests. DNS validation does **not** pin the remote
provider's resolution: its security policy and downstream redirect handling still
need separate acceptance evidence. This prevents the local arbitrary-URL-fetch
class of DNS rebinding; it does not claim to audit the provider's infrastructure.

Outputs preserve actual source URLs, UTC retrieval times, provider operation,
publication timestamps when supplied, summary/page evidence kinds, byte counts,
and explicit truncation fields. Search summaries remain search observations.
Fetch returns no final URL, so the adapter does not invent redirect provenance.
`content_sha256` hashes returned content only; a truncated source is not described
as a complete read. Empty searches return no sources. All source text is marked
untrusted, and gateway authorization still applies to subsequent model calls.

Default limits: 128 query characters, 10 results, 20 seconds per entire operation,
2 MB decoded response body, 256 KB normalized output, 16 KB page text per search result,
and 100 KB per fetched page. The tool gateway imposes its independent work budget.
There are no implicit retries. Timeout, rate-limit, authentication, insufficient
balance, unavailable-page and policy errors remain distinct and sanitized.
`cost_meter` identifies provider request accounting; read-only does not mean free.
Daily monetary budgets remain a control-plane responsibility.

Protocol references checked 2026-11-13: [Search and Fetch guide](https://api-docs.deepseek.com/guides/search/),
[Search API](https://api-docs.deepseek.com/api/search/), and
[Fetch API](https://api-docs.deepseek.com/api/fetch/).
Tests use synthetic data, injected DNS results and `httpx.MockTransport`;
no real credential, paid API call or live provider quality test was used.

Validation on 2026-11-13: 67 research tests and 2 Harness probe tests passed;
Ruff lint/format and mypy passed for the changed code. Reproduce with:

```sh
.venv/bin/pytest tests/research tests/execution/test_harness_probe.py -q
.venv/bin/ruff check src/groupagent/research tests/research
.venv/bin/mypy src/groupagent/research
```
