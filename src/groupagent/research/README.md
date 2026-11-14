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
Search additionally shares a 16,000-byte UTF-8 budget across all returned titles,
snippets, summaries and content, divided equally among admitted results; original
prefixes carry explicit truncation markers while URLs, timestamps and provenance
are retained, and the normalized-output hard limit still applies.
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

## Budget and controlled browser extension (2026-11-14)

The production registrar now requires
`register_research_tools(gateway, client, budget=ledger,
price_ids={"search": ..., "fetch": ...})`. Each real operation reserves and begins
before HTTP, then settles one `calls` unit under `web.search` or `web.fetch`.
Failure or cancellation keeps an unknown reservation. Stable attempt keys and
the gateway's durable receipt prevent automatic duplicate paid calls. The older
budget-free registrar form remains only for an explicit mock HTTP transport.
A read-only tool grant is not an exemption from the monetary budget.

`PublicPageBroker(allowed_hosts, resolver=..., client=...)` provides a separate
controlled host fetch for public HTML. Every redirect needs an approved host and
public DNS answers; the connection pins an already checked IP with the original
Host/SNI. It forwards no credentials, auth hook or cookies, including those on a
borrowed client. Video URLs, non-HTML content, oversized pages, login denials and
CAPTCHA challenges stop clearly. It never downloads a video or bypasses a challenge.

`ControlledBrowser(broker, isolated_worker).open(context, url)` passes the bounded
HTML to a real Chromium worker under the same network-disabled Docker limits.
The implemented profile is static HTML: scripts, active embeds, audio/video,
frames and remote resources are removed; JavaScript is disabled. Inline raster
images are decoded into static PNGs inside the worker. This produces real DOM
text plus a screenshot and content hashes, not a claim that interactive/login
websites are supported. Browser pages and screenshots remain untrusted data.
The tool registrar can persist the screenshot as a source-associated `MediaRef`.
This direct public fetch has no provider charge; choosing the paid DeepSeek fetch
route still requires its ledger above.

The real Chromium cases passed on immutable worker image
`sha256:766cfdf7992d6b83b05e7c2d16059f1522cd3aa94a4d4acedaae715731047e6a`
with `libatomic1`, a private temporary Chromium home, zero browser egress and
static-content sanitization. The tests verify rendered text/screenshot, blocked
scripts and network attempts, and challenge detection. Broker URL/DNS/redirect,
credential-forwarding and budget tests use deterministic HTTP/DNS fixtures;
they do not establish live source coverage or permission to bypass website rules.
