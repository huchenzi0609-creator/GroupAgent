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
When content is requested, metadata has separate shares so a long snippet cannot
consume the entire body allowance.
The low-level client performs no implicit retries. Timeout, rate-limit,
authentication, insufficient balance, unavailable-page and policy errors remain
distinct and sanitized.
`cost_meter` identifies provider request accounting; read-only does not mean free.
Daily monetary budgets remain a control-plane responsibility.

## Received evidence and research sessions (2026-11-16)

`register_research_tools(...)` now returns a `ResearchSession`. Its optional
`session=` argument accepts `ResearchSession(client, EvidenceStore(root),
question=work.goal)`; alternatively pass `evidence_store=`. Existing calls keep
working and use `.research-evidence` inside the scoped work workspace. Registration
does not add any tool to a work grant. `web_read` is a separately granted local
continuation tool alongside `web_search` and `web_fetch`.

The host-only `search_received`/`fetch_received` transport methods preserve
received source bodies under the existing 2,000,000-byte HTTP response guard.
The session saves immutable UTF-8 blobs before projecting model-visible text.
The default scope storage bound is 32,000,000 bytes. Receipt `evidence_ref`
manifests bind group/work/member, source URL, operation, evidence kind, retrieval
time and full-body hash; they contain no model-chosen filesystem paths. Source
identities and byte hashes are checked on restore and every local/cache read,
and symlinks are refused. The response's normal `content_sha256` still hashes
only the actual returned excerpt; `evidence_ref.content_sha256` hashes the saved
received body. Neither proves that an upstream page was complete.

Successful reads cache by actual normalized provider arguments and canonical
URL, rather than model call ID. Concurrent duplicate requests join one in-flight
read. Each invocation still passes current gateway authorization; a hit returns
the original retrieval time, citation and actual fee receipt without another
HTTP request or reservation. Failures, unknown fees and failed attempts are not
cached or refunded. `refresh=true` explicitly requests a new external observation;
it is a session control and is never forwarded as a provider API parameter.
Restoration uses successful trusted gateway receipts and their manifests, so
JSON key order has no semantic effect and no new business database is required.

Model projections share a deterministic CJK/identifier passage ranker with
`project_research_sources`, the independent review projection. Search text has
per-field shares and a collection bound; fetching keeps a small relevant view.
`web_read(source_id, query=...)` locates other saved passages without network
access. Without a query, `offset` and `limit` are UTF-8 byte positions/bounds and
`next_offset` permits exact continuation. A saved search body remains a search
observation even when read locally. Stored content remains untrusted data.

`tests/research/test_session.py` verifies cache/singleflight fee accounting,
JSON-order-independent restart, actual citation preservation, local tail reads,
Chinese ranking, field/UTF-8 bounds, explicit refresh, independent partial failure,
fresh authorization, work separation, tampering and symlink rejection using only
synthetic data and `httpx.MockTransport`. This is offline evidence, not live web
quality, native delivery or production acceptance.

Protocol references checked 2026-11-13: [Search and Fetch guide](https://api-docs.deepseek.com/guides/search/),
[Search API](https://api-docs.deepseek.com/api/search/), and
[Fetch API](https://api-docs.deepseek.com/api/fetch/).
Tests use synthetic data, injected DNS results and `httpx.MockTransport`;
no real credential, paid API call or live provider quality test was used.

Validation on 2026-11-13: 67 research tests and 2 Harness probe tests passed;
Ruff lint/format and mypy passed for the changed code. Reproduce with:

```sh
.venv/bin/pytest tests/research tests/execution/test_langgraph_core.py -q
.venv/bin/ruff check src/groupagent/research tests/research
.venv/bin/mypy src/groupagent/research
```

## Budget and controlled browser extension (2026-11-14)

The production registrar now requires
`register_research_tools(gateway, client, budget=ledger,
price_ids={"search": ..., "fetch": ...})`. Each real operation reserves and begins
before HTTP, then settles one `calls` unit under `web.search` or `web.fetch`.
Failure or cancellation keeps an unknown reservation. The registrar retries only
transient timeout, rate-limit and provider-unavailable read failures, with one
additional attempt by default and a hard maximum of two additional attempts.
Each retry rechecks current authorization and public URL policy, reserves and
begins a distinct `:retry:N` budget attempt, and retains every earlier unknown
fee. Backoff and requests share the work deadline; budget denial, cancellation,
revocation, unavailable pages and permanent provider errors stop that read.
Public reads use `READ_ONLY` execution receipts: an unavailable source is safe for
the model to replace or omit while producing an explicitly partial result, even
when its provider charge remains unknown. Mutable tools retain their separate
reconciliation fences. Stable attempt keys and the gateway's durable receipt
prevent automatic duplicate paid calls after replay. The older
budget-free registrar form remains only for an explicit mock HTTP transport.
A read-only tool grant is not an exemption from the monetary budget.

Read recovery validation on 2026-11-16: 225 offline research/gateway/engine tests
passed, plus 22 isolated PostgreSQL research/billing tests. Synthetic HTTP cases
cover transient recovery, 404 and empty-page replacement, replay, current-policy
revocation, deadlines and denied retry funding. Real PostgreSQL verifies that an
unknown fee survives restart, a separately funded retry settles independently,
and daily 50 CNY / total 20 CNY fixture caps remain unchanged. Existing mutable
tool uncertainty fences are included in the offline suite. Ruff and research
mypy passed. These checks made no real provider request or native channel send.

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

## Existing public images (2026-11-16)

`PublicImageService(media, resolver=..., client=None)` and
`register_public_image_tools(gateway, service, source_factory=...)` add two
separately granted tools. After the existing budgeted `web_search` finds a page,
`web_image_candidates(page_url, limit=10)` extracts inert `img`/`srcset`, lazy
image URLs, direct image links and Open Graph/Twitter metadata. It returns exact
candidate URLs, page URL/hash, actual UTC access time, and explicitly unverified
dimension hints. It does not execute scripts or fetch subresources. The next
`web_image_import(candidate_id)` accepts only an ID observed by that same
group/work/actor and returns a flat `PublicImageRef(MediaRef)`, with verified dimensions, usable
for native image delivery or document generation with embedded images.
Retrieving an existing public image is independent of the disabled external
image-generation capability and adds no new provider or paid search path.

The transport reuses the public-page broker's checked public DNS/IP pinning,
standard-port URL policy, credential-free requests and redirect checks. Its two
fixed content profiles accept HTML for discovery and JPEG/PNG/WebP/GIF for
import. Private/ambiguous targets, credentials, non-image MIME, authentication
denials, access challenges and encoded responses fail explicitly. Every redirect
is checked again, including a public image CDN redirect. Owned HTTP clients close
after each request operation. The default discovery bound is 1 MiB per page and
twenty returned candidate IDs per observation. Resident metadata uses LRU frames
of six pages, 100 candidates and 100 imported references. Eviction is not a task
quota: the registrar restores an evicted requested page/candidate and its validated
import from scoped trusted receipts without another HTTP request or decode.
Default cumulative image, download and discovery quotas are disabled (`None`).
Owners may explicitly set positive `max_images`, `max_download_attempts` and
`max_discovery_attempts`; those quotas still account for actual failed attempts
and survive receipt restoration. The media service's configured image count
bounds a single analysis batch, not all imports in a work. Single-image byte and
pixel bounds remain at most 15 MiB and 40 MP. Imports run serially and share the
existing isolated worker. No generic binary-download tool is exposed.

Untrusted image bytes go through `MediaService.ingest_image` and the existing
`image_validate` sandbox operation before artifact registration. Declared MIME
must match the worker's decoded format. Original bytes and SHA-256 remain intact;
GIF metadata marks a static first-frame view. Host-built `public_image`
provenance persists the requested/final image URLs, page URL/hash, retrieval time,
actual MIME, dimensions and hash in a namespace that cannot replace the trusted
media source or scope. Page metadata does not prove image relevance, high
resolution or a reuse license.
The result includes `page_citation` and `image_citation`, computed from the stored
page/image UTC retrieval times. Discovery also reports the actual
`html_bytes_received`. Direct observations use `ResearchSource` provider
`public_http` and evidence kinds `public_image_page`/`public_image` when composed
into the runtime's citation validator.

The registrar restores candidate IDs and completed imports from
`gateway.export_ledger(context.scope)`, using the engine's existing trusted
checkpoint. Receipt types, successful tool names, scope fingerprint, page
provenance, canonical public URLs, limits and exact reconstructed citation strings
are checked before use. A restart or
resource wait can continue with the issued ID; a different work/member or a
model-written workspace cache cannot grant an import. Existing imported versions
are checked against stored media references and reused instead of downloading
again. Failed/revoked sources and changed artifact bytes still stop.
Recovery counts confirmed HTTP/decoder failure codes toward attempt budgets;
limit, admission and derivative recovery failures did not start another request.
Budget exhaustion preserves valid earlier evidence and cached image reuse.
Historical failed receipts are read as recorded and never rewritten.

Tests in `tests/research/test_public_images.py` use synthetic fictional frog
rasters, injected DNS and HTTP transports. They cover source/hash preservation,
two-image Word structure, original formats, private/mixed/rebound DNS, redirect
checks, absent credentials/cookies, chunked byte caps, MIME/pixel failures,
inert discovery, work/actor scope, receipt recovery and source withdrawal. The
RecordingGateway regression uses an in-memory registry fixture to check that two
successful imports, MIME failures, final selections and historical limit/recovery
errors survive repeated restoration. Separate tests verify replacement images,
distinct-image cardinality and explicit page/download quotas across restarts.
The default profile also verifies thirteen failed candidate downloads followed
by ten distinct imports while the analysis batch remains four and the decoder
peak remains one. A deliberately small LRU frame verifies bounded metadata and
receipt-based reuse of evicted targets across eight pages and a restart. The
fixture worker calls the real decoder in process only for these generated test
bytes; this is not new Docker isolation, live web coverage, rendered Word or
native channel acceptance evidence. No live provider request or production data
was used for this change.
