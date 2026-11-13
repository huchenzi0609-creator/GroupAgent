# Trusted persistence capabilities

Only the trusted control service holds the PostgreSQL connection. Models and workers do not construct `ActorContext`, choose policy grants, or receive a store connection.

## Scoped text replies

`tx.enqueue_reply(request_event_id, text, read_only_authorized=False)` creates one text response to a persisted event in the transaction's group. The event actor must exactly match the context member, including for an owner caller. The event must have verified channel provenance and cannot be a system, membership or recall event.

Members and owners may use this narrow reply method. Observers and maintainers additionally require `read_only_authorized=True`, supplied by trusted group policy. A model's candidate `reply` decision is not itself the policy grant. The actor role remains unchanged.

The server constructs the `text:` payload, SHA-256 digest and fixed `reply:{event_id}:1` key. The outbox retains a group-scoped foreign key to the source event, with database constraints restricting this capability to one text-only intent per request. Callers cannot choose a recipient, work ID, artifact payload, hash or alternative retry key through this method. The original `enqueue_delivery` retains its member/owner permission requirement.

An observer or maintainer may claim, finish, cancel while pending, or reconcile only its own bound text reply. The same role cannot dispatch an unbound standalone intent or a work artifact. Dispatch still checks policy revision, and uncertain outcomes still require reconciliation using the original intent.

## Transport checkpoints without synthetic messages

`tx.advance_cursor(stream_id, cursor, expected_cursor_revision=..., reason="channel_batch_complete")` is reserved for an owner or maintainer identity supplied by the trusted transport. It supports a batch containing no authorized group messages, or a final batch cursor beyond filtered events, without inventing a `GroupEvent`.

The first checkpoint creates revision 1 from expected revision 0. A different cursor requires an exact revision match. Repeating the current cursor returns the existing checkpoint without increasing its revision, even if the retry carries its original expected revision. Competing different checkpoints cannot silently overwrite each other. Audit records contain the stream reference, revision numbers and a short machine reason code; they omit the opaque cursor and message contents.

The database cannot derive ordering from an opaque cursor. The transport must verify account/stream scope, source epoch and non-regression against the batch it consumed. CAS detects concurrent writes; it is not a substitute for those transport checks. Durable event intake and the final checkpoint must commit before upstream acknowledgement. Event decision processing has its separate durable pending ledger.

`ingest_event` behavior is unchanged: replaying a duplicate source event never advances or regresses the cursor. `advance_cursor` is the explicit transport capability for committing a verified batch checkpoint.
