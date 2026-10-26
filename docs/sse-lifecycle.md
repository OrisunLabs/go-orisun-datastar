# SSE lifecycle

Todo, profile and ops use the same `streamViewStoreFatMorph` sequence: refresh projected state into ViewStore, watch that key with deletes ignored, subscribe to invalidation, and queue a catch-up refresh. The catch-up covers changes between the first refresh and subscription setup. Reconnecting clients repeat this sequence and receive current state rather than depending on an event replay cursor.

Callbacks enqueue into a buffered notification channel. The request handler's select loop performs read-model refreshes and all SSE patches; only ViewStore watcher updates produce fragments. The official Datastar SDK formats and flushes the stream. Template `data-init` helpers keep long-lived data requests open alongside mutations with request cancellation disabled.

The stream owns a child context and cancels it on every exit, including setup, JSON decoding and client-write failures. It closes its projection subscription and KV watcher. Memory watchers exit on explicit stop as well as context cancellation. NATS forwarding can be interrupted while the consumer channel is full and stops the underlying watcher when forwarding exits. This retains the existing streaming sequence while fixing resource cleanup.

Development reload keeps its existing `/reload` SSE and `/hotreload` ping behavior and remains gated by the router's development flag. Its request select exits on disconnect. No reload broadcast behavior or client retry settings are changed in this slice.

`task test` covers actual HTTP todo/profile refresh, disconnect and reconnect, ops disconnect, reload disconnect/ping, post-subscription catch-up, serialized patching, all setup/error exits and failed SDK writes. ViewStore tests cover concurrent memory puts/stops and blocked NATS forwarding; a disposable embedded JetStream server verifies KV initial state, ignored deletes, cancellation and reconnect snapshots. Focused race checks are `go test -race ./internal/httpui ./internal/viewstore`. Tests use disposable local servers and no application data.
