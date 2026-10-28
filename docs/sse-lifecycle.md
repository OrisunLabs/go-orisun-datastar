# SSE lifecycle

Todo, profile and ops use the same `streamViewStoreFatMorph` sequence: refresh projected state into ViewStore, watch that key with deletes ignored, subscribe to invalidation, and queue a catch-up refresh. The catch-up covers changes between the first refresh and subscription setup. Reconnecting clients repeat this sequence and receive current state rather than depending on an event replay cursor.

Callbacks enqueue into a buffered notification channel. The request handler's select loop performs read-model refreshes and all SSE patches; only ViewStore watcher updates produce fragments. The official Datastar SDK formats and flushes the stream. Template `data-init` helpers keep long-lived data requests open alongside mutations with request cancellation disabled.

The stream owns a child context and cancels it on every exit, including setup, JSON decoding and client-write failures. It closes its projection subscription and KV watcher. Memory watchers exit on explicit stop as well as context cancellation. NATS forwarding can be interrupted while the consumer channel is full and stops the underlying watcher when forwarding exits. This retains the existing streaming sequence while fixing resource cleanup.

Development reload keeps its existing `/reload` SSE and `/hotreload` ping behavior and remains gated by the router's development flag. Its request select exits on disconnect. No reload broadcast behavior or client retry settings are changed in this slice.

`task test` covers actual HTTP todo/profile refresh, disconnect and reconnect, ops disconnect, reload disconnect/ping, post-subscription catch-up, serialized patching, all setup/error exits and failed SDK writes. ViewStore tests cover concurrent memory puts/stops and blocked NATS forwarding; a disposable embedded JetStream server verifies KV initial state, ignored deletes, cancellation and reconnect snapshots. Focused race checks are `go test -race ./internal/httpui ./internal/viewstore`. Tests use disposable local servers and no application data.

Memory watchers keep a bounded queue of view-state snapshots. On overflow, they evict the oldest pending snapshot and retain the newest update without blocking projection writes. Retained entries stay in order. Delete notifications follow the same rule when enabled; `IgnoreDeletes` watchers retain the latest put. Slow watchers do not affect fast watchers. These queues are not durable event logs and may omit intermediate snapshots. Overflow regressions verify the final snapshot/delete remains deliverable after a burst.

Registration processing uses the same ViewStore loop with projection invalidation and a two-second fallback refresh for workflow completion that follows a projection notification. It redirects only when workflow completion and credential readiness agree, and closes subscriptions on terminal outcomes or disconnect. Tests cover delayed projection, notification ordering and reconnect catch-up.

Password-reset processing uses the same single-writer ViewStore loop, with a two-second refresh and reconnect catch-up. It patches success only after both completion evidence and the credential watermark are ready, then closes its ticker/watcher. The token and password never enter ViewStore.

Account-deletion progress remains public after sign-out and uses the same stage-only ViewStore loop with a two-second refresh. It waits for matching effective deletion and completion, then patches the terminal message and closes. Tests cover disconnect/reconnect before completion, completion catch-up, terminal reconnects, cleanup failure and invalid progress identities.
