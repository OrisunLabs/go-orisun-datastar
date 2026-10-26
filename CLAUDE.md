# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Read `AGENTS.md` for the full agent guide (reference-project porting rules, CQRS-SSE stream shapes, NATS KV view-state flow, Datastar priority order, visual system). This file is the condensed version.

## What this is

Go-Orisun-Datastar: a Go starter for event-sourced, server-rendered web apps. chi + templ + Datastar SSE + Orisun event sourcing + sqlc/SQLite + Tailwind v4/DaisyUI. The `sqlite` branch (this one) is local-first: SQLite for auth/read models, embedded Orisun SQLite for events, Orisun's embedded NATS/JetStream for notifications — no external services. A `postgres` branch holds the Postgres variant. The sibling `../frases-backend` project is the canonical reference implementation to port patterns from; `../northstar` is the CSS-tooling reference.

## Commands

Use Task, not Make. All tools (air, templ, sqlc, gotailwind) are pinned in `go.mod` and run via `go tool`.

```bash
task migrate      # run migrations (required before first run)
task dev          # templ watcher + air server watcher + CSS watcher; serves http://localhost:3001
task test         # go test ./... (regenerates sqlc + templ first)
task vet          # go vet ./...
task build        # go build ./cmd/server
task templ        # regenerate *_templ.go from .templ files
task sqlc         # regenerate internal/dbsql from queries/ + migrations/
task css          # compile src/input.css -> static/style.css
task seed         # run seed tasks and exit
task reset-read-models  # rebuild read models from events
```

Single test: `go test ./internal/features/todo/ -run TestName` (generate first with `task templ && task sqlc` if `.templ`/`.sql` files changed).

## Architecture

Event-sourced CQRS. All business mutations are immutable Orisun events; SQLite holds only projections (read models), auth/session tables, and event-handler checkpoints. Never mutate business state directly in SQLite; never bypass event saving for domain changes.

Write flow: chi handler parses input and attaches `eventstore.HTTPCommandMetadata` → command-handler slice loads a minimal event context, validates (existence/ownership/idempotency), appends typed events via injected `eventstore.Saver`/`eventstore.Retriever` → event-handler slices project into SQLite read models and checkpoint → NATS notifications invalidate view state → Datastar SSE patches server-rendered templ fragments.

Command/read split (CQRS-style SSE): mutation handlers return small SSE responses (ack, redirect, focused fragment/signal patch) — never large fresh read-model renders. Long-lived read streams subscribe to NATS/read-model notifications, reload the read model, and patch fragments. All SSE writes happen from the handler's own select loop over a buffered channel — never from subscription callback goroutines. `internal/viewstore` holds per-session view state (NATS KV with memory fallback); keep KV payloads small.

Feature slices (`internal/features/todo` is the exemplar): one file per command handler (`create_todo.go`), one per event handler (`todo_read_model_event_handler.go`), read model owns its persistence, `ports.go` holds narrow consumer-driven interfaces for external side effects. Features own their read models — a feature must not query another feature's projection tables; project the needed facts into its own read model instead.

Key wiring: `cmd/server/main.go` (entry point, infra wiring, event-handler startup), `internal/httpui/router.go` (router; register feature routes in per-feature `*_handlers.go` files, don't grow router.go), `internal/eventstore` (Orisun adapter, event types, checkpoints), `internal/eventcatalog/indexes.go` (event-store indexes, ensured at startup).

## Invariants

- Every Orisun query criterion and append subset query must include `eventType` (subscriptions excepted). When changing query/append shapes, update `internal/eventcatalog/indexes.go` and the table in `docs/orisun-event-store-indexes.md`.
- PII in events is never plaintext: use encrypted `protectedpii.Value` fields plus a blind-index hash field for lookups (e.g. `email` + `emailHash`). Per-user subject keys live in `subject_pii_keys`; see `docs/protected-pii-events.md`.
- When changing SQLite read-model queries, add/update indexes in `migrations/` for predicates, joins, ordering, and fan-out paths.
- Generated IDs go through `internal/uuidv7.NewString()`.
- Use the Datastar Go SDK for all SSE (`PatchElementTempl`, `Redirect`, signal patches); never hand-write SSE protocol lines. Prefer server-rendered morphs > redirects > signals > `ExecuteScript` > vanilla JS. Morph targets need stable IDs.
- Reference static assets via `internal/resources.StaticPath` in templ layouts, not hardcoded `/static/...`.

## Generated files (committed, never hand-edit)

- `internal/views/*_templ.go` — from `.templ` files (`task templ`)
- `internal/dbsql/*.go` — from `queries/*.sql` + `migrations/` via sqlc zombiezen plugin (`task sqlc`)
- `static/style.css` — from `src/input.css` (`task css`); edit `src/input.css` only