# Go-Orisun-Datastar

Go-Orisun-Datastar is a Go starter for event-sourced, server-rendered web applications. It combines [Orisun](https://github.com/OrisunLabs/Orisun), chi, templ, Datastar, sqlc, Tailwind, and DaisyUI into a small application that is useful as a reference and as a starting point.

The `sqlite` branch uses SQLite for auth/session tables and read models, embedded Orisun stores immutable domain events in SQLite, and the app borrows Orisun's embedded NATS/JetStream connection for event notifications and view-state updates. Email messages are logged, uploads are written to `static/uploads`, and no external email, object storage, or NATS service is required.

## Branches

This repository keeps database-backed variants on separate branches:

- `postgres`: PostgreSQL for app tables plus embedded Orisun Postgres. This is the Postgres-backed variant.
- `sqlite`: SQLite for app tables plus embedded Orisun SQLite. Use this when you want the easiest local development path.

If a branch is already checked out in a separate worktree, use that worktree path or remove it before switching branches in this checkout.

## Features

- Go HTTP server with `github.com/go-chi/chi/v5`
- Server-rendered `.templ` views
- Datastar SSE for form submissions, redirects, indicators, fragment patches, sparse signals, and long-running streams
- Datastar SDK SSE compression with `datastar.WithCompression()`
- HTTP response compression for normal HTML, CSS, JavaScript, JSON, text, and SVG responses
- [Orisun](https://github.com/OrisunLabs/Orisun) event sourcing with typed domain events
- Embedded Orisun NATS/JetStream reused directly by the app; no NATS URL is needed
- CQRS-style read models and replayable event-handler slices
- Command-handler slices for business writes
- sqlc-generated `database/sql` access using the pure Go SQLite driver
- UUIDv7 identifiers for generated IDs
- Authentication, sessions, email verification, password reset, profile editing, and todos
- Local logged email sender for development and demos
- Local filesystem upload storage served from `/static/uploads`
- Tailwind v4 and DaisyUI compiled by `go tool gotailwind`
- Task-based development workflow

## Requirements

- Go matching the version in `go.mod`
- Task

All project tools are pinned in `go.mod` and invoked with `go tool` through `Taskfile.yml`.

## Quick Start

```bash
task migrate
task dev
```

Open `http://localhost:3001` unless you override `PORT`.

For a production-style build:

```bash
task build
./server
```

## Commands

```bash
task dev          # run templ, CSS, and server watchers
task server-watch # run the Go server through air
task css          # compile src/input.css to static/style.css
task css-watch    # watch and compile CSS
task templ        # generate *_templ.go files
task sqlc         # generate database code
task migrate      # run database migrations and exit
task seed         # run seed tasks and exit
task test         # run Go tests
task vet          # run go vet
task build        # build ./cmd/server
```

## Configuration

The app has useful defaults. Override these environment variables when needed:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3001` | HTTP server port |
| `APP_URL` | `http://localhost:$PORT` | Base URL used in generated links |
| `SQLITE_PATH` | `data/appdb.sqlite` | Application SQLite database |
| `ORISUN_SQLITE_DIR` | `data/app-orisun` | Embedded Orisun SQLite directory |
| `ORISUN_GENERAL_BOUNDARY` | `go_orisun_datastar` | Orisun boundary name |
| `UPLOAD_DIR` | `static/uploads` | Local upload destination |
| `UPLOAD_BASE_URL` | `/static/uploads` | Public URL prefix for uploaded files |
| `BETTER_AUTH_SECRET` | development secret | Session/auth secret |
| `NODE_ENV` | `development` | Controls development cookie behavior and hot reload |

The SQLite branch needs no external database. Application and event files are separate. Use a fresh database for this starter schema; existing event payloads are not rewritten or upgraded automatically.

## Architecture

All business changes are written as Orisun events. HTTP handlers parse input and call command handlers. Command handlers validate intent, load the minimal event context they need, and append typed events. Event handlers project those events into read models and checkpoint progress.

The UI flow follows [the Tao of Datastar](https://data-star.dev/guide/the_tao_of_datastar): keep state on the backend, render templ fragments on the server, patch elements or sparse signals over SSE, and use redirects or normal anchors for navigation.

Typical write flow:

1. A chi handler parses input and builds command metadata.
2. A command-handler slice validates the command and appends typed Orisun events.
3. Event-handler slices consume events, update read models, and checkpoint progress.
4. Embedded NATS notifications invalidate or refresh server-side view state.
5. Datastar streams compressed SSE patches for templ fragments and sparse signals.

Keep direct SQL writes limited to read models, auth/session tables, and event-handler checkpoints. Domain state should flow through events.

## Slice Layout

Feature folders are organized around behavior:

- command handlers live in their own files, such as `create_todos_command_handler.go`, `rename_todos_command_handler.go`, and `upload_profile_image_command_handler.go`
- each command slice owns its load context instead of depending on a shared aggregate loader
- event handlers live in their own files, such as `todo_read_model_event_handler.go`
- read models own query/projection persistence
- `ports.go` files hold consumer-driven interfaces only when a slice needs an external capability

Generated identifiers should go through `internal/uuidv7.NewString()`.

## Styling

Edit `src/input.css`; do not edit `static/style.css` directly. DaisyUI is loaded through local Tailwind plugin bundles under `src/daisyui`, and `task css` compiles the generated stylesheet.

## Generated Files

The repository includes generated files so the app is easy to inspect and build:

- `internal/views/*_templ.go` from templ
- `internal/dbsql/*.go` from sqlc
- `static/style.css` from Tailwind/DaisyUI

Regenerate them with:

```bash
task sqlc
task templ
task css
```

## Backend patterns

Commands use narrow ports, bounded latest-event context reads and one `SaveCommandEventsV2` append preserving every independently checked query and position. Orisun owns persisted write contexts; domain ancestry is separate from consistency metadata. Stateful command tests use real in-memory Orisun SQLite through `internal/eventstoretest` through the same V2 Orisun adapter.

See [backend architecture patterns and remaining conversions](docs/backend-patterns.md). Existing NATS KV view-state remains supported.

Todo projection persistence uses a transaction-bound repository and an ordered state fold. See [todo projection transactions](docs/todo-projection-transactions.md), [auth projection transactions](docs/auth-projection-transactions.md), and [profile projection transactions](docs/profile-projection-transactions.md). Native projection checks run automatically with `task test` in isolated temporary SQLite files. No external database or `TEST_DATABASE_URL` is required.

SQLite persistence and verification are described in [SQLite branch parity](docs/sqlite-parity.md).
