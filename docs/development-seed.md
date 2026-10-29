# Development seed

Run `task seed` before `task dev`. Seeding is opt-in and exits without starting HTTP; normal startup never seeds. `NODE_ENV=production` rejects seeding before database access. Do not combine `-seed-only` with migration/reset flags. Use a local development database with no concurrent application writers.

Dataset version 1 provides two verified accounts:

| Username | Email | Password |
| --- | --- | --- |
| alex.demo | alex@example.test | StarterDemo123! |
| sam.demo | sam@example.test | StarterDemo123! |

Each account owns a profile bio and four todos: active, completed, renamed then completed/reopened, and completed/deleted. Three are visible; the fourth demonstrates retained deletion history and stays out of the list. Users have separate ownership and inventories. These are public demo credentials, not production accounts.

`internal/developmentseed` composes public commands only. Signup runs registration start, OTP generation/validation, independent registration acceptance and workflow completion. Profile changes use the public bio command. Todo setup uses only the batch Create/Rename/Complete/Reopen/Delete commands, including one-item batches. There are no direct event appends or projection-row inserts. Subject-key creation remains the auth command's infrastructure dependency.

Every seed effect carries system audit metadata plus dataset/version/key/step ownership. User IDs come from recorded registration facts; the dataset reserves fixed canonical UUIDv7 todo creation IDs. Operation IDs are allocated through the normal UUIDv7 helper and recovered from recorded stages. Bounded event queries reconstruct the existing prefix before continuing. Partial failures can resume; a successful rerun appends nothing and returns the same identity/position inventory. No extra seed registry/table is needed.

A matching email/title alone does not grant ownership. The seeder rejects foreign username/email or reserved-ID collisions, changed credentials/names/profiles/todo history, deleted users and missing subject keys. It never resets credentials, restores erased data, renames another record, renews an expired completed workflow, or resets a database. Fix ownership conflicts deliberately rather than deleting/reseeding automatically.

Seed-only startup composes the complete history before starting auth, profile and todo projection subscribers. Other workflow/email/deletion subscribers stay off. This avoids reactions racing composition or executing unrelated pending work. Normal application startup still enables every handler. Readiness checks each owner's existing full commit/prepare watermark, including the deleted todo, plus the verified credential row. Primary-key probes serve these queries; no extra watermark columns or indexes are necessary. The command exits successfully only when the final facts are projected, with a bounded readiness timeout.

Both database branches use the same dataset, commands and readiness contract. Tests use real isolated Orisun SQLite for command histories and race barriers; native projection checks verify readiness and demo login on the owning database. Verification never targets a user's database.
