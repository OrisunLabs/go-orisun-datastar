# Registration workflow

Signup records `UserRegistrationStarted` with frozen registration details and a separately allocated `userId`. Pending attempts do not reserve email or username and do not create login credentials or profile rows.

OTP generation and validation reference the pending registration. Validation authorizes finalization; it does not itself create a user. The HTTP flow finalizes immediately, and a checkpointed validation-event reaction retries interrupted finalization.

Finalization passes the complete frozen input into the independent `RegisterUserCommandHandler`. That command checks accepted identity uniqueness and emits a scope-free `UserRegistered` containing protected profile details, the password hash, registration time and email-verification time. It has no dependency on pending registration or OTP events. Auth and profile projections consume this accepted fact alone.

The workflow then records `UserRegistrationCompleted` with its authorization and accepted-effect references. Retries recover the same accepted identity without duplicate effects. A competing accepted username/email produces `UserRegistrationFailed` with `identity_unavailable`. A pre-existing accepted fact cannot be adopted as a later workflow effect.

This is a fresh-install event/schema change. Old registration history is not upcast, and no database reset or data conversion is performed.
