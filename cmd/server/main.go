package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"database/sql"

	"github.com/OrisunLabs/go-orisun-datastar/internal/appdb"
	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/config"
	"github.com/OrisunLabs/go-orisun-datastar/internal/dbsql"
	"github.com/OrisunLabs/go-orisun-datastar/internal/developmentseed"
	"github.com/OrisunLabs/go-orisun-datastar/internal/email"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventcatalog"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/httpui"
	"github.com/OrisunLabs/go-orisun-datastar/internal/natsbus"
	"github.com/OrisunLabs/go-orisun-datastar/internal/protectedpii"
	"github.com/OrisunLabs/go-orisun-datastar/internal/storage"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
)

type runOptions struct {
	migrateOnly     bool
	seedOnly        bool
	resetReadModels bool
}

type appComponents struct {
	sessionManager    *auth.SessionManager
	authUsers         *auth.AuthUserStore
	authProjection    *auth.SQLiteProjectionRepository
	verifications     *auth.VerificationStore
	todoReadModel     *todo.ReadModel
	todoProjection    *todo.SQLiteProjectionRepository
	profileReadModel  *profile.ReadModel
	profileProjection *profile.SQLiteProjectionRepository
	checkpointer      eventstore.Checkpointer
	emailSender       email.Sender
	profileStorage    profile.ObjectStore
	piiKeys           *auth.SubjectPiiKeyStore
	accountDeletion   *auth.AccountDataDeletionStore
}

type eventHandler interface {
	StartSubscribing(context.Context) error
	StopSubscribing()
}

type eventHandlerFactory struct {
	projection bool
	name       string
	create     func() (eventHandler, error)
}

func main() {
	opts := parseOptions()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(ctx, stop, config.Load(), opts, logger); err != nil {
		logger.Error("run app", "err", err)
		os.Exit(1)
	}
}

func parseOptions() runOptions {
	migrateOnly := flag.Bool("migrate-only", false, "run database migrations and exit")
	seedOnly := flag.Bool("seed-only", false, "run seed tasks and exit")
	resetReadModels := flag.Bool("reset-read-models", false, "reset SQLite read models and event-handler checkpoints")
	flag.Parse()
	return runOptions{migrateOnly: *migrateOnly, seedOnly: *seedOnly, resetReadModels: *resetReadModels}
}

func run(ctx context.Context, stop context.CancelFunc, cfg config.Config, opts runOptions, logger *slog.Logger) error {
	if opts.seedOnly && !cfg.DevelopmentCookie {
		return fmt.Errorf("development seeding is disabled in production")
	}
	if opts.seedOnly && (opts.migrateOnly || opts.resetReadModels) {
		return fmt.Errorf("seed-only cannot be combined with migration/reset flags")
	}
	db, err := appdb.Open(ctx, cfg.SQLitePath)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	defer db.Close()

	if err := appdb.Migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate sqlite: %w", err)
	}
	if opts.migrateOnly {
		logger.Info("sqlite migrations complete")
		return nil
	}
	if opts.resetReadModels {
		if err := resetReadModels(ctx, db); err != nil {
			return fmt.Errorf("reset read models: %w", err)
		}
		logger.Info("read models and event-handler checkpoints reset", "database", cfg.SQLitePath)
		return nil
	}

	orisunStore, err := startEventStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer orisunStore.Close(context.Background())

	bus, err := natsbus.FromConn(orisunStore.NATSConnection())
	if err != nil {
		return fmt.Errorf("borrow embedded orisun nats: %w", err)
	}
	defer bus.Close()

	viewStore := newViewStore(bus, logger)
	components := newAppComponents(db, orisunStore, cfg, logger)
	var seeded developmentseed.Result
	if opts.seedOnly {
		seeded, err = developmentseed.Seed(ctx, orisunStore, components.piiKeys)
		if err != nil {
			return fmt.Errorf("seed development data: %w", err)
		}
	}
	factories := eventHandlerFactories(orisunStore, bus, cfg, components, logger)
	if opts.seedOnly {
		factories = projectionFactories(factories)
	}
	handlers, err := startEventHandlers(ctx, factories)
	if err != nil {
		return err
	}
	defer stopEventHandlers(handlers)
	if opts.seedOnly {
		readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := developmentseed.WaitForProjections(readyCtx, seeded, developmentseed.Readers{Auth: components.authUsers, Profiles: components.profileReadModel, Todos: components.todoReadModel}); err != nil {
			return err
		}
		logger.Info("development seed ready", "version", seeded.Version, "users", len(seeded.Users))
		for _, u := range seeded.Users {
			logger.Info("demo login ready", "email", u.Email)
		}
		return nil
	}

	app := httpui.Server{
		Sessions:               components.sessionManager,
		RegistrationReadiness:  components.authUsers,
		PasswordResetReadiness: components.authUsers,
		PIIKeys:                components.piiKeys,
		Verifications:          components.verifications,
		Todos:                  components.todoReadModel,
		Profiles:               components.profileReadModel,
		EventSaver:             orisunStore,
		EventRetriever:         orisunStore,
		ProfileStorage:         components.profileStorage,
		Subscriber:             bus,
		ViewStore:              viewStore,
		Development:            cfg.DevelopmentCookie,
		Logger:                 logger,
	}
	return serveHTTP(ctx, stop, cfg.Port, app.Routes(), logger)
}

func resetReadModels(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	queries := dbsql.New(tx)
	if err := queries.ResetReadModelAuthSessions(ctx); err != nil {
		return err
	}
	if err := queries.ResetReadModelAuthAccounts(ctx); err != nil {
		return err
	}
	if err := queries.ResetReadModelAuthVerifications(ctx); err != nil {
		return err
	}
	if err := queries.ResetReadModelProfiles(ctx); err != nil {
		return err
	}
	if err := queries.ResetReadModelTodos(ctx); err != nil {
		return err
	}
	if err := queries.ResetReadModelAuthUsers(ctx); err != nil {
		return err
	}
	if err := queries.ResetEventHandlerCheckpoints(ctx); err != nil {
		return err
	}
	return tx.Commit()
}

func startEventStore(ctx context.Context, cfg config.Config) (*eventstore.EmbeddedOrisun, error) {
	store, err := eventstore.StartEmbeddedOrisun(ctx, eventstore.EmbeddedConfig{
		Boundary:     cfg.OrisunBoundary,
		SQLiteDir:    cfg.OrisunSQLiteDir,
		NATSStoreDir: eventstore.PollingStoreDir(),
		LogLevel:     "info",
	})
	if err != nil {
		return nil, fmt.Errorf("start embedded orisun: %w", err)
	}
	if err := store.EnsureBoundaryIndexes(ctx, eventcatalog.BoundaryIndexes()); err != nil {
		store.Close(context.Background())
		return nil, fmt.Errorf("ensure orisun indexes: %w", err)
	}
	return store, nil
}

func newViewStore(bus *natsbus.Bus, logger *slog.Logger) viewstore.Store {
	store, err := viewstore.NewNATSStore(bus.Conn(), "go-starter-view-state", 5*time.Minute)
	if err != nil {
		logger.Warn("using in-memory view store fallback", "err", err)
		return viewstore.NewMemoryStore()
	}
	return store
}

func newAppComponents(db *sql.DB, store *eventstore.EmbeddedOrisun, cfg config.Config, logger *slog.Logger) appComponents {
	authUsers := auth.NewAuthUserStore(db)
	sessionManager := auth.NewSessionManager(db, authUsers, !cfg.DevelopmentCookie)
	verifications := auth.NewVerificationStore(db)
	todoReadModel := todo.NewReadModel(db)
	profileReadModel := profile.NewReadModel(db)
	profileStorage := storage.NewLocalProvider(cfg.UploadDir, cfg.UploadBaseURL)
	piiKeys := auth.NewSubjectPiiKeyStore(db, protectedpii.FromEnv())
	accountDeletion := auth.NewAccountDataDeletionStore(db, profileStorage)

	return appComponents{
		sessionManager:    sessionManager,
		authUsers:         authUsers,
		authProjection:    auth.NewSQLiteProjectionRepository(db),
		verifications:     verifications,
		todoReadModel:     todoReadModel,
		todoProjection:    todo.NewSQLiteProjectionRepository(db),
		profileReadModel:  profileReadModel,
		profileProjection: profile.NewSQLiteProjectionRepository(db),
		checkpointer:      eventstore.NewSQLiteCheckpointer(db),
		emailSender:       email.LogSender{Logger: logger},
		profileStorage:    profileStorage,
		piiKeys:           piiKeys,
		accountDeletion:   accountDeletion,
	}
}

func eventHandlerFactories(store *eventstore.EmbeddedOrisun, bus *natsbus.Bus, cfg config.Config, components appComponents, logger *slog.Logger) []eventHandlerFactory {
	return []eventHandlerFactory{
		{
			name: "registration OTP",
			create: func() (eventHandler, error) {
				return auth.NewRegistrationOTPToBeGeneratedEventHandler(store, components.checkpointer, store, store, logger)
			},
		},
		{name: "registration execution", create: func() (eventHandler, error) {
			return auth.NewRegistrationExecutionEventHandler(store, components.checkpointer, store, store, components.piiKeys, logger)
		}},
		{name: "password reset execution", create: func() (eventHandler, error) {
			return auth.NewPasswordResetExecutionEventHandler(store, components.checkpointer, store, store, logger)
		}},
		{
			name: "password reset completion",
			create: func() (eventHandler, error) {
				return auth.NewPasswordResetCompletionEventHandler(store, components.checkpointer, store, store, logger)
			},
		},
		{
			name: "registration completion",
			create: func() (eventHandler, error) {
				return auth.NewRegistrationCompletionEventHandler(store, components.checkpointer, store, store, logger)
			},
		},
		{
			name: "email validation OTP",
			create: func() (eventHandler, error) {
				return auth.NewEmailValidationOTPToBeSentEventHandler(store, components.checkpointer, store, store, components.emailSender, components.piiKeys, logger)
			},
		},
		{
			name: "password reset email",
			create: func() (eventHandler, error) {
				return auth.NewPasswordResetEmailToBeSentEventHandler(store, components.checkpointer, store, store, components.emailSender, cfg.AppURL, components.piiKeys, logger)
			},
		},
		{
			name:       "auth user projection",
			projection: true,
			create: func() (eventHandler, error) {
				return auth.NewAuthUserProjectionEventHandler(store, components.checkpointer, components.authProjection, components.piiKeys, authProfileSource(), authUserInvalidator{publisher: bus}, logger)
			},
		},
		{name: "account deletion completion", create: func() (eventHandler, error) {
			return auth.NewAccountDeletionCompletionEventHandler(store, components.checkpointer, store, store, logger)
		}},

		{
			name: "account deletion",
			create: func() (eventHandler, error) {
				return auth.NewAccountDeletionEventHandler(store, components.checkpointer, store, store, components.accountDeletion, components.piiKeys, logger)
			},
		},
		{
			name:       "profile read model",
			projection: true,
			create: func() (eventHandler, error) {
				return profile.NewReadModelEventHandler(store, components.checkpointer, components.profileProjection, bus, components.piiKeys, logger)
			},
		},
		{
			name:       "todo read model",
			projection: true,
			create: func() (eventHandler, error) {
				return todo.NewTodoReadModelEventHandler(store, components.checkpointer, components.todoProjection, bus, logger)
			},
		},
	}
}

func startEventHandlers(ctx context.Context, factories []eventHandlerFactory) ([]eventHandler, error) {
	handlers := make([]eventHandler, 0, len(factories))
	for _, factory := range factories {
		handler, err := factory.create()
		if err != nil {
			stopEventHandlers(handlers)
			return nil, fmt.Errorf("create %s event handler: %w", factory.name, err)
		}
		if err := handler.StartSubscribing(ctx); err != nil {
			stopEventHandlers(append(handlers, handler))
			return nil, fmt.Errorf("start %s event handler: %w", factory.name, err)
		}
		handlers = append(handlers, handler)
	}
	return handlers, nil
}

func stopEventHandlers(handlers []eventHandler) {
	for i := len(handlers) - 1; i >= 0; i-- {
		handlers[i].StopSubscribing()
	}
}

func serveHTTP(ctx context.Context, stop context.CancelFunc, port string, handler http.Handler, logger *slog.Logger) error {
	server := &http.Server{Addr: ":" + port, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		logger.Info("starting server", "addr", "http://localhost:"+port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}
	return nil
}

// Seed-only runs projections after composing complete public-command histories.
// It does not execute unrelated pending workflows or start an HTTP listener.
func projectionFactories(factories []eventHandlerFactory) []eventHandlerFactory {
	var selected []eventHandlerFactory
	for _, factory := range factories {
		if factory.projection {
			selected = append(selected, factory)
		}
	}
	return selected
}
