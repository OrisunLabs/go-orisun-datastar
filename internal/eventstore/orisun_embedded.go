package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	boundarymodel "github.com/OrisunLabs/Orisun/boundary"
	"github.com/OrisunLabs/Orisun/config"
	embeddedpg "github.com/OrisunLabs/Orisun/embedded/postgres"
	coreeventstore "github.com/OrisunLabs/Orisun/eventstore"
	orisunlog "github.com/OrisunLabs/Orisun/logging"
	orisunapi "github.com/OrisunLabs/Orisun/orisun"
	natsgo "github.com/nats-io/nats.go"
)

type EmbeddedOrisun struct {
	store        *embeddedpg.Store
	client       OrisunBoundaryClient
	indexManager orisunapi.BoundaryIndexManager
	boundary     string
}

// OrisunBoundaryClient is the narrow embedded client shared by production and tests.
type OrisunBoundaryClient interface {
	GetWriteContext(context.Context, *orisunapi.GetWriteContextRequest) (*orisunapi.WriteContext, error)
	SaveEventsV2(context.Context, []orisunapi.EventWithMapTags, string, []*orisunapi.ConsistencyObservation) (*orisunapi.Position, error)
	GetEvents(context.Context, *orisunapi.GetEventsRequest) (orisunapi.ReadEventBatch, error)
	GetLatestByCriteria(context.Context, orisunapi.LatestByCriteriaQuery) (orisunapi.LatestByCriteriaBatch, error)
	SubscribeToEvents(context.Context, coreeventstore.SubscribeRequest, coreeventstore.EventHandler) error
}

func NewOrisunBoundaryStore(client OrisunBoundaryClient, boundary string) *EmbeddedOrisun {
	return &EmbeddedOrisun{client: client, boundary: boundary}
}

type EmbeddedConfig struct {
	Boundary         string
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresDatabase string
	PostgresSSLMode  string
	NATSStoreDir     string
	LogLevel         string
}

func StartEmbeddedOrisun(ctx context.Context, cfg EmbeddedConfig) (*EmbeddedOrisun, error) {
	appConfig, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}
	if cfg.Boundary == "" {
		cfg.Boundary = "go_orisun_datastar"
	}
	if cfg.PostgresSSLMode == "" {
		cfg.PostgresSSLMode = "disable"
	}
	if cfg.NATSStoreDir == "" {
		cfg.NATSStoreDir = "/tmp/go-event-starter-orisun-nats"
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}

	appConfig.Backend.Type = "postgres"
	appConfig.Postgres.Host = cfg.PostgresHost
	appConfig.Postgres.Port = cfg.PostgresPort
	appConfig.Postgres.User = cfg.PostgresUser
	appConfig.Postgres.Password = cfg.PostgresPassword
	appConfig.Postgres.Name = cfg.PostgresDatabase
	appConfig.Postgres.SSLMode = cfg.PostgresSSLMode
	appConfig.Postgres.AdminSchema = "admin"
	appConfig.Postgres.ListenEnabled = true
	appConfig.Admin.Boundary = "orisun_admin"
	appConfig.Nats.StoreDir = cfg.NATSStoreDir
	appConfig.Nats.Port = -1
	appConfig.Nats.Cluster.Enabled = false
	appConfig.Logging.Level = cfg.LogLevel

	logger := orisunlog.InitializeDefaultLogger(appConfig.Logging)
	store, err := embeddedpg.Start(ctx, appConfig, logger)
	if err != nil {
		return nil, err
	}
	if err := ensureApplicationBoundary(ctx, store, cfg.Boundary); err != nil {
		store.Close(context.Background())
		return nil, err
	}

	return &EmbeddedOrisun{
		store:        store,
		client:       store,
		indexManager: store,
		boundary:     cfg.Boundary,
	}, nil
}

func ensureApplicationBoundary(ctx context.Context, store *embeddedpg.Store, name string) error {
	_, err := store.CreateBoundary(ctx, boundarymodel.Definition{
		Name:        name,
		Description: "Starter app events",
		Placement:   boundarymodel.Placement{Backend: "postgres", Namespace: "public"},
	})
	if err != nil {
		// Creation can race another instance or find an existing definition.
		// Verify the durable definition below before accepting either case.
		if _, lookupErr := store.GetBoundary(ctx, name); lookupErr != nil {
			return fmt.Errorf("create application boundary: %w", err)
		}
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		boundary, err := store.GetBoundary(waitCtx, name)
		if err != nil {
			return fmt.Errorf("get application boundary: %w", err)
		}
		if boundary.Placement.Backend != "postgres" || boundary.Placement.Namespace != "public" {
			return fmt.Errorf("application boundary %q has unexpected placement: %+v", name, boundary.Placement)
		}
		if boundary.Status == boundarymodel.StatusFailed {
			return fmt.Errorf("application boundary %q provisioning failed: %s", name, boundary.LastError)
		}
		if boundary.Status == boundarymodel.StatusActive && store.RequireBoundaryActive(name) == nil {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("wait for application boundary %q: %w", name, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

func (s *EmbeddedOrisun) Close(ctx context.Context) {
	if s == nil {
		return
	}
	if s.store != nil {
		s.store.Close(ctx)
	}
}

func (s *EmbeddedOrisun) NATSConnection() *natsgo.Conn {
	if s == nil || s.store == nil {
		return nil
	}
	return s.store.NATSConnection()
}

func (s *EmbeddedOrisun) SaveEventsV2(ctx context.Context, events []DomainEvent, consistency []ConsistencyObservation) (WriteResult, error) {
	toSave := make([]orisunapi.EventWithMapTags, 0, len(events))
	for _, event := range events {
		toSave = append(toSave, orisunapi.EventWithMapTags{EventId: event.EventID, EventType: event.EventType, Data: flattenMap(event.Data), Metadata: event.Metadata})
	}
	observations := make([]*orisunapi.ConsistencyObservation, 0, len(consistency))
	for _, observation := range consistency {
		observations = append(observations, &orisunapi.ConsistencyObservation{Query: toOrisunQuery(observation.Query), Position: toOrisunPosition(observation.Position)})
	}
	saved, err := s.client.SaveEventsV2(ctx, toSave, s.boundary, observations)
	if err != nil {
		if strings.Contains(err.Error(), "OptimisticConcurrencyException:StreamVersionConflict") {
			return WriteResult{}, fmt.Errorf("%w: %w", ErrConflict, err)
		}
		return WriteResult{}, err
	}
	return WriteResult{Position: fromOrisunPosition(saved)}, nil
}

func (s *EmbeddedOrisun) GetEvents(ctx context.Context, from Position, count int, direction Direction, query Query) ([]ResolvedEvent, error) {
	if count <= 0 {
		count = 100
	}
	req := &orisunapi.GetEventsRequest{
		Boundary:     s.boundary,
		FromPosition: toOrisunPosition(from),
		Count:        uint32(count),
		Direction:    orisunapi.Direction_ASC,
		Query:        toOrisunQuery(query),
	}
	if direction == Backward {
		req.Direction = orisunapi.Direction_DESC
	}
	resp, err := s.client.GetEvents(ctx, req)
	if err != nil {
		return nil, err
	}
	resolved := make([]ResolvedEvent, 0, len(resp))
	for _, event := range resp {
		mapped, err := fromOrisunReadEvent(event)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, mapped)
	}
	return resolved, nil
}

func (s *EmbeddedOrisun) GetLatestByCriteria(ctx context.Context, criteria []Criterion) (LatestByCriteriaResult, error) {
	batch, err := s.client.GetLatestByCriteria(ctx, orisunapi.LatestByCriteriaQuery{
		Boundary: s.boundary,
		Criteria: toOrisunReadCriteria(criteria),
	})
	if err != nil {
		return LatestByCriteriaResult{}, err
	}
	result := LatestByCriteriaResult{
		Results: make([]LatestCriterionResult, 0, len(batch.Matches)),
		ContextPosition: Position{
			Commit:  batch.ContextCommitPosition,
			Prepare: batch.ContextPreparePosition,
		},
	}
	for i, latest := range batch.Matches {
		mapped := LatestCriterionResult{Criterion: criteria[i]}
		if latest.Found {
			event, err := fromOrisunReadEvent(latest.Event)
			if err != nil {
				return LatestByCriteriaResult{}, err
			}
			mapped.Event = &event
		}
		result.Results = append(result.Results, mapped)
	}
	return result, nil
}

func (s *EmbeddedOrisun) SubscribeToEvents(ctx context.Context, subscriberName string, after Position, query Query, handle func(context.Context, ResolvedEvent) error) error {
	return s.client.SubscribeToEvents(ctx, coreeventstore.SubscribeRequest{
		Boundary:       s.boundary,
		SubscriberName: subscriberName,
		AfterPosition: &coreeventstore.Position{
			CommitPosition:  after.Commit,
			PreparePosition: after.Prepare,
		},
		Query: toOrisunCoreQuery(query),
	}, func(handlerCtx context.Context, event coreeventstore.ReadEvent) error {
		resolved, err := fromOrisunCoreReadEvent(event)
		if err != nil {
			return err
		}
		return handle(handlerCtx, resolved)
	})
}

func fromOrisunCoreReadEvent(event coreeventstore.ReadEvent) (ResolvedEvent, error) {
	data := map[string]any{}
	if event.Data != "" {
		if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
			return ResolvedEvent{}, err
		}
	}
	metadata := map[string]any{}
	if event.Metadata != "" {
		_ = json.Unmarshal([]byte(event.Metadata), &metadata)
	}
	return ResolvedEvent{
		WriteID:  event.WriteID,
		Position: Position{Commit: event.Position.CommitPosition, Prepare: event.Position.PreparePosition},
		Event: DomainEvent{
			EventID:   event.EventID,
			EventType: event.EventType,
			Data:      unflattenMap(data),
			Metadata:  metadata,
		},
	}, nil
}

func toOrisunCoreQuery(query Query) coreeventstore.Query {
	criteria := make([]coreeventstore.Criterion, 0, len(query.Criteria))
	for _, criterion := range query.Criteria {
		tags := make([]coreeventstore.Tag, 0, len(criterion.Tags))
		for _, tag := range criterion.Tags {
			tags = append(tags, coreeventstore.Tag{Key: toOrisunQueryKey(tag.Key), Value: tag.Value})
		}
		criteria = append(criteria, coreeventstore.Criterion{Tags: tags})
	}
	return coreeventstore.Query{Criteria: criteria}
}

func fromOrisunReadEvent(event orisunapi.ReadEvent) (ResolvedEvent, error) {
	data := map[string]any{}
	if event.Data != "" {
		if err := json.Unmarshal([]byte(event.Data), &data); err != nil {
			return ResolvedEvent{}, err
		}
	}
	metadata := map[string]any{}
	if event.Metadata != "" {
		_ = json.Unmarshal([]byte(event.Metadata), &metadata)
	}
	return ResolvedEvent{
		WriteID:  event.WriteId,
		Position: Position{Commit: event.CommitPosition, Prepare: event.PreparePosition},
		Event: DomainEvent{
			EventID:   event.EventId,
			EventType: event.EventType,
			Data:      unflattenMap(data),
			Metadata:  metadata,
		},
	}, nil
}

func toOrisunPosition(position Position) *orisunapi.Position {
	return &orisunapi.Position{CommitPosition: position.Commit, PreparePosition: position.Prepare}
}

func fromOrisunPosition(position *orisunapi.Position) Position {
	if position == nil {
		return NoEventPosition
	}
	return Position{Commit: position.CommitPosition, Prepare: position.PreparePosition}
}

func toOrisunQuery(query Query) *orisunapi.Query {
	if len(query.Criteria) == 0 {
		return nil
	}
	return &orisunapi.Query{Criteria: toOrisunCriteria(query.Criteria)}
}

func toOrisunCriteria(input []Criterion) []*orisunapi.Criterion {
	criteria := make([]*orisunapi.Criterion, 0, len(input))
	for _, criterion := range input {
		tags := make([]*orisunapi.Tag, 0, len(criterion.Tags))
		for _, tag := range criterion.Tags {
			tags = append(tags, &orisunapi.Tag{Key: toOrisunQueryKey(tag.Key), Value: tag.Value})
		}
		criteria = append(criteria, &orisunapi.Criterion{Tags: tags})
	}
	return criteria
}

func toOrisunReadCriteria(input []Criterion) []orisunapi.ReadCriterion {
	criteria := make([]orisunapi.ReadCriterion, 0, len(input))
	for _, criterion := range input {
		tags := make([]orisunapi.ReadTag, 0, len(criterion.Tags))
		for _, tag := range criterion.Tags {
			tags = append(tags, orisunapi.ReadTag{Key: toOrisunQueryKey(tag.Key), Value: tag.Value})
		}
		criteria = append(criteria, orisunapi.ReadCriterion{Tags: tags})
	}
	return criteria
}

func flattenMap(input map[string]any) map[string]any {
	output := map[string]any{}
	var walk func(prefix string, value any)
	walk = func(prefix string, value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				next := key
				if prefix != "" {
					next = prefix + "." + key
				}
				walk(next, child)
			}
		default:
			output[prefix] = typed
		}
	}
	for key, value := range input {
		walk(key, value)
	}
	return output
}

func unflattenMap(input map[string]any) map[string]any {
	output := map[string]any{}
	for key, value := range input {
		parts := strings.Split(key, ".")
		current := output
		for i, part := range parts {
			if i == len(parts)-1 {
				current[part] = value
				continue
			}
			next, ok := current[part].(map[string]any)
			if !ok {
				next = map[string]any{}
				current[part] = next
			}
			current = next
		}
	}
	return output
}

func PollingStoreDir() string {
	return fmt.Sprintf("/tmp/go-event-starter-orisun-%d", time.Now().UnixNano())
}

// Envelope selectors are mapped only at the storage boundary, including indexes.
func toOrisunQueryKey(key string) string {
	switch key {
	case EventTypeField:
		return "__eventType"
	case "eventId":
		return "__eventId"
	default:
		return key
	}
}

// GetWriteContext reads the store-owned observations for a boundary-scoped write.
func (s *EmbeddedOrisun) GetWriteContext(ctx context.Context, writeID string) (WriteContext, error) {
	stored, err := s.client.GetWriteContext(ctx, &orisunapi.GetWriteContextRequest{Boundary: s.boundary, WriteId: writeID})
	if err != nil {
		return WriteContext{}, err
	}
	result := WriteContext{WriteID: stored.WriteId, Consistency: make([]ConsistencyObservation, 0, len(stored.Consistency))}
	for _, observation := range stored.Consistency {
		query := Query{Criteria: make([]Criterion, 0, len(observation.Query.Criteria))}
		for _, criterion := range observation.Query.Criteria {
			tags := make([]Tag, 0, len(criterion.Tags))
			for _, tag := range criterion.Tags {
				key := tag.Key
				switch key {
				case "__eventType":
					key = EventTypeField
				case "__eventId":
					key = "eventId"
				}
				tags = append(tags, Tag{Key: key, Value: tag.Value})
			}
			query.Criteria = append(query.Criteria, Criterion{Tags: tags})
		}
		result.Consistency = append(result.Consistency, ConsistencyObservation{Query: query, Position: fromOrisunPosition(observation.Position)})
	}
	return result, nil
}
