package todo

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type ReopenAllCompletedTodosCommand struct {
	UserID   string
	Metadata CommandMetadata
}

func ReopenAllCompletedTodosCommandHandler(ctx context.Context, command ReopenAllCompletedTodosCommand, readModel TodoReadModelReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadReopenAllCompletedTodosContext(ctx, readModel, command.UserID)
	if err != nil {
		return err
	}
	for _, item := range model.todos {
		if !item.Completed {
			continue
		}
		if _, err := ReopenTodoCommandHandler(ctx, ReopenTodoCommand{
			UserID:   command.UserID,
			TodoID:   item.TodoID,
			Metadata: command.Metadata,
		}, saver, retriever); err != nil {
			return err
		}
	}
	return nil
}

type reopenAllCompletedTodosContext struct {
	todos []views.Todo
}

func loadReopenAllCompletedTodosContext(ctx context.Context, readModel TodoReadModelReader, userID string) (*reopenAllCompletedTodosContext, error) {
	todos, err := readModel.List(ctx, userID)
	if err != nil {
		return nil, err
	}
	return &reopenAllCompletedTodosContext{todos: todos}, nil
}
