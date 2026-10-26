package todo

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type CompleteAllActiveTodosCommand struct {
	UserID   string
	Metadata CommandMetadata
}

func CompleteAllActiveTodosCommandHandler(ctx context.Context, command CompleteAllActiveTodosCommand, readModel TodoReadModelReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadCompleteAllActiveTodosContext(ctx, readModel, command.UserID)
	if err != nil {
		return err
	}
	for _, item := range model.todos {
		if item.Completed {
			continue
		}
		if _, err := CompleteTodoCommandHandler(ctx, CompleteTodoCommand{
			UserID:   command.UserID,
			TodoID:   item.TodoID,
			Metadata: command.Metadata,
		}, saver, retriever); err != nil {
			return err
		}
	}
	return nil
}

type completeAllActiveTodosContext struct {
	todos []views.Todo
}

func loadCompleteAllActiveTodosContext(ctx context.Context, readModel TodoReadModelReader, userID string) (*completeAllActiveTodosContext, error) {
	todos, err := readModel.List(ctx, userID)
	if err != nil {
		return nil, err
	}
	return &completeAllActiveTodosContext{todos: todos}, nil
}
