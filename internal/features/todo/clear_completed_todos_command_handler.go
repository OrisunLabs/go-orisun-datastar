package todo

import (
	"context"

	"github.com/OrisunLabs/go-orisun-datastar/internal/commandlimits"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

type ClearCompletedTodosCommand struct {
	UserID   string
	Metadata CommandMetadata
}

func ClearCompletedTodosCommandHandler(ctx context.Context, command ClearCompletedTodosCommand, readModel TodoReadModelReader, saver eventstore.SaverV2, retriever eventstore.LatestRetriever) error {
	if err := commandlimits.Assert(command); err != nil {
		return err
	}
	model, err := loadClearCompletedTodosContext(ctx, readModel, command.UserID)
	if err != nil {
		return err
	}
	for _, item := range model.todos {
		if !item.Completed {
			continue
		}
		if _, err := DeleteTodoCommandHandler(ctx, DeleteTodoCommand{
			UserID:   command.UserID,
			TodoID:   item.TodoID,
			Metadata: command.Metadata,
		}, saver, retriever); err != nil {
			return err
		}
	}
	return nil
}

type clearCompletedTodosContext struct {
	todos []views.Todo
}

func loadClearCompletedTodosContext(ctx context.Context, readModel TodoReadModelReader, userID string) (*clearCompletedTodosContext, error) {
	todos, err := readModel.List(ctx, userID)
	if err != nil {
		return nil, err
	}
	return &clearCompletedTodosContext{todos: todos}, nil
}
