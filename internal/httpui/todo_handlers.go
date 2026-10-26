package httpui

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"

	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/todo"
	"github.com/OrisunLabs/go-orisun-datastar/internal/uuidv7"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
)

type todoListViewState struct {
	Todos []views.Todo `json:"todos"`
}

func (s Server) todoRoutes(r chi.Router) {
	r.Get("/todos", s.todosPage)
	r.Get("/todos/stream", s.todosStream)
	r.Post("/todos", s.createTodo)
	r.Post("/todos/{todoID}/rename", s.renameTodo)
	r.Post("/todos/{todoID}/complete", s.completeTodo)
	r.Post("/todos/{todoID}/reopen", s.reopenTodo)
	r.Post("/todos/{todoID}/delete", s.deleteTodo)
	r.Post("/todos/bulk/complete-active", s.completeActiveTodos)
	r.Post("/todos/bulk/reopen-completed", s.reopenCompletedTodos)
	r.Post("/todos/bulk/clear-completed", s.deleteTodos)
}

func (s Server) todosPage(w http.ResponseWriter, r *http.Request) {
	user := currentUser(r)
	todos, err := s.Todos.List(r.Context(), user.UserRegisteredID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = views.TodosPage(user, todos).Render(r.Context(), w)
}

func (s Server) todosStream(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.stream.connect", nil)
	user := currentUser(r)
	sse := newSSE(w, r)
	ctx := r.Context()
	key := viewstore.TodoListKey(s.sessionID(r), user.UserRegisteredID)

	err := streamViewStoreFatMorph[todoListViewState](ctx, sse, viewStoreFatMorphConfig[todoListViewState]{
		Key:     key,
		Store:   s.ViewStore,
		Refresh: func(ctx context.Context) error { return s.refreshTodoViewState(ctx, key, user.UserRegisteredID) },
		Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
			return s.Subscriber.Subscribe(ctx, todo.Channel(user.UserRegisteredID), func(context.Context, []byte) {
				notify()
			})
		},
		Patch: func(sse *datastar.ServerSentEventGenerator, state todoListViewState) error {
			return sse.PatchElementTempl(views.TodoWorkspace(state.Todos), datastar.WithSelector("#todo-workspace"))
		},
	})
	if err != nil {
		_ = alert(sse, err.Error())
	}
}

func (s Server) createTodo(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.create", nil)
	_ = r.ParseForm()
	user := currentUser(r)
	err := todo.CreateTodosCommandHandler(r.Context(), todo.CreateTodosCommand{
		UserID:   user.UserRegisteredID,
		Titles:   map[string]string{uuidv7.NewString(): r.FormValue("title")},
		Metadata: eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	if err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return flashError(sse, err.Error()) })
		return
	}
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return clearNewTodoTitle(sse) })
}

func (s Server) renameTodo(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.rename", map[string]any{"todoId": chi.URLParam(r, "todoID")})
	_ = r.ParseForm()
	user := currentUser(r)
	err := todo.RenameTodosCommandHandler(r.Context(), todo.RenameTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: map[string]todo.TodoRename{chi.URLParam(r, "todoID"): {EventID: uuidv7.NewString(), Title: r.FormValue("title"), ExpectedTitle: r.FormValue("expectedTitle")}},
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) completeTodo(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.complete", map[string]any{"todoId": chi.URLParam(r, "todoID")})
	user := currentUser(r)
	err := todo.CompleteTodosCommandHandler(r.Context(), todo.CompleteTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: map[string]string{chi.URLParam(r, "todoID"): uuidv7.NewString()},
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) reopenTodo(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.reopen", map[string]any{"todoId": chi.URLParam(r, "todoID")})
	user := currentUser(r)
	err := todo.ReopenTodosCommandHandler(r.Context(), todo.ReopenTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: map[string]string{chi.URLParam(r, "todoID"): uuidv7.NewString()},
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) deleteTodo(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.delete", map[string]any{"todoId": chi.URLParam(r, "todoID")})
	user := currentUser(r)
	err := todo.DeleteTodosCommandHandler(r.Context(), todo.DeleteTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: map[string]string{chi.URLParam(r, "todoID"): uuidv7.NewString()},
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) completeActiveTodos(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.bulk.complete_active", nil)
	user := currentUser(r)
	items, err := s.Todos.List(r.Context(), user.UserRegisteredID)
	if err != nil {
		emptySSE(w, r, err)
		return
	}
	operations := make(map[string]string)
	for _, item := range items {
		if !item.Completed {
			operations[item.TodoID] = uuidv7.NewString()
		}
	}
	err = todo.CompleteTodosCommandHandler(r.Context(), todo.CompleteTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: operations,
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) reopenCompletedTodos(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.bulk.reopen_completed", nil)
	user := currentUser(r)
	items, err := s.Todos.List(r.Context(), user.UserRegisteredID)
	if err != nil {
		emptySSE(w, r, err)
		return
	}
	operations := make(map[string]string)
	for _, item := range items {
		if item.Completed {
			operations[item.TodoID] = uuidv7.NewString()
		}
	}
	err = todo.ReopenTodosCommandHandler(r.Context(), todo.ReopenTodosCommand{
		UserID:     user.UserRegisteredID,
		Operations: operations,
		Metadata:   eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) deleteTodos(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "todo.bulk.clear_completed", nil)
	user := currentUser(r)
	items, err := s.Todos.List(r.Context(), user.UserRegisteredID)
	if err != nil {
		emptySSE(w, r, err)
		return
	}
	operations := make(map[string]string)
	for _, item := range items {
		if item.Completed {
			operations[item.TodoID] = uuidv7.NewString()
		}
	}
	err = todo.DeleteTodosCommandHandler(r.Context(), todo.DeleteTodosCommand{
		UserID:           user.UserRegisteredID,
		Operations:       operations,
		RequireCompleted: true,
		Metadata:         eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever)
	emptySSE(w, r, err)
}

func (s Server) refreshTodoViewState(ctx context.Context, key string, userRegisteredID string) error {
	todos, err := s.Todos.List(ctx, userRegisteredID)
	if err != nil {
		return err
	}
	return viewstore.PutState(ctx, s.ViewStore, key, todoListViewState{Todos: todos})
}
