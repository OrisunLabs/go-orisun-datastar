package httpui

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/starfederation/datastar-go/datastar"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/features/profile"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
	"github.com/OrisunLabs/go-orisun-datastar/internal/viewstore"
)

type profileViewState struct {
	User views.User `json:"user"`
}

func (s Server) profileRoutes(r chi.Router) {
	r.With(noCache).Get("/profile", s.profilePage)
	r.Get("/profile/stream", s.profileStream)
	r.With(noCache).Get("/profile/edit", s.profileEdit)
	r.With(noCache).Get("/profile/settings", s.settings)
	r.With(noCache).Post("/profile/bio", s.updateBio)
	r.With(noCache).Post("/profile/avatar", s.uploadAvatar)
	r.With(noCache).Post("/profile/header-image", s.uploadHeader)
	r.With(noCache).Post("/user/name", s.updateName)
	r.With(noCache).Post("/settings/change-password", s.changePassword)
	r.With(noCache).Post("/settings/delete-account", s.deleteAccount)
}

func (s Server) profilePage(w http.ResponseWriter, r *http.Request) {
	user, err := s.profileUser(r.Context(), currentUser(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = views.Profile(user).Render(r.Context(), w)
}

func (s Server) profileStream(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "profile.stream.connect", nil)
	user := currentUser(r)
	sse := newSSE(w, r)
	ctx := r.Context()
	key := viewstore.ProfileKey(s.sessionID(r), user.UserRegisteredID)

	err := streamViewStoreFatMorph[profileViewState](ctx, sse, viewStoreFatMorphConfig[profileViewState]{
		Key:     key,
		Store:   s.ViewStore,
		Refresh: func(ctx context.Context) error { return s.refreshProfileViewState(ctx, key, user) },
		Subscribe: func(ctx context.Context, notify func()) (closeableSubscription, error) {
			return s.Subscriber.Subscribe(ctx, profile.Channel(user.UserRegisteredID), func(context.Context, []byte) {
				notify()
			})
		},
		Patch: func(sse *datastar.ServerSentEventGenerator, state profileViewState) error {
			return sse.PatchElementTempl(views.ProfilePanel(state.User), datastar.WithSelector("#profile-panel"))
		},
	})
	if err != nil {
		_ = alert(sse, err.Error())
	}
}

func (s Server) profileEdit(w http.ResponseWriter, r *http.Request) {
	user, err := s.profileUser(r.Context(), currentUser(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = views.ProfileEdit(user, nil).Render(r.Context(), w)
}

func (s Server) settings(w http.ResponseWriter, r *http.Request) {
	_ = views.Settings(currentUser(r)).Render(r.Context(), w)
}

func (s Server) updateBio(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "profile.update_bio", nil)
	_ = r.ParseForm()
	user := currentUser(r)
	err := profile.UpdateProfileBioCommandHandler(r.Context(), profile.UpdateProfileBioCommand{
		UserID:   user.UserRegisteredID,
		Bio:      r.FormValue("bio"),
		Metadata: eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever, s.PIIKeys)
	if err != nil {
		patchTempl(w, r, views.ProfileEditPanel(user, map[string]string{"bio": err.Error()}), datastar.WithSelectorID("profile-edit-page"))
		return
	}
	actionSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return sse.Redirect("/profile") })
}

func (s Server) uploadAvatar(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "profile.upload_avatar", nil)
	s.uploadImage(w, r, false)
}

func (s Server) uploadHeader(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "profile.upload_header", nil)
	s.uploadImage(w, r, true)
}

func (s Server) uploadImage(w http.ResponseWriter, r *http.Request, header bool) {
	user := currentUser(r)
	data, contentType, err := readUpload(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := profile.UploadProfileImageCommandHandler(r.Context(), profile.UploadProfileImageCommand{
		UserID:      user.UserRegisteredID,
		Data:        data,
		ContentType: contentType,
		Header:      header,
		Metadata:    eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever, s.ProfileStorage); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/profile", http.StatusFound)
}

func (s Server) updateName(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "user.update_name", nil)
	_ = r.ParseForm()
	user := currentUser(r)
	_, err := auth.UpdateUserNameCommandHandler(r.Context(), auth.UpdateUserNameCommand{
		User:     user,
		Name:     r.FormValue("name"),
		Metadata: eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.EventSaver, s.EventRetriever, s.PIIKeys)
	actionSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		if err != nil {
			return alert(sse, err.Error())
		}
		return sse.Redirect("/profile/settings")
	})
}

func (s Server) changePassword(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "settings.change_password", nil)
	_ = r.ParseForm()
	user := currentUser(r)
	err := auth.ChangePasswordCommandHandler(r.Context(), auth.ChangePasswordCommand{
		User:            user,
		CurrentPassword: r.FormValue("currentPassword"),
		NewPassword:     r.FormValue("newPassword"),
		Metadata:        eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.PasswordCredentials, s.EventSaver, s.EventRetriever)
	actionSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		if err != nil {
			return alert(sse, err.Error())
		}
		return sse.Redirect("/profile/settings")
	})
}

func (s Server) deleteAccount(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "settings.delete_account", nil)
	_ = r.ParseForm()
	user := currentUser(r)
	_, err := auth.RequestAccountDeletionCommandHandler(r.Context(), auth.RequestAccountDeletionCommand{
		User:     user,
		Password: r.FormValue("password"),
		Metadata: eventstore.HTTPCommandMetadata(r, user.UserRegisteredID),
	}, s.PasswordCredentials, s.EventSaver, s.EventRetriever)
	actionSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		if err != nil {
			return alert(sse, err.Error())
		}
		s.Sessions.ClearSessionCookie(w)
		return sse.Redirect("/login")
	})
}

func (s Server) refreshProfileViewState(ctx context.Context, key string, current views.User) error {
	user, err := s.profileUser(ctx, current)
	if err != nil {
		return err
	}
	return viewstore.PutState(ctx, s.ViewStore, key, profileViewState{User: user})
}

func (s Server) profileUser(ctx context.Context, current views.User) (views.User, error) {
	user, err := s.Profiles.User(ctx, current.UserRegisteredID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return current, nil
		}
		return views.User{}, err
	}
	user.EmailVerified = current.EmailVerified
	return user, nil
}

func readUpload(r *http.Request) ([]byte, string, error) {
	if err := r.ParseMultipartForm(6 * 1024 * 1024); err != nil {
		return nil, "", err
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		return nil, "", err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 6*1024*1024))
	if err != nil {
		return nil, "", err
	}
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}
	return data, contentType, nil
}
