package httpui

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/starfederation/datastar-go/datastar"

	"github.com/OrisunLabs/go-orisun-datastar/internal/auth"
	"github.com/OrisunLabs/go-orisun-datastar/internal/eventstore"
	"github.com/OrisunLabs/go-orisun-datastar/internal/views"
)

func (s Server) authRoutes(r chi.Router) {
	r.With(noCache).Get("/account-deletion/{requestID}/processing", s.accountDeletionProcessingPage)
	r.With(noCache).Get("/account-deletion/{requestID}/stream", s.accountDeletionProgressStream)
	r.With(noCache).Get("/register", render(views.Register(nil)))
	r.With(noCache, signupRateLimit).Post("/register", s.register)
	r.With(noCache).Get("/login", render(views.Login(nil)))
	r.With(noCache, loginRateLimit).Post("/login", s.login)
	r.Post("/logout", s.logout)
	r.With(noCache).Get("/forgot-password", render(views.ForgotPassword(nil)))
	r.With(noCache, forgotPasswordRateLimit).Post("/forgot-password", s.forgotPassword)
	r.With(noCache).Get("/reset-password/{token}", func(w http.ResponseWriter, r *http.Request) {
		_ = views.ResetPassword(chi.URLParam(r, "token"), nil).Render(r.Context(), w)
	})
	r.With(noCache).Get("/password-reset/{authorizationID}/processing", s.passwordResetProcessingPage)
	r.With(noCache).Get("/password-reset/{authorizationID}/stream", s.passwordResetProgressStream)
	r.With(noCache, resetPasswordRateLimit).Post("/reset-password/{token}", s.resetPassword)
	r.With(noCache).Get("/register/{registrationID}/processing", s.registrationProcessingPage)
	r.With(noCache).Get("/register/{registrationID}/stream", s.registrationProgressStream)
	r.With(noCache).Get("/register/{registrationID}/validate-email", func(w http.ResponseWriter, r *http.Request) {
		_ = views.ValidateEmail(chi.URLParam(r, "registrationID"), nil).Render(r.Context(), w)
	})
	r.With(noCache, otpValidateRateLimit).Post("/register/{registrationID}/validate-email", s.validateEmail)
	r.With(noCache, otpResendRateLimit).Post("/register/{registrationID}/send-email-validation-otp", s.sendOTP)
}

func (s Server) register(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.register", nil)
	if err := r.ParseForm(); err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return flashError(sse, err.Error()) })
		return
	}
	year, _ := strconv.Atoi(r.FormValue("yearOfBirth"))
	registration, err := auth.StartUserRegistrationCommandHandler(r.Context(), auth.StartUserRegistrationCommand{
		Username: r.FormValue("username"), Email: r.FormValue("email"), Password: r.FormValue("password"),
		FirstName: r.FormValue("firstName"), LastName: r.FormValue("lastName"), YearOfBirth: year,
		Metadata: eventstore.HTTPCommandMetadata(r, ""),
	}, s.EventSaver, s.EventRetriever, s.PIIKeys)
	if err != nil {
		patchTempl(w, r, views.RegisterForm(map[string]string{"error": err.Error()}), datastar.WithSelectorID("auth-page"))
		return
	}
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		return sse.Redirect("/register/" + registration.UserRegistrationStartedID + "/validate-email")
	})
}

func (s Server) login(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.login", nil)
	if err := r.ParseForm(); err != nil {
		writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return flashError(sse, err.Error()) })
		return
	}
	user, token, err := s.Sessions.Login(r.Context(), r.FormValue("email"), r.FormValue("password"))
	userRegisteredID := ""
	if err == nil {
		userRegisteredID = user.UserRegisteredID
	}
	_ = auth.RecordLoginAttemptCommandHandler(r.Context(), auth.RecordLoginAttemptCommand{
		AttemptedIdentifier: r.FormValue("email"),
		IPAddress:           r.RemoteAddr,
		UserRegisteredID:    userRegisteredID,
		Succeeded:           err == nil,
		Metadata:            eventstore.HTTPCommandMetadata(r, userRegisteredID),
	}, s.EventSaver)
	if err != nil {
		patchTempl(w, r, views.LoginForm(map[string]string{"error": err.Error()}), datastar.WithSelectorID("auth-page"))
		return
	}
	s.Sessions.SetSessionCookie(w, token)
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return sse.Redirect("/todos") })
}

func (s Server) logout(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.logout", nil)
	if cookie, err := r.Cookie(s.Sessions.SessionCookieName()); err == nil {
		_ = s.Sessions.Logout(r.Context(), cookie.Value)
	}
	s.Sessions.ClearSessionCookie(w)
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (s Server) forgotPassword(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.forgot_password", nil)
	_ = r.ParseForm()
	_, _ = auth.RequestPasswordResetCommandHandler(r.Context(), auth.RequestPasswordResetCommand{
		EmailAddress: r.FormValue("email"),
		Metadata:     eventstore.HTTPCommandMetadata(r, ""),
	}, s.EventSaver, s.EventRetriever, s.PIIKeys)
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error { return sse.Redirect("/login") })
}

func (s Server) resetPassword(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.reset_password", nil)
	_ = r.ParseForm()
	token := chi.URLParam(r, "token")
	authorizationID, err := auth.AuthorizePasswordResetCommandHandler(r.Context(), auth.ResetPasswordCommand{
		Token:    token,
		Password: r.FormValue("password"),
		Metadata: eventstore.HTTPCommandMetadata(r, ""),
	}, s.Verifications, s.EventSaver, s.EventRetriever)
	if err != nil {
		patchTempl(w, r, views.ResetPasswordForm(token, map[string]string{"error": err.Error()}), datastar.WithSelectorID("auth-page"))
		return
	}
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		return sse.Redirect("/password-reset/" + authorizationID + "/processing")
	})
}

func (s Server) validateEmail(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.validate_email", map[string]any{"registrationId": chi.URLParam(r, "registrationID")})
	_ = r.ParseForm()
	registrationID := chi.URLParam(r, "registrationID")
	metadata := eventstore.HTTPCommandMetadata(r, "")
	err := auth.ValidateEmailVerificationOTPCommandHandler(r.Context(), auth.ValidateEmailVerificationOTPCommand{UserRegistrationStartedID: registrationID, Code: r.FormValue("otp"), Metadata: metadata}, s.EventSaver, s.EventRetriever)
	if err != nil {
		patchTempl(w, r, views.ValidateEmailForm(registrationID, map[string]string{"error": err.Error()}), datastar.WithSelectorID("auth-page"))
		return
	}

	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		return sse.Redirect("/register/" + registrationID + "/processing")
	})
}

func (s Server) sendOTP(w http.ResponseWriter, r *http.Request) {
	setRequestAction(r, "auth.send_email_validation_otp", map[string]any{"registrationId": chi.URLParam(r, "registrationID")})
	registrationID := chi.URLParam(r, "registrationID")
	_, err := auth.GenerateEmailVerificationOTPCommandHandler(r.Context(), auth.GenerateEmailVerificationOTPCommand{UserRegistrationStartedID: registrationID, Metadata: eventstore.HTTPCommandMetadata(r, "")}, s.EventSaver, s.EventRetriever)
	if err != nil {
		patchTempl(w, r, views.ValidateEmailForm(registrationID, map[string]string{"error": err.Error()}), datastar.WithSelectorID("auth-page"))
		return
	}
	writeSSE(w, r, func(sse *datastar.ServerSentEventGenerator) error {
		return sse.Redirect("/register/" + registrationID + "/validate-email")
	})
}
