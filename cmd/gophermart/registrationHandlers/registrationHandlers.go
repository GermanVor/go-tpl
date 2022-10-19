package registrationhandlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/GermanVor/go-tpl/internal/common"
	userStor "github.com/GermanVor/go-tpl/internal/userStor"
	"github.com/go-chi/chi"
)

const (
	SessionTokenName = "sessionToken"
)

func CheckUserTokenMiddleware(next http.Handler, stor userStor.Interface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(SessionTokenName)
		if err != nil {
			if errors.Is(err, http.ErrNoCookie) {
				w.WriteHeader(http.StatusUnauthorized)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}

			return
		}

		userID, err := stor.GetUserID(cookie.Value)
		if err == nil {
			newContext := context.WithValue(r.Context(), common.UserIDContextKey, userID)
			r = r.WithContext(newContext)

			next.ServeHTTP(w, r)
		} else {
			if errors.Is(err, userStor.ErrUnknownSessionToken) {
				w.WriteHeader(http.StatusUnauthorized)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	})
}

type UserRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func setUserCookie(w http.ResponseWriter, userID string) http.ResponseWriter {
	cookie := &http.Cookie{
		Name:  SessionTokenName,
		Value: userID,
	}

	http.SetCookie(w, cookie)

	return w
}

func RegisterHandler(w http.ResponseWriter, r *http.Request, stor userStor.Interface) {
	if r.Header.Get("Content-Type") != common.ApplicationJSONStr {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 100))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	userObj := UserRequest{}
	err = json.Unmarshal(bodyBytes, &userObj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	sessionToken, err := stor.SignIn(userObj.Login, userObj.Password)
	if err != nil {
		if errors.Is(err, userStor.ErrLoginOccupied) {
			http.Error(w, err.Error(), http.StatusConflict)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	setUserCookie(w, sessionToken)
	w.WriteHeader(http.StatusOK)
}

func LoginHandler(w http.ResponseWriter, r *http.Request, stor userStor.Interface) {
	if r.Header.Get("Content-Type") != common.ApplicationJSONStr {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 100))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	userObj := UserRequest{}
	err = json.Unmarshal(bodyBytes, &userObj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	userID, err := stor.LogIn(userObj.Login, userObj.Password)
	if err != nil {
		if errors.Is(err, userStor.ErrUnknownUser) {
			http.Error(w, err.Error(), http.StatusUnauthorized)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	setUserCookie(w, userID)
	w.WriteHeader(http.StatusOK)
}

func InitRouter(r chi.Router, stor userStor.Interface) {
	r.Post("/api/user/register", func(w http.ResponseWriter, r *http.Request) {
		RegisterHandler(w, r, stor)
	})

	r.Post("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		LoginHandler(w, r, stor)
	})
}
