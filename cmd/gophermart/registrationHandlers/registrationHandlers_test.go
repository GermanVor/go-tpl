package registrationhandlers_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	registrationHandlers "github.com/GermanVor/go-tpl/cmd/gophermart/registrationHandlers"
	gophermartStor "github.com/GermanVor/go-tpl/internal/gophermartStor"
	userStor "github.com/GermanVor/go-tpl/internal/userStor"
	"github.com/bmizerany/assert"
	"github.com/go-chi/chi"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

const databaseURI = "postgres://zzman:@localhost:5432/test"

var userObj = registrationHandlers.UserRequest{
	Login:    "Qwerty",
	Password: "qwertY",
}

func cleanDatabase() {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	dropTableNameList := []string{
		"users",
		"balances",
	}

	for _, tableName := range dropTableNameList {
		sql := "DROP TABLE IF EXISTS " + tableName

		_, err = conn.Exec(context.TODO(), sql)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
}

func init() {
	cleanDatabase()
}

type StorMock struct {
	gophermartStor.Interface
}

// Need for correct work of SignIn registration handler
func createBalancesTable() {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	tx, err := conn.Begin(context.TODO())
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer tx.Rollback(context.TODO())

	gophermartStor.CreateBalancesTable(tx)

	tx.Commit(context.TODO())
}

func createTestEnv() (string, func()) {
	createBalancesTable()

	r := chi.NewRouter()

	userStorage := userStor.Init(databaseURI)
	registrationHandlers.InitRouter(r, userStorage)

	ts := httptest.NewServer(r)

	destructor := func() {
		ts.Close()
		cleanDatabase()
	}

	return ts.URL, destructor
}

func getSessionCookie(cookies []*http.Cookie) *http.Cookie {
	for _, cookie := range cookies {
		if cookie.Name == registrationHandlers.SessionTokenName {
			return cookie
		}
	}

	return nil
}

func registerUser(t *testing.T, endpointURL string, userData []byte) *http.Response {
	req, err := http.NewRequest(
		http.MethodPost,
		endpointURL+"/api/user/register",
		bytes.NewReader(userData),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestRegisterHandler(t *testing.T) {
	endpointURL, destructor := createTestEnv()
	defer destructor()

	userData, err := json.Marshal(userObj)
	require.NoError(t, err)

	t.Run("Success registretion", func(t *testing.T) {
		resp := registerUser(t, endpointURL, userData)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotEqual(t, getSessionCookie(resp.Cookies()), (*http.Cookie)(nil))
	})

	t.Run("Try create account with the same login", func(t *testing.T) {
		resp := registerUser(t, endpointURL, userData)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusConflict, resp.StatusCode)
		assert.Equal(t, getSessionCookie(resp.Cookies()), (*http.Cookie)(nil))
	})
}

func TestLoginHandler(t *testing.T) {
	endpointURL, destructor := createTestEnv()
	defer destructor()

	userData, err := json.Marshal(userObj)
	require.NoError(t, err)

	resp := registerUser(t, endpointURL, userData)
	defer resp.Body.Close()

	t.Run("Success Login", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodPost,
			endpointURL+"/api/user/login",
			bytes.NewReader(userData),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotEqual(t, getSessionCookie(resp.Cookies()), (*http.Cookie)(nil))
	})

	t.Run("Negative Login", func(t *testing.T) {
		unknownUserObj := userObj
		unknownUserObj.Login += "@"

		userData, err := json.Marshal(unknownUserObj)
		require.NoError(t, err)

		req, err := http.NewRequest(
			http.MethodPost,
			endpointURL+"/api/user/login",
			bytes.NewReader(userData),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
		assert.Equal(t, getSessionCookie(resp.Cookies()), (*http.Cookie)(nil))
	})
}

func TestCheckUserTokenMiddleware(t *testing.T) {
	createBalancesTable()

	key := "qwerty"

	r := chi.NewRouter()

	userStorage := userStor.Init(databaseURI)

	r.Group(func(r chi.Router) {
		registrationHandlers.InitRouter(r, userStorage)
	})

	r.Group(func(r chi.Router) {
		r.Use(func(h http.Handler) http.Handler {
			return registrationHandlers.CheckUserTokenMiddleware(h, userStorage)
		})

		r.Get("/test", func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(key))
			w.WriteHeader(http.StatusOK)
		})
	})

	ts := httptest.NewServer(r)
	endpointURL := ts.URL
	defer func() {
		ts.Close()
		cleanDatabase()
	}()

	userData, err := json.Marshal(userObj)
	require.NoError(t, err)

	resp := registerUser(t, endpointURL, userData)
	defer resp.Body.Close()

	sessitonCookie := getSessionCookie(resp.Cookies())
	require.NotEqual(t, sessitonCookie, (*http.Cookie)(nil))

	t.Run("Success request with cookie", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodGet,
			endpointURL+"/test",
			nil,
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		req.AddCookie(sessitonCookie)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		bodyBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.Equal(t, key, string(bodyBytes))
	})

	t.Run("Negative request with cookie", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodGet,
			endpointURL+"/test",
			nil,
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

		bodyBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		assert.NotEqual(t, key, hex.EncodeToString(bodyBytes))
	})
}
