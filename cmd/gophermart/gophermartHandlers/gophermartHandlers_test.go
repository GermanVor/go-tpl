package gophermarthandlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	gophermartHandlers "github.com/GermanVor/go-tpl/cmd/gophermart/gophermartHandlers"
	registrationHandlers "github.com/GermanVor/go-tpl/cmd/gophermart/registrationHandlers"
	accrualStor "github.com/GermanVor/go-tpl/internal/accrualStor"
	"github.com/GermanVor/go-tpl/internal/common"
	gophermartStor "github.com/GermanVor/go-tpl/internal/gophermartStor"
	"github.com/bmizerany/assert"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

const databaseURI = "postgres://zzman:@localhost:5432/test"
const orderID = "70757088342"

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
		"ordersPool",
		"balances",
		"orderHistory",
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

var i = 0
var mux sync.RWMutex
var accrualScenerio = []accrualStor.Order{
	{Order: "", Accrual: 0, Status: accrualStor.OrderStatusRegistered},
	{Order: "", Accrual: 11.5, Status: accrualStor.OrderStatusProcessing},
	{Order: "", Accrual: 22, Status: accrualStor.OrderStatusProcessed},
}

func initAccrualServerMock() (string, func()) {
	r := chi.NewRouter()

	r.Get("/api/orders/{orderID}", func(w http.ResponseWriter, r *http.Request) {
		mux.RLock()
		order := accrualScenerio[i]
		mux.RUnlock()

		order.Order = chi.URLParam(r, "orderID")

		bytes, err := json.Marshal(order)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Write(bytes)
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(r)

	destructor := func() {
		ts.Close()
	}

	return ts.URL, destructor
}

func createTestEnv(t *testing.T, accrualAddress string) (string, func()) {
	r := chi.NewRouter()

	gophermartStorage := gophermartStor.Init(databaseURI, accrualAddress)

	// balance row creates in SignIn handler
	const userID = "qwertyUserID"
	{
		conn, err := pgxpool.Connect(context.TODO(), databaseURI)
		require.NoError(t, err)

		tx, err := conn.Begin(context.TODO())
		require.NoError(t, err)

		require.NoError(t, gophermartStor.CreateBalance(tx, userID))
		require.NoError(t, tx.Commit(context.TODO()))
	}

	r.Group(func(r chi.Router) {
		r.Use(func(h http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				newContext := context.WithValue(r.Context(), common.UserIDContextKey, userID)
				r = r.WithContext(newContext)

				h.ServeHTTP(w, r)
			})
		})

		gophermartHandlers.InitRouter(r, gophermartStorage)
	})

	ts := httptest.NewServer(r)

	destructor := func() {
		ts.Close()
		cleanDatabase()
	}

	return ts.URL, destructor
}

func setOrderRequest(t *testing.T, endpointURL string, orderID string) *http.Response {
	orderIDData := []byte(orderID)

	req, err := http.NewRequest(
		http.MethodPost,
		endpointURL+"/api/user/orders",
		bytes.NewReader(orderIDData),
	)

	require.NoError(t, err)

	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestSetOrder(t *testing.T) {
	endpointURL, destructor := createTestEnv(t, "")
	defer destructor()

	t.Run("Success POST /api/user/orders", func(t *testing.T) {
		resp := setOrderRequest(t, endpointURL, orderID)
		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("Success x2 POST /api/user/orders", func(t *testing.T) {
		resp := setOrderRequest(t, endpointURL, orderID)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	})
}

func getOrdersRequest(t *testing.T, endpointURL string) *http.Response {
	req, err := http.NewRequest(
		http.MethodGet,
		endpointURL+"/api/user/orders",
		nil,
	)

	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestGetOrder(t *testing.T) {
	accrualAddress, accrualDestructor := initAccrualServerMock()
	defer accrualDestructor()

	endpointURL, destructor := createTestEnv(t, accrualAddress)
	defer destructor()

	{
		resp := setOrderRequest(t, endpointURL, orderID)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		resp.Body.Close()
	}

	time.Sleep(2 * time.Second)

	t.Run("Success GET /api/user/orders New Status", func(t *testing.T) {
		resp := getOrdersRequest(t, endpointURL)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respBody []gophermartStor.OrdersForEachObject
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		require.Equal(t, 1, len(respBody))
		assert.Equal(t, gophermartStor.OrderStatusNew, respBody[0].Status)
		assert.Equal(t, accrualScenerio[i].Accrual, respBody[0].Accrual)
		assert.Equal(t, orderID, respBody[0].Number)
	})

	mux.Lock()
	i++
	mux.Unlock()
	time.Sleep(2 * time.Second)

	t.Run("Success GET /api/user/orders Processing Status", func(t *testing.T) {
		resp := getOrdersRequest(t, endpointURL)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respBody []gophermartStor.OrdersForEachObject
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		require.Equal(t, 1, len(respBody))
		assert.Equal(t, gophermartStor.OrderStatusProcessing, respBody[0].Status)
		assert.Equal(t, accrualScenerio[i].Accrual, respBody[0].Accrual)
		assert.Equal(t, orderID, respBody[0].Number)
	})

	mux.Lock()
	i++
	mux.Unlock()
	time.Sleep(2 * time.Second)

	t.Run("Success GET /api/user/orders Processed Status", func(t *testing.T) {
		resp := getOrdersRequest(t, endpointURL)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var respBody []gophermartStor.OrdersForEachObject
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		require.Equal(t, 1, len(respBody))
		assert.Equal(t, gophermartStor.OrderStatusProcessed, respBody[0].Status)
		assert.Equal(t, accrualScenerio[i].Accrual, respBody[0].Accrual)
		assert.Equal(t, orderID, respBody[0].Number)
	})
}

func checkBalanceRequest(t *testing.T, endpointURL string) *http.Response {
	req, err := http.NewRequest(
		http.MethodGet,
		endpointURL+"/api/user/balance",
		nil,
	)

	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func makeWithdrawRequest(t *testing.T, endpointURL string, withdraw gophermartHandlers.MakeWithdrawResponse) *http.Response {
	reqBodyBytes, err := json.Marshal(withdraw)
	require.NoError(t, err)

	req, err := http.NewRequest(
		http.MethodPost,
		endpointURL+"/api/user/balance/withdraw",
		bytes.NewReader(reqBodyBytes),
	)

	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestBalancesHandlers(t *testing.T) {
	accrualAddress, accrualDestructor := initAccrualServerMock()
	defer accrualDestructor()

	endpointURL, destructor := createTestEnv(t, accrualAddress)
	defer destructor()

	t.Run("Check Balance Before Order", func(t *testing.T) {
		resp := checkBalanceRequest(t, endpointURL)
		defer resp.Body.Close()

		var respBody gophermartStor.Balance
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		assert.Equal(t, float64(0), respBody.Current)
		assert.Equal(t, float64(0), respBody.Withdrawn)
	})

	{
		i = 2
		resp := setOrderRequest(t, endpointURL, orderID)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		resp.Body.Close()
	}

	time.Sleep(2 * time.Second)

	t.Run("Check After Order", func(t *testing.T) {
		resp := checkBalanceRequest(t, endpointURL)
		defer resp.Body.Close()

		var respBody gophermartStor.Balance
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		assert.Equal(t, accrualScenerio[i].Accrual, respBody.Current)
		assert.Equal(t, float64(0), respBody.Withdrawn)
	})

	{
		resp := checkBalanceRequest(t, endpointURL)
		defer resp.Body.Close()

		var respBody gophermartStor.Balance
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		require.Equal(t, accrualScenerio[i].Accrual, respBody.Current)
	}

	delta := float64(1.5)
	reqBody := gophermartHandlers.MakeWithdrawResponse{
		Order: orderID,
		Sum:   accrualScenerio[i].Accrual - delta,
	}

	t.Run("Success Withdraw Request", func(t *testing.T) {
		resp := makeWithdrawRequest(t, endpointURL, reqBody)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("Withdraw History check", func(t *testing.T) {
		req, err := http.NewRequest(
			http.MethodGet,
			endpointURL+"/api/user/withdrawals",
			nil,
		)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equal(t, http.StatusOK, resp.StatusCode)

		var respBody []gophermartStor.WithdrawalObject
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		require.Equal(t, 1, len(respBody))
		assert.Equal(t, reqBody.Sum, respBody[0].Sum)
		assert.Equal(t, reqBody.Order, respBody[0].Order)
	})

	t.Run("Check After Withdraw", func(t *testing.T) {
		resp := checkBalanceRequest(t, endpointURL)
		defer resp.Body.Close()

		var respBody gophermartStor.Balance
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))

		assert.Equal(t, delta, respBody.Current)
		assert.Equal(t, reqBody.Sum, respBody.Withdrawn)
	})
}
