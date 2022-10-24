package accrualhandlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	accrualHandlers "github.com/GermanVor/go-tpl/cmd/accrual/accrualHandlers"
	accrualStor "github.com/GermanVor/go-tpl/internal/accrualStor"
	"github.com/bmizerany/assert"
	"github.com/go-chi/chi"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/require"
)

const databaseURI = "postgres://zzman:@localhost:5432/test"

func cleanDatabase() {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	dropTableNameList := []string{
		"ordersReward",
		"goods",
		"goodsBaskets",
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

func createTestEnv() (string, func()) {
	r := chi.NewRouter()

	stor := accrualStor.Init(databaseURI, 10)
	accrualHandlers.InitRouter(r, stor)

	ts := httptest.NewServer(r)

	destructor := func() {
		ts.Close()
		cleanDatabase()
	}

	return ts.URL, destructor
}

var (
	orderID = "70757088342"

	goodsRewards = []accrualStor.GoodReward{
		{
			Match:      "Rty",
			Reward:     10,
			RewardType: accrualStor.RewardTypePercent,
		},
		{
			Match:      "Qwe",
			Reward:     11.2,
			RewardType: accrualStor.RewardTypePT,
		},
	}

	orderPackage = accrualStor.OrderPackage{
		Order: orderID,
		Goods: []accrualStor.Good{
			{
				Description: "SAsd " + goodsRewards[0].Match + " dsxd",
				Price:       100,
			},
			{
				Description: "sacca " + goodsRewards[1].Match + " asdsd",
				Price:       110.5,
			},
		},
	}
)

func setGoodsRewardsRequest(t *testing.T, goodReward accrualStor.GoodReward, endpointURL string) *http.Response {
	goodsData, err := json.Marshal(goodReward)
	require.NoError(t, err)

	req, err := http.NewRequest(
		http.MethodPost,
		endpointURL+"/api/goods",
		bytes.NewReader(goodsData),
	)

	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestRegisterGoodsRewards(t *testing.T) {
	endpointURL, destructor := createTestEnv()
	defer destructor()

	t.Run("Success /api/goods", func(t *testing.T) {
		for _, goodReward := range goodsRewards {
			resp := setGoodsRewardsRequest(t, goodReward, endpointURL)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			resp.Body.Close()
		}
	})

	t.Run("Negative /api/goods", func(t *testing.T) {
		for _, goodReward := range goodsRewards {
			resp := setGoodsRewardsRequest(t, goodReward, endpointURL)
			assert.Equal(t, http.StatusConflict, resp.StatusCode)
			resp.Body.Close()
		}
	})
}

func setOrderRequest(t *testing.T, endpointURL string) *http.Response {
	orderData, err := json.Marshal(orderPackage)
	require.NoError(t, err)

	req, err := http.NewRequest(
		http.MethodPost,
		endpointURL+"/api/orders",
		bytes.NewReader(orderData),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestRegisterOrder(t *testing.T) {
	endpointURL, destructor := createTestEnv()
	defer destructor()

	{
		resp := setGoodsRewardsRequest(t, goodsRewards[0], endpointURL)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	{
		resp := setGoodsRewardsRequest(t, goodsRewards[1], endpointURL)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	t.Run("Success Order registration /api/orders", func(t *testing.T) {
		resp := setOrderRequest(t, endpointURL)
		assert.Equal(t, http.StatusAccepted, resp.StatusCode)
		resp.Body.Close()
	})

	t.Run("Negative Order registration /api/orders", func(t *testing.T) {
		resp := setOrderRequest(t, endpointURL)
		assert.Equal(t, http.StatusConflict, resp.StatusCode)
		resp.Body.Close()
	})
}

func getOrderRequest(t *testing.T, endpointURL string, orderID string) *http.Response {
	req, err := http.NewRequest(
		http.MethodGet,
		endpointURL+"/api/orders/"+orderID,
		nil,
	)

	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func TestGetOrder(t *testing.T) {
	endpointURL, destructor := createTestEnv()
	defer destructor()

	{
		resp := setGoodsRewardsRequest(t, goodsRewards[0], endpointURL)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	{
		resp := setGoodsRewardsRequest(t, goodsRewards[1], endpointURL)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()
	}

	{
		resp := setOrderRequest(t, endpointURL)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		resp.Body.Close()
	}

	t.Run("Success Get Order Status /api/orders/{orderID}", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				t.Log("accural service has not calculated user accrual")
				t.Fail()
				return
			case <-ticker.C:
				resp := getOrderRequest(t, endpointURL, orderID)

				respBody := accrualStor.Order{}
				if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
					resp.Body.Close()
					t.Log(err.Error())
					t.Fail()
					return
				}
				resp.Body.Close()

				if respBody.Status == accrualStor.OrderStatusProcessed {
					assert.Equal(t, http.StatusOK, resp.StatusCode)

					expectedAccural := goodsRewards[1].Reward + orderPackage.Goods[0].Price*(goodsRewards[0].Reward/100)

					assert.Equal(t, orderID, respBody.Order)
					assert.Equal(t, expectedAccural, respBody.Accrual)
					assert.Equal(t, "PROCESSED", string(respBody.Status))

					return
				}
			}
		}
	})

	t.Run("Negative Get Order Status /api/orders/{orderID}", func(t *testing.T) {
		resp := getOrderRequest(t, endpointURL, orderID+"qweqwe")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}
