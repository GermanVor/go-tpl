package gophermarthandlers

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/GermanVor/go-tpl/internal/common"
	gophermartStor "github.com/GermanVor/go-tpl/internal/gophermartStor"
	"github.com/go-chi/chi"
)

func SetOrderHandler(w http.ResponseWriter, r *http.Request, stor gophermartStor.Interface) {
	if r.Header.Get("Content-Type") != common.TextPlaneStr {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	orderID := string(bodyBytes)

	userID := common.GetContextUserID(r)
	status, err := stor.InitOrder(userID, orderID)
	if err != nil {
		if errors.Is(err, gophermartStor.ErrOrderAlreadyAccepted) {
			http.Error(w, err.Error(), http.StatusConflict)
		} else if errors.Is(err, gophermartStor.ErrInvalidOrderIDFormat) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	if status == gophermartStor.SetOrderStatusAccepted {
		w.WriteHeader(http.StatusAccepted)
	} else if status == gophermartStor.SetOrderStatusAlreadyAccepted {
		w.WriteHeader(http.StatusOK)
	}
}

func GetOrdersHandler(w http.ResponseWriter, r *http.Request, stor gophermartStor.Interface) {
	userID := common.GetContextUserID(r)

	arr := make([]*gophermartStor.OrdersForEachObject, 0)
	err := stor.OrdersForEach(userID, func(order *gophermartStor.OrdersForEachObject) error {
		arr = append(arr, order)
		return nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(arr) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	bytes, err := json.Marshal(arr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", common.ApplicationJSONStr)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

type GetBalanceResponse gophermartStor.Balance

func GetBalanceHandler(w http.ResponseWriter, r *http.Request, stor gophermartStor.Interface) {
	userID := common.GetContextUserID(r)
	balance, err := stor.GetBalance(userID)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	balanceBytes, err := json.Marshal(balance)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", common.ApplicationJSONStr)
	w.Write(balanceBytes)
}

type MakeWithdrawResponse struct {
	Order string `json:"order"`
	Sum   float64 `json:"sum"`
}

func MakeWithdrawHandler(w http.ResponseWriter, r *http.Request, stor gophermartStor.Interface) {
	userID := common.GetContextUserID(r)

	response := MakeWithdrawResponse{}
	if err := json.NewDecoder(r.Body).Decode(&response); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := stor.MakeWithdrawBalance(userID, response.Order, response.Sum)
	if err != nil {
		if errors.Is(err, gophermartStor.ErrNotEnoughFunds) {
			http.Error(w, err.Error(), http.StatusPaymentRequired)
		} else if errors.Is(err, gophermartStor.ErrInvalidOrderIDFormat) {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func GetWithdrawalsHandler(w http.ResponseWriter, r *http.Request, stor gophermartStor.Interface) {
	userID := common.GetContextUserID(r)

	arr := make([]*gophermartStor.WithdrawalObject, 0)
	err := stor.WithdrawalsForEach(userID, func(withdrawal *gophermartStor.WithdrawalObject) error {
		arr = append(arr, withdrawal)
		return nil
	})

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if len(arr) == 0 {
		w.WriteHeader(http.StatusNoContent)
	} else {
		bytes, err := json.Marshal(arr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", common.ApplicationJSONStr)
		w.WriteHeader(http.StatusOK)
		w.Write(bytes)
	}
}

func InitRouter(r chi.Router, stor gophermartStor.Interface) {
	r.Route("/api/user/orders", func(r chi.Router) {
		r.Post("/", func(w http.ResponseWriter, r *http.Request) {
			SetOrderHandler(w, r, stor)
		})

		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			GetOrdersHandler(w, r, stor)
		})
	})

	r.Route("/api/user/balance", func(r chi.Router) {
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			GetBalanceHandler(w, r, stor)
		})

		r.Post("/withdraw", func(w http.ResponseWriter, r *http.Request) {
			MakeWithdrawHandler(w, r, stor)
		})
	})

	r.Get("/api/user/withdrawals", func(w http.ResponseWriter, r *http.Request) {
		GetWithdrawalsHandler(w, r, stor)
	})
}
