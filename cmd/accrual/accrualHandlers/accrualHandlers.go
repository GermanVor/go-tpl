package accrualhandlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	accrualStor "github.com/GermanVor/go-tpl/internal/accrualStor"
	"github.com/GermanVor/go-tpl/internal/common"
	"github.com/go-chi/chi"
)

type GetOrderResponse accrualStor.Order

func GetOrderHandler(w http.ResponseWriter, r *http.Request, stor accrualStor.Interface) {
	orderID := chi.URLParam(r, "orderID")

	orderPtr, err := stor.GetOrder(orderID)
	if err != nil {
		switch {
		case errors.Is(err, accrualStor.ErrInvalidOrderIDFormat):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, accrualStor.ErrExceededRequestsNumber):
			http.Error(w, err.Error(), http.StatusTooManyRequests)
		case errors.Is(err, accrualStor.ErrUnknownOrderID):
			http.Error(w, err.Error(), http.StatusNoContent)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	bytes, err := json.Marshal(orderPtr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Println("QWERTY", orderID, orderPtr)

	w.Write(bytes)
	w.WriteHeader(http.StatusOK)
}

func SetOrderHandler(w http.ResponseWriter, r *http.Request, stor accrualStor.Interface) {
	orderPackage := accrualStor.OrderPackage{}

	if err := json.NewDecoder(r.Body).Decode(&orderPackage); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err := stor.SetOrder(orderPackage)
	if err != nil {
		switch {
		case errors.Is(err, accrualStor.ErrInvalidOrderIDFormat):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, accrualStor.ErrOrderAlreadyAccepted):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func SetGoodRewardHandler(w http.ResponseWriter, r *http.Request, stor accrualStor.Interface) {
	if r.Header.Get("Content-Type") != common.ApplicationJSONStr {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	goodReward := accrualStor.GoodReward{}
	if err := json.NewDecoder(r.Body).Decode(&goodReward); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err := stor.SetGoodReward(goodReward)
	if err != nil {
		switch {
		case errors.Is(err, accrualStor.ErrInvalidGoodReward):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, accrualStor.ErrGoodRewardAlreadyAccepted):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

		return
	}

	w.WriteHeader(http.StatusOK)
}

func InitRouter(r *chi.Mux, stor accrualStor.Interface) {
	r.Get("/api/orders/{orderID}", func(w http.ResponseWriter, r *http.Request) {
		GetOrderHandler(w, r, stor)
	})

	r.Post("/api/orders", func(w http.ResponseWriter, r *http.Request) {
		SetOrderHandler(w, r, stor)
	})

	r.Post("/api/goods", func(w http.ResponseWriter, r *http.Request) {
		SetGoodRewardHandler(w, r, stor)
	})
}
