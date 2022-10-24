package common

import (
	"net/http"
	"strconv"

	"github.com/jackc/pgconn"
)

const (
	ApplicationJSONStr = "application/json"
	TextPlaneStr       = "text/plain"
	SessionTokenName   = "sessionToken"
)


type ContextKey uint

const (
	UserIDContextKey ContextKey = 1
)

func GetContextUserID(r *http.Request) string {
	return r.Context().Value(UserIDContextKey).(string)
}

func CheckOrderIDFormat(orderID string) bool {
	number, err := strconv.ParseUint(orderID, 10, 64)
	if err != nil {
		return false
	}

	checksum := func(number uint64) uint64 {
		luhn := uint64(0)
		for i := 0; number > 0; i++ {
			cur := number % 10

			if i%2 == 0 {
				cur = cur * 2
				if cur > 9 {
					cur = cur%10 + cur/10
				}
			}

			luhn += cur
			number = number / 10
		}

		return luhn % 10
	}

	return (number%10+checksum(number/10))%10 == 0
}

func IsAlreadyCreatedRowErr(err error) bool {
	if e, ok := err.(*pgconn.PgError); ok && e.Code == "23505" {
		return true
	}

	return false
}
