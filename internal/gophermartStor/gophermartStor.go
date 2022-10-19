package gophermartstor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"math"
	"net/http"
	"time"

	accrualStor "github.com/GermanVor/go-tpl/internal/accrualStor"
	"github.com/GermanVor/go-tpl/internal/common"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type SetOrderStatus uint

const (
	SetOrderStatusErr SetOrderStatus = iota
	SetOrderStatusAlreadyAccepted
	SetOrderStatusAccepted
)

type OrderStatus string

const (
	OrderStatusNew        OrderStatus = "NEW"
	OrderStatusProcessing OrderStatus = "PROCESSING"
	OrderStatusInvalid    OrderStatus = "INVALID"
	OrderStatusProcessed  OrderStatus = "PROCESSED"
)

type OrdersForEachObject struct {
	Number     string      `json:"number"`
	Status     OrderStatus `json:"status"`
	Accrual    float64     `json:"accrual,omitempty"`
	UploadedAt string      `json:"uploaded_at"`
}
type OrdersForEachHandler func(order *OrdersForEachObject) error

type WithdrawalObject struct {
	Order       string  `json:"order"`
	Sum         float64 `json:"sum"`
	ProcessedAt string  `json:"processed_at"`
}
type WithdrawalsForEachHandler func(withdrawal *WithdrawalObject) error

type Interface interface {
	InitOrder(userID string, orderID string) (SetOrderStatus, error)
	OrdersForEach(userID string, handler OrdersForEachHandler) error
	GetBalance(userID string) (*Balance, error)
	MakeWithdrawBalance(userID string, orderID string, sum float64) error
	WithdrawalsForEach(userID string, handler WithdrawalsForEachHandler) error
}

type storageObject struct {
	Interface

	dbPool *pgxpool.Pool

	accrualAddress string
}

var (
	ErrOrderAlreadyAccepted = errors.New("order already accepted another user")
	ErrInvalidOrderFormat   = errors.New("invalid order format")

	ErrNotEnoughFunds       = errors.New("there are not enough funds in the account")
	ErrInvalidOrderIDFormat = errors.New("invalid order id format")
)

const (
	// SELECT userID FROM ordersPool WHERE orderID=$1
	getUserIDByOrderSQL = "SELECT userID FROM ordersPool WHERE orderID=$1"

	// INSERT INTO ordersPool (userID, orderID, status, uploaded_at)
	// VALUES ($1, $2, $3, $4)
	initOrderSQL = "INSERT INTO ordersPool (userID, orderID, status, uploaded_at) " +
		"VALUES ($1, $2, $3, $4)"

	// UPDATE ordersPool SET (status, accrual, uploaded_at) = ($2, $3, $4) WHERE orderID=$1
	setOrderSQL = "UPDATE ordersPool SET (status, accrual, uploaded_at) = ($2, $3, $4) WHERE orderID=$1"

	// SELECT orderID, status, uploaded_at, accrual FROM ordersPool;
	selectOrderSQL = "SELECT orderID, status, uploaded_at, accrual FROM ordersPool WHERE userID=$1"

	// UPDATE balances SET current=current-$2 WHERE current-$2>=0 AND userID=$1
	spendBalanceSQL = "UPDATE balances SET current=current-$2, withdrawn=$2 WHERE current-$2>=0 AND userID=$1"

	// UPDATE balances SET current=current+$2 WHERE userID=$1
	increaseBalanceSQL = "UPDATE balances SET current=current+$2 WHERE userID=$1"

	// SELECT current, withdrawn FROM balances WHERE userID=$1;
	selectBalanceSQL = "SELECT current, withdrawn FROM balances WHERE userID=$1"

	// SELECT orderID, sum, processed_at FROM orderHistory WHERE userID=$1
	selectWithdrawalSQL = "SELECT orderID, sum, processed_at FROM orderHistory WHERE userID=$1"

	// INSERT INTO orderHistory (userID, orderID, sum, processed_at) VALUES ($1, $2, $3, $4)
	addWithdrawalSQL = "INSERT INTO orderHistory (userID, orderID, sum, processed_at) VALUES ($1, $2, $3, $4)"

	// INSERT INTO balances (userID, current, withdrawn) VALUES ($1, 0, 0)
	CreateBalanceSQL = "INSERT INTO balances (userID, current, withdrawn) VALUES ($1, 0, 0)"
)

func CreateBalancesTable(tx pgx.Tx) error {
	sql := "CREATE TABLE IF NOT EXISTS balances (" +
		"userID text UNIQUE, " +
		"current decimal, " +
		"withdrawn decimal" +
		")"

	_, err := tx.Exec(context.TODO(), sql)
	return err
}

func Init(databaseURI string, accrualAddress string) Interface {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Printf("Connected to DB %s successfully, gophermartStor\n", databaseURI)

	tx, err := conn.Begin(context.TODO())
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer tx.Rollback(context.TODO())

	{
		sql := "CREATE TABLE IF NOT EXISTS ordersPool (" +
			"userID text, " +
			"orderID text UNIQUE, " +
			"status text, " +
			"accrual decimal DEFAULT 0, " +
			"uploaded_at text" +
			")"

		_, err = tx.Exec(context.TODO(), sql)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	{
		err := CreateBalancesTable(tx)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	{
		sql := "CREATE TABLE IF NOT EXISTS orderHistory (" +
			"userID text UNIQUE, " +
			"orderID text, " +
			"sum decimal, " +
			"processed_at text" +
			")"

		_, err = tx.Exec(context.TODO(), sql)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	err = tx.Commit(context.TODO())
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("Created Tables (ordersPool, orderHistory, balances) successfully")

	return &storageObject{
		dbPool:         conn,
		accrualAddress: accrualAddress,
	}
}

func getTime() string {
	return time.Now().Format(time.RFC3339)
}

func (stor *storageObject) setOrder(userID string, order accrualStor.Order) error {
	order.Accrual = math.Ceil(order.Accrual*100) / 100
	uploadedAt := getTime()

	status := OrderStatusInvalid
	switch order.Status {
	case accrualStor.OrderStatusProcessing:
		status = OrderStatusProcessing
	case accrualStor.OrderStatusProcessed:
		status = OrderStatusProcessed
	}

	if order.Status != accrualStor.OrderStatusProcessed {
		_, err := stor.dbPool.Exec(
			context.TODO(),
			setOrderSQL,
			order.Order,
			status,
			order.Accrual,
			uploadedAt,
		)

		return err
	}

	tx, err := stor.dbPool.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())

	_, err = tx.Exec(
		context.TODO(),
		setOrderSQL,
		order.Order,
		status,
		order.Accrual,
		uploadedAt,
	)

	if err != nil {
		return err
	}

	_, err = stor.dbPool.Exec(
		context.TODO(),
		increaseBalanceSQL,
		userID,
		order.Accrual,
	)
	if err != nil {
		return err
	}

	return tx.Commit(context.TODO())
}

func pollFunc(url string) (*accrualStor.Order, error) {
	req, err := http.NewRequest(
		http.MethodGet,
		url,
		nil,
	)

	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	order := accrualStor.Order{}
	err = json.Unmarshal(bodyBytes, &order)
	if err != nil {
		return nil, err
	}

	return &order, nil
}

func (stor *storageObject) startPolling(userID string, orderID string) {
	ticker := time.NewTicker(time.Second)

	go func() {
		defer func() {
			ticker.Stop()
		}()

		prevAccrualOrderStatus := accrualStor.OrderStatusRegistered

		for {
			<-ticker.C
			order, err := pollFunc(stor.accrualAddress + "/api/orders/" + orderID)
			if err != nil {
				log.Println("polling error", userID, err)
				continue
			}

			if order == nil {
				continue
			}

			if order.Order != orderID {
				continue
			}

			if prevAccrualOrderStatus == order.Status {
				continue
			}

			err = stor.setOrder(userID, *order)
			if err != nil {
				log.Println("polling error", userID, order, err)
				continue
			}

			prevAccrualOrderStatus = order.Status

			if order.Status == accrualStor.OrderStatusInvalid ||
				order.Status == accrualStor.OrderStatusProcessed {
				break
			}
		}
	}()
}

func (stor *storageObject) InitOrder(userID string, orderID string) (SetOrderStatus, error) {
	if !common.CheckOrderIDFormat(orderID) {
		return SetOrderStatusErr, ErrInvalidOrderIDFormat
	}

	uploadedAt := getTime()
	_, err := stor.dbPool.Exec(
		context.TODO(),
		initOrderSQL,
		userID,
		orderID,
		OrderStatusNew,
		uploadedAt,
	)
	if err != nil {
		if common.IsAlreadyCreatedRowErr(err) {
			tableUserID := ""

			err = stor.dbPool.QueryRow(
				context.TODO(),
				getUserIDByOrderSQL,
				orderID,
			).Scan(&tableUserID)
			if err == nil {
				if tableUserID == userID {
					return SetOrderStatusAlreadyAccepted, nil
				} else {
					return SetOrderStatusErr, ErrOrderAlreadyAccepted
				}
			}
		}

		return SetOrderStatusErr, err
	}

	stor.startPolling(userID, orderID)
	return SetOrderStatusAccepted, nil
}

func (stor *storageObject) OrdersForEach(userID string, handler OrdersForEachHandler) error {
	rows, err := stor.dbPool.Query(context.TODO(), selectOrderSQL, userID)
	if err != nil {
		return err
	}

	for rows.Next() {
		order := &OrdersForEachObject{}

		accrual := float64(0)
		err := rows.Scan(&order.Number, &order.Status, &order.UploadedAt, &accrual)
		if err != nil {
			return err
		}

		if order.Status != OrderStatusNew {
			order.Accrual = accrual
		}

		handler(order)
	}

	return nil
}

type Balance struct {
	Current   float64 `json:"current"`
	Withdrawn float64 `json:"withdrawn"`
}

func (stor *storageObject) GetBalance(userID string) (*Balance, error) {
	balance := &Balance{}

	err := stor.dbPool.QueryRow(context.TODO(), selectBalanceSQL, userID).
		Scan(&balance.Current, &balance.Withdrawn)
	if err != nil {
		return nil, err
	}

	return balance, nil
}

func (stor *storageObject) MakeWithdrawBalance(userID string, orderID string, sum float64) error {
	if !common.CheckOrderIDFormat(orderID) {
		return ErrInvalidOrderIDFormat
	}

	tx, err := stor.dbPool.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer tx.Rollback(context.TODO())

	tag, err := tx.Exec(context.TODO(), spendBalanceSQL, userID, sum)
	if err != nil {
		return err
	}

	if tag.RowsAffected() == 0 {
		return ErrNotEnoughFunds
	}

	_, err = tx.Exec(
		context.TODO(),
		addWithdrawalSQL,
		userID,
		orderID,
		sum,
		getTime(),
	)

	if err != nil {
		return err
	}

	return tx.Commit(context.TODO())
}

func (stor *storageObject) WithdrawalsForEach(userID string, handler WithdrawalsForEachHandler) error {
	rows, err := stor.dbPool.Query(context.TODO(), selectWithdrawalSQL, userID)
	if err != nil {
		return err
	}

	for rows.Next() {
		withdrawal := &WithdrawalObject{}

		err := rows.Scan(&withdrawal.Order, &withdrawal.Sum, &withdrawal.ProcessedAt)
		if err != nil {
			return err
		}

		handler(withdrawal)
	}

	return nil
}

// FOR REGISTRATION STOR
func CreateBalance(tx pgx.Tx, userID string) error {
	_, err := tx.Exec(context.TODO(), CreateBalanceSQL, userID)
	return err
}
