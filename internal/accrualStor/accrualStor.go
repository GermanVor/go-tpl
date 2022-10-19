package accrualstor

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/GermanVor/go-tpl/internal/common"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type OrderStatus string

const (
	OrderStatusRegistered OrderStatus = "REGISTERED"
	OrderStatusInvalid    OrderStatus = "INVALID"
	OrderStatusProcessing OrderStatus = "PROCESSING"
	OrderStatusProcessed  OrderStatus = "PROCESSED"
)

type Order struct {
	Order   string      `json:"order"`
	Status  OrderStatus `json:"status"`
	Accrual float64     `json:"accrual"`
}

type Good struct {
	Description string  `json:"description"`
	Price       float64 `json:"price"`
}

type OrderPackage struct {
	Order string `json:"order"`
	Goods []Good `json:"goods"`
}

type RewardType string

const (
	RewardTypePercent RewardType = "%"
	RewardTypePT      RewardType = "pt"
)

type GoodReward struct {
	Match      string     `json:"match"`
	Reward     float64    `json:"reward"`
	RewardType RewardType `json:"reward_type"`
}

type Interface interface {
	GetOrder(orderID string) (*Order, error)
	SetOrder(orderPackage OrderPackage) error
	SetGoodReward(goodReward GoodReward) error
}

func InitCheckRequestsLimiter(requestCountLimit uint16) func() bool {
	countMux := sync.Mutex{}
	count := uint16(0)

	ticker := time.NewTicker(time.Minute)

	go func() {
		for {
			<-ticker.C
			countMux.Lock()
			count = 0
			countMux.Unlock()
		}
	}()

	checkRequestCountLimit := func() bool {
		countMux.Lock()
		defer countMux.Unlock()

		if count+1 >= requestCountLimit {
			return false
		}
		count++

		return true
	}

	return checkRequestCountLimit
}

type storageObject struct {
	Interface

	dbPool *pgxpool.Pool

	checkRequestsLimit func() bool
}

var (
	ErrOrderAlreadyAccepted      = errors.New("order already accepted another user for processing")
	ErrGoodRewardAlreadyAccepted = errors.New("good reward already accepted another user for processing")
	ErrInvalidGoodReward         = errors.New("invalid good reward")
	ErrInvalidOrderIDFormat      = errors.New("invalid order id format")
	ErrExceededRequestsNumber    = errors.New("too many requests")
	ErrUnknownOrderID            = errors.New("unknown order id")
)

var (
	// SELECT status, accrual FROM ordersReward WHERE order=$1
	getOrderSQL = "SELECT status, accrual FROM ordersReward WHERE orderID=$1"

	// INSERT INTO ordersReward (orderID, status, accrual) VALUES ($1, $2, $3)
	insertOrderSQL = "INSERT INTO ordersReward (orderID, status, accrual) VALUES ($1, $2, $3)"

	// UPDATE ordersReward SET status=$2 WHERE orderID=$1
	setOrderStatusSQL = "UPDATE ordersReward SET status=$2 WHERE orderID=$1"

	// UPDATE ordersReward SET status='PROCESSED', accrual=$2 WHERE orderID=$1
	setOrderAccrualSQL = "UPDATE ordersReward SET " +
		"status='" + string(OrderStatusProcessed) + "', accrual=$2 WHERE orderID=$1"

	// INSERT INTO goodsBaskets (orderID, description, price) VALUES ($1, $2, $3)
	setGoodsBasketsSQL = "INSERT INTO goodsBaskets (orderID, description, price) VALUES ($1, $2, $3)"

	// INSERT INTO goods (match, reward, reward_type) VALUES ($1, $2, $3)
	setGoodRewardSQL = "INSERT INTO goods (match, reward, reward_type) VALUES ($1, $2, $3)"

	// SELECT match, reward, reward_type FROM goods
	selectGoodRewardSQL = "SELECT match, reward, reward_type FROM goods"
)

func Init(databaseURI string, requestCountLimit uint16) Interface {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Printf("Connected to DB %s successfully\n", databaseURI)

	tx, err := conn.Begin(context.TODO())
	if err != nil {
		log.Fatalln(err.Error())
	}
	defer tx.Rollback(context.TODO())

	{
		sql := "CREATE TABLE IF NOT EXISTS ordersReward (" +
			"orderID text UNIQUE, " +
			"status text, " +
			"accrual decimal" +
			")"

		_, err = tx.Exec(context.TODO(), sql)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	{
		sql := "CREATE TABLE IF NOT EXISTS goods (" +
			"match text UNIQUE, " +
			"reward decimal, " +
			"reward_type text" +
			")"

		_, err = tx.Exec(context.TODO(), sql)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}

	{
		sql := "CREATE TABLE IF NOT EXISTS goodsBaskets (" +
			"orderID text, " +
			"description text, " +
			"price decimal" +
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

	log.Println("Created Tables (ordersReward, goods, goodsBaskets) successfully")

	return &storageObject{
		dbPool:             conn,
		checkRequestsLimit: InitCheckRequestsLimiter(requestCountLimit),
	}
}

func (stor *storageObject) GetOrder(orderID string) (*Order, error) {
	if !common.CheckOrderIDFormat(orderID) {
		return nil, ErrInvalidOrderIDFormat
	}

	if !stor.checkRequestsLimit() {
		return nil, ErrExceededRequestsNumber
	}

	order := &Order{
		Order: orderID,
	}

	err := stor.dbPool.QueryRow(context.TODO(), getOrderSQL, orderID).
		Scan(&order.Status, &order.Accrual)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrUnknownOrderID
		}

		return nil, err
	}

	return order, nil
}

func (stor *storageObject) startCalculateAccrual(orderPackage OrderPackage) {
	var err error
	defer func() {
		if err != nil {
			log.Println("Order Calculating order ", orderPackage.Order, err)
			stor.dbPool.Exec(
				context.TODO(),
				setOrderStatusSQL,
				orderPackage.Order,
				OrderStatusInvalid,
			)
		}
	}()

	_, err = stor.dbPool.Exec(
		context.TODO(),
		setOrderStatusSQL,
		orderPackage.Order,
		OrderStatusProcessing,
	)
	if err != nil {
		return
	}

	rows, err := stor.dbPool.Query(context.TODO(), selectGoodRewardSQL)
	if err != nil {
		return
	}

	accrual := float64(0)
	goodReward := GoodReward{}

	for rows.Next() {
		err = rows.Scan(&goodReward.Match, &goodReward.Reward, &goodReward.RewardType)
		if err != nil {
			return
		}

		for i := 0; i != len(orderPackage.Goods); i++ {
			if strings.Contains(orderPackage.Goods[i].Description, goodReward.Match) {
				switch goodReward.RewardType {
				case RewardTypePT:
					accrual += goodReward.Reward
				case RewardTypePercent:
					accrual += float64(orderPackage.Goods[i].Price) * float64(goodReward.Reward) / 100
				}
			}
		}
	}

	_, err = stor.dbPool.Exec(
		context.TODO(),
		setOrderAccrualSQL,
		orderPackage.Order,
		accrual,
	)
}

func (stor *storageObject) SetOrder(orderPackage OrderPackage) error {
	if !common.CheckOrderIDFormat(orderPackage.Order) {
		return ErrInvalidOrderIDFormat
	}

	_, err := stor.dbPool.Exec(
		context.TODO(),
		insertOrderSQL,
		orderPackage.Order,
		OrderStatusRegistered,
		float64(0),
	)
	if err != nil {
		if common.IsAlreadyCreatedRowErr(err) {
			return ErrOrderAlreadyAccepted
		}

		return err
	}

	tx, err := stor.dbPool.Begin(context.TODO())
	if err != nil {
		return err
	}
	defer func(stor *storageObject, orderPackage OrderPackage) {
		if errors.Is(tx.Rollback(context.TODO()), pgx.ErrTxClosed) && err == nil {
			stor.startCalculateAccrual(orderPackage)
		}
	}(stor, orderPackage)

	for i := 0; i != len(orderPackage.Goods); i++ {
		_, err = tx.Exec(context.TODO(),
			setGoodsBasketsSQL,
			orderPackage.Order,
			orderPackage.Goods[i].Description,
			orderPackage.Goods[i].Price,
		)
		if err != nil {
			return err
		}
	}

	err = tx.Commit(context.TODO())
	return err
}

func checkGoodRewardType(rewardType string) bool {
	switch rewardType {
	case string(RewardTypePT):
	case string(RewardTypePercent):
	default:
		return false
	}

	return true
}

func (stor *storageObject) SetGoodReward(goodReward GoodReward) error {
	if goodReward.Match == "" || goodReward.Reward < 0 || !checkGoodRewardType(string(goodReward.RewardType)) {
		return ErrInvalidGoodReward
	}

	_, err := stor.dbPool.Exec(
		context.TODO(),
		setGoodRewardSQL,
		goodReward.Match,
		goodReward.Reward,
		goodReward.RewardType,
	)

	if err != nil {
		if common.IsAlreadyCreatedRowErr(err) {
			return ErrGoodRewardAlreadyAccepted
		}

		return err
	}

	return nil
}
