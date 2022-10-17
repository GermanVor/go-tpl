package userstor

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"strconv"

	"github.com/GermanVor/go-tpl/internal/common"
	gophermartStor "github.com/GermanVor/go-tpl/internal/gophermartStor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"golang.org/x/crypto/scrypt"
)

var (
	// INSERT INTO users (login, pass, salt, sessionToken) VALUES ($1, $2, $3, $4);
	insertUserSQL = "INSERT INTO users (login, pass, salt, sessionToken) " +
		"VALUES ($1, $2, $3, $4) RETURNING users.userID"

	// SELECT sessionToken FROM users WHERE login=$1 AND pass=$2;
	findUserBylogpassSQL = "SELECT sessionToken FROM users WHERE login=$1 AND pass=$2"

	// SELECT salt FROM users WHERE login=$1;
	getSaltSQL = "SELECT salt FROM users WHERE login=$1"

	// SELECT userID FROM users WHERE sessionToken=$1;
	getUserIDSQL = "SELECT userID FROM users WHERE sessionToken=$1"
)

type Interface interface {
	SignIn(login string, pass string) (string, error)
	LogIn(login string, pass string) (string, error)

	GetUserID(userID string) (string, error)
}

var (
	ErrLoginOccupied       = errors.New("the login is already occupied")
	ErrUnknownUser         = errors.New("invalid login/password pair")
	ErrUnknownSessionToken = errors.New("unknown user token")
)

type storageObject struct {
	Interface

	dbPool *pgxpool.Pool
}

func Init(databaseURI string) Interface {
	conn, err := pgxpool.Connect(context.TODO(), databaseURI)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Printf("Connected to DB %s successfully\n", databaseURI)

	sql := "CREATE TABLE IF NOT EXISTS users (" +
		"login text UNIQUE, " +
		"pass text, " +
		"salt text, " +
		"userID SERIAL, " +
		"sessionToken text " +
		");"

	_, err = conn.Exec(context.TODO(), sql)
	if err != nil {
		log.Fatalln(err.Error())
	}

	log.Println("Created Table successfully")

	return &storageObject{
		dbPool: conn,
	}
}

func createSessionToken() string {
	return uuid.New().String()
}

func getLogin(login string) string {
	loginHash := sha1.Sum([]byte(login))
	return hex.EncodeToString(loginHash[:])
}

func getPass(pass string, salt []byte) (string, error) {
	passHash, err := scrypt.Key([]byte(pass), salt, 1<<14, 8, 1, 64)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(passHash), nil
}

func (stor *storageObject) SignIn(login string, pass string) (string, error) {
	salt := make([]byte, 32)
	_, err := io.ReadFull(rand.Reader, salt)
	if err != nil {
		return "", err
	}
	saltStr := hex.EncodeToString(salt)

	loginStr := getLogin(login)
	passStr, err := getPass(pass, salt)
	if err != nil {
		return "", err
	}

	sessionToken := createSessionToken()

	tx, err := stor.dbPool.Begin(context.TODO())
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.TODO())

	userID := 0
	err = tx.QueryRow(
		context.TODO(),
		insertUserSQL,
		loginStr,
		passStr,
		saltStr,
		sessionToken,
	).Scan(&userID)

	if err != nil {
		if common.IsAlreadyCreatedRowErr(err) {
			return "", ErrLoginOccupied
		}

		return "", err
	}

	err = gophermartStor.CreateBalance(tx, strconv.Itoa(userID))
	if err != nil {
		return "", err
	}

	return sessionToken, tx.Commit(context.TODO())
}

func (stor *storageObject) LogIn(login string, pass string) (string, error) {
	loginStr := getLogin(login)

	saltStr := ""
	err := stor.dbPool.QueryRow(context.TODO(), getSaltSQL, loginStr).Scan(&saltStr)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrUnknownUser
		} else {
			return "", err
		}
	}

	salt, err := hex.DecodeString(saltStr)
	if err != nil {
		return "", err
	}

	passStr, err := getPass(pass, salt)
	if err != nil {
		return "", err
	}

	userID := ""

	err = stor.dbPool.QueryRow(
		context.TODO(),
		findUserBylogpassSQL,
		loginStr,
		passStr,
	).Scan(&userID)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrUnknownUser
		} else {
			return "", err
		}
	}

	// place where we can recreate userID

	return userID, nil
}

func (stor *storageObject) GetUserID(sessionToken string) (string, error) {
	userID := 0
	err := stor.dbPool.QueryRow(context.TODO(), getUserIDSQL, sessionToken).Scan(&userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrUnknownSessionToken
		} else {
			return "", err
		}
	}

	return strconv.Itoa(userID), nil
}
