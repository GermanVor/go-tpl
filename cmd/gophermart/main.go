package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	gophermartHandlers "github.com/GermanVor/go-tpl/cmd/gophermart/gophermartHandlers"
	registrationHandlers "github.com/GermanVor/go-tpl/cmd/gophermart/registrationHandlers"
	gophermartStor "github.com/GermanVor/go-tpl/internal/gophermartStor"
	userStor "github.com/GermanVor/go-tpl/internal/userStor"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/joho/godotenv"
)

var defaultCompressibleContentTypes = []string{
	"application/javascript",
	"application/json",
	"text/css",
	"text/html",
	"text/plain",
	"text/xml",
}

var address = "localhost:8081"
var accrualAddress = "localhost:8080"
var databaseURI = "postgres://zzman:@localhost:5432/postgres"

func initEnv() {
	const aUsage = "Service launch address and port"
	const dbUsage = "Database connection address"
	const rUsage = "Address of the accrual calculation system"

	godotenv.Load(".env")
	flag.Parse()

	// -------------- RUN_ADDRESS --------------
	if addressEnv, ok := os.LookupEnv("RUN_ADDRESS"); ok {
		address = addressEnv
	}
	flag.StringVar(&address, "a", address, aUsage)
	// -----------------------------------------

	// -------------- DATABASE_URI --------------
	if databaseURLEnv, ok := os.LookupEnv("DATABASE_URI"); ok {
		databaseURI = databaseURLEnv
	}
	flag.StringVar(&databaseURI, "d", databaseURI, dbUsage)
	// ------------------------------------------

	// -------------- ACCRUAL_SYSTEM_ADDRESS --------------
	if accrualURLEnv, ok := os.LookupEnv("ACCRUAL_SYSTEM_ADDRESS"); ok {
		accrualAddress = accrualURLEnv
	}
	flag.StringVar(&accrualAddress, "r", accrualAddress, rUsage)
	// ----------------------------------------------------
}

func main() {
	initEnv()

	gophermartStorage := gophermartStor.Init(databaseURI, accrualAddress)
	userStorage := userStor.Init(databaseURI)

	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Compress(5, defaultCompressibleContentTypes...))

	// Public
	r.Group(func(r chi.Router) {
		registrationHandlers.InitRouter(r, userStorage)
	})

	// Private
	r.Group(func(r chi.Router) {
		r.Use(func(h http.Handler) http.Handler {
			return registrationHandlers.CheckUserTokenMiddleware(h, userStorage)
		})

		gophermartHandlers.InitRouter(r, gophermartStorage)
	})

	log.Println("Server Started: http://" + address)

	log.Fatal(http.ListenAndServe(address, r))
}
