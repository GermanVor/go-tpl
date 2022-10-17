package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	accrualHandlers "github.com/GermanVor/go-tpl/cmd/accrual/accrualHandlers"
	accrualStor "github.com/GermanVor/go-tpl/internal/accrualStor"
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

var address = "localhost:8080"
var databaseURI = "postgres://zzman:@localhost:5432/postgres"

func InitEnv() {
	const aUsage = "Service launch address and port"
	const dbUsage = "Database connection address"

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
}

func main() {
	InitEnv()

	stor := accrualStor.Init(databaseURI, 10000)
	r := chi.NewRouter()

	r.Use(middleware.Logger)
	r.Use(middleware.Compress(5, defaultCompressibleContentTypes...))

	accrualHandlers.InitRouter(r, stor)

	log.Println("Server Started: http://" + address)

	log.Fatal(http.ListenAndServe(address, r))
}
