package main

import (
	"log"

	"github.com/chmikata/proglog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8888")
	log.Fatal(srv.ListenAndServe())
}
