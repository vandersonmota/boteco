package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/vandersonmota/potatomq/src/server"
)

func main() {
	log.Println("PotatoMQ daemon started")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig)
	s := server.NewTCPServer("tcp", ":9876", sig)
	s.Listen()
}
