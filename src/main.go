package main

import (
	"log"
	"os"
	"os/signal"
)

func main() {
	log.Println("PotatoMQ daemon started")
	sig := make(chan os.Signal, 1)
	signal.Notify(sig)
	<-sig
}
