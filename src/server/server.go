package server

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"
)

type Server interface {
	Listen() error
}

type TCPServer struct {
	address    string
	tcpVersion string
	sigChan    chan os.Signal
}

func (s *TCPServer) Listen() error {
	l, err := net.Listen(s.tcpVersion, s.address)
	if err != nil {
		log.Fatalf("Could not start server at address %s", s.address)
		return errors.New("Error when creating the TCP Server")
	}
	defer l.Close()
	rand.Seed(time.Now().Unix())

	for {
		log.Println("Default")
		go func() {
			c, err := l.Accept()
			if err != nil {
				log.Printf("Error accepting connection: %s", err.Error())

			}
			fmt.Printf("Serving %s\n", c.RemoteAddr().String())
			c.Close()
		}()
		s := <-s.sigChan
		switch s {
		case syscall.SIGTERM:
			log.Println("SIGTERM")
			l.Close()
			// TODO: Cleanup code
			return nil
		case syscall.SIGINT:
			log.Println("SIGINT")
			l.Close()
			// TODO: Cleanup code
			return nil
		}
		l.Close()
		log.Println("Stopped:", s)
		return nil
	}

}

func NewTCPServer(version, address string, s chan os.Signal) *TCPServer {
	return &TCPServer{
		address:    address,
		sigChan:    s,
		tcpVersion: version,
	}
}
