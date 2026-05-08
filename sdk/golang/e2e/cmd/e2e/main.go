// Binary e2e starts an in-process S3 server (gofakes3) and a mock gRPC
// controller, prints their addresses as JSON to stdout, and waits for
// SIGINT/SIGTERM to shut down.
//
// Other-language SDK tests run this binary and parse the first JSON line to
// obtain the endpoints.
//
// Usage:
//
//	# Build once
//	go build -o /tmp/e2e-server ./sdk/golang/e2e/cmd/e2e/
//
//	# Run
//	/tmp/e2e-server
//
//	# Or run directly
//	go run ./sdk/golang/e2e/cmd/e2e/
package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/open-bytestack/bytestack/sdk/golang/e2e"
)

// startupInfo is printed as the first JSON line on stdout.
type startupInfo struct {
	S3Endpoint     string `json:"s3_endpoint"`
	ControllerAddr string `json:"controller_addr"`
}

func main() {
	srv, err := e2e.Start(true)
	if err != nil {
		log.Fatalf("start e2e servers: %v", err)
	}

	if err := json.NewEncoder(os.Stdout).Encode(startupInfo{
		S3Endpoint:     srv.S3Endpoint(),
		ControllerAddr: srv.ControllerAddr(),
	}); err != nil {
		log.Fatalf("encode startup info: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	srv.Close()
}
