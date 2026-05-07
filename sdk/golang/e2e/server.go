// Package e2e provides an in-process S3 test server (backed by gofakes3)
// and an optional mock gRPC controller for Bytestack SDK integration tests.
//
// The binary at sdk/golang/e2e/cmd/e2e/ starts both servers and prints their
// addresses as JSON, allowing cross-language SDK tests to reuse the same
// setup.
package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	controllerpb "github.com/open-bytestack/bytestack/proto/src/controller"
)

// mockControllerServer returns sequential stack IDs starting from 1.
type mockControllerServer struct {
	controllerpb.UnimplementedControllerServer
	nextStackID uint64
}

func (s *mockControllerServer) NextStackID(_ context.Context, _ *emptypb.Empty) (*controllerpb.StackID, error) {
	id := s.nextStackID
	s.nextStackID++
	return &controllerpb.StackID{StackId: id}, nil
}

// Server bundles an in-process gofakes3 S3 server and an optional mock
// gRPC controller.
type Server struct {
	s3Endpoint     string
	controllerAddr string
	s3Lis          net.Listener
	s3Srv          *http.Server
	grpcLis        net.Listener
	grpcSrv        *grpc.Server
}

// Start creates and starts both servers on random ports.
// If withController is false, only the S3 server starts.
func Start(withController bool) (*Server, error) {
	s := &Server{}

	// --- S3 server (gofakes3) ------------------------------------------------
	backend := s3mem.New()
	faker := gofakes3.New(backend)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("s3 listen: %w", err)
	}
	s.s3Lis = lis
	s.s3Endpoint = fmt.Sprintf("http://%s", lis.Addr().String())
	s.s3Srv = &http.Server{Handler: faker.Server()}
	go s.s3Srv.Serve(lis) //nolint: errcheck

	// --- Mock gRPC controller (optional) -------------------------------------
	if withController {
		glis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			s.Close()
			return nil, fmt.Errorf("grpc listen: %w", err)
		}
		s.grpcLis = glis
		s.controllerAddr = glis.Addr().String()

		gsrv := grpc.NewServer()
		s.grpcSrv = gsrv
		controllerpb.RegisterControllerServer(gsrv, &mockControllerServer{nextStackID: 1})
		go gsrv.Serve(glis) //nolint: errcheck
	}

	return s, nil
}

// S3Endpoint returns the S3 server URL (e.g. "http://127.0.0.1:54321").
func (s *Server) S3Endpoint() string { return s.s3Endpoint }

// ControllerAddr returns the gRPC controller address (e.g. "127.0.0.1:54322"),
// or "" if the controller was not started.
func (s *Server) ControllerAddr() string { return s.controllerAddr }

// Close shuts down both servers.
func (s *Server) Close() error {
	if s.grpcSrv != nil {
		s.grpcSrv.Stop()
	}
	if s.grpcLis != nil {
		s.grpcLis.Close()
	}
	if s.s3Srv != nil {
		return s.s3Srv.Close()
	}
	return nil
}
