/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runnable

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// GRPCServer converts the given gRPC server into a runnable.
// The server name is just being used for logging.
func GRPCServer(name string, srv *grpc.Server, port int) manager.Runnable {
	return GRPCServerWithGracefulShutdownTimeout(name, srv, port, 0)
}

// GRPCServerWithGracefulShutdownTimeout converts the given gRPC server into a runnable.
// If gracefulShutdownTimeout is greater than zero, shutdown falls back to Stop after
// the timeout expires.
func GRPCServerWithGracefulShutdownTimeout(name string, srv *grpc.Server, port int, gracefulShutdownTimeout time.Duration) manager.Runnable {
	return manager.RunnableFunc(func(ctx context.Context) error {
		// Use "name" key as that is what manager.Server does as well.
		log := ctrl.Log.WithValues("name", name)
		log.Info("gRPC server starting")

		// Start listening.
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return fmt.Errorf("gRPC server failed to listen - %w", err)
		}

		log.Info("gRPC server listening", "port", port)

		// Shutdown on context closed. Make sure the goroutine does not leak.
		doneCh := make(chan struct{})
		defer close(doneCh)
		go func() {
			select {
			case <-ctx.Done():
				log.Info("gRPC server shutting down")
				if gracefulShutdownTimeout <= 0 {
					srv.GracefulStop()
					return
				}

				gracefulStopCh := make(chan struct{})
				go func() {
					srv.GracefulStop()
					close(gracefulStopCh)
				}()

				timer := time.NewTimer(gracefulShutdownTimeout)
				defer timer.Stop()

				select {
				case <-gracefulStopCh:
				case <-timer.C:
					log.Info("gRPC graceful shutdown timeout exceeded, forcing stop", "timeout", gracefulShutdownTimeout)
					srv.Stop()
				}
			case <-doneCh:
			}
		}()

		// Keep serving until terminated.
		if err := srv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			return fmt.Errorf("gRPC server failed - %w", err)
		}
		log.Info("gRPC server terminated")
		return nil
	})
}
