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

package server

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/pflag"
)

// TestEndpointTargetPorts
func TestEndpointTargetPorts(t *testing.T) {
	tests := []struct {
		name          string
		fs            *pflag.FlagSet
		args          []string
		expectError   bool // expect validation error
		expectedPorts []int
	}{
		{
			name: "Valid multiple flags order check",
			args: []string{
				"--endpoint-target-ports", "8080",
				"--endpoint-target-ports", "9090",
				"--endpoint-target-ports", "80",
			},
			expectError:   false,
			expectedPorts: []int{8080, 9090, 80},
		},
		{
			name: "Valid comma separated list",
			args: []string{
				"--endpoint-target-ports", "8080,9090,80",
			},
			expectError:   false,
			expectedPorts: []int{8080, 9090, 80},
		},
		{
			name: "Handle duplicates order preservation",
			args: []string{
				"--endpoint-target-ports", "8080",
				"--endpoint-target-ports", "9090",
				"--endpoint-target-ports", "8080",
				"--endpoint-target-ports", "9090",
			},
			expectError:   false,
			expectedPorts: []int{8080, 9090},
		},
		{
			name: "Invalid negative port number",
			args: []string{
				"--endpoint-target-ports", "8080",
				"--endpoint-target-ports", "-1",
			},
			expectError:   true,
			expectedPorts: []int{8080, -1},
		},
		{
			name: "Invalid over max port range",
			args: []string{
				"--endpoint-target-ports", "65536",
			},
			expectError:   true,
			expectedPorts: []int{65536},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fs = pflag.NewFlagSet(tt.name, pflag.ContinueOnError)

			opts := NewOptions()
			opts.AddFlags(tt.fs)

			argv := make([]string, 0, 4+len(tt.args))
			argv = append(argv, "--endpoint-selector", "app=vllm", "--config-file", "fake-config.yaml") // avoid an options validation error
			argv = append(argv, tt.args...)

			if err := tt.fs.Parse(argv); err != nil {
				t.Fatalf("Failed to parse flags: %v", err)
			}

			if err := opts.Complete(); err != nil {
				if !tt.expectError {
					t.Fatalf("Complete failed unexpectedly with error: %v", err)
				}
				return
			}

			err := opts.Validate()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected a validation error but got none.")
				}
				return
			}

			if err != nil {
				t.Fatalf("Validate failed unexpectedly with error: %v", err)
			}

			if diff := cmp.Diff(tt.expectedPorts, opts.EndpointTargetPorts); diff != "" {
				t.Errorf("Resulting ports mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGRPCGracefulShutdownTimeout(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		want        time.Duration
		expectError bool
	}{
		{
			name: "default waits indefinitely",
			want: 0,
		},
		{
			name: "valid duration",
			args: []string{"--grpc-graceful-shutdown-timeout", "30s"},
			want: 30 * time.Second,
		},
		{
			name:        "negative duration",
			args:        []string{"--grpc-graceful-shutdown-timeout=-1s"},
			want:        -1 * time.Second,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := pflag.NewFlagSet(tt.name, pflag.ContinueOnError)
			opts := NewOptions()
			opts.AddFlags(fs)

			argv := make([]string, 0, 4+len(tt.args))
			argv = append(argv, "--endpoint-selector", "app=vllm", "--config-file", "fake-config.yaml", "--endpoint-target-ports", "8080")
			argv = append(argv, tt.args...)

			if err := fs.Parse(argv); err != nil {
				t.Fatalf("Failed to parse flags: %v", err)
			}
			if err := opts.Complete(); err != nil {
				t.Fatalf("Complete failed unexpectedly with error: %v", err)
			}

			err := opts.Validate()
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected a validation error but got none.")
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate failed unexpectedly with error: %v", err)
			}

			if opts.GRPCGracefulShutdownTimeout != tt.want {
				t.Fatalf("GRPCGracefulShutdownTimeout = %v, want %v", opts.GRPCGracefulShutdownTimeout, tt.want)
			}
		})
	}
}
