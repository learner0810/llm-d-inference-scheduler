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

package requestcontrol

import (
	"context"
	"errors"
	"fmt"
	"time"

	fwkrc "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

// executePluginsAsDAG executes DataProducer plugins as a DAG based on their dependencies asynchronously.
// So, a plugin is executed only after all its dependencies have been executed.
// If there is a cycle or any plugin fails with error, it returns an error.
func executePluginsAsDAG(ctx context.Context, plugins []fwkrc.DataProducer, request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	for _, plugin := range plugins {
		if err := plugin.Produce(ctx, request, endpoints); err != nil {
			return fmt.Errorf("DataProducer %q failed: %w", plugin.TypedName().String(), err)
		}
	}
	return nil
}

// dataProducerPluginsWithTimeout executes DataProducer plugins with a timeout.
// The timeout is cooperative: plugins receive a child context with a deadline,
// and the director waits for them to return before moving on with shared
// request-scoped objects.
func dataProducerPluginsWithTimeout(ctx context.Context, timeout time.Duration, plugins []fwkrc.DataProducer,
	request *fwksched.InferenceRequest, endpoints []fwksched.Endpoint) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	err := executePluginsAsDAG(ctx, plugins, request, endpoints)
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf("DataProducer execution timed out: %w", ctx.Err())
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}
