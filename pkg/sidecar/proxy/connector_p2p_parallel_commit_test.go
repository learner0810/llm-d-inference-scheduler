/*
Copyright 2026 The llm-d Authors.

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

package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive

	"github.com/llm-d/llm-d-router/pkg/common/routing"
)

type p2pCommitEnv struct {
	baseAddr    string
	prefillHost string
}

func startP2PCommitProxy(prefill, decode http.Handler, waitTimeout time.Duration) *p2pCommitEnv {
	ctx, cancelFn := context.WithCancel(newTestContext())
	stoppedCh := make(chan struct{})

	decodeBackend := httptest.NewServer(decode)
	DeferCleanup(decodeBackend.Close)
	prefillBackend := httptest.NewServer(prefill)
	DeferCleanup(prefillBackend.Close)

	decodeURL, err := url.Parse(decodeBackend.URL)
	Expect(err).ToNot(HaveOccurred())
	proxy := NewProxy(Config{
		Port:                 "0",
		DecoderURL:           decodeURL,
		KVConnector:          KVConnectorOffloading,
		P2PConnectorPort:     defaultP2PConnectorPort,
		P2PDecodeWaitTimeout: waitTimeout,
	})
	go func() {
		defer GinkgoRecover()
		proxy.allowlistValidator = &AllowlistValidator{enabled: false}
		Expect(proxy.Start(ctx)).To(Succeed())
		stoppedCh <- struct{}{}
	}()
	<-proxy.readyCh

	DeferCleanup(func() {
		cancelFn()
		<-stoppedCh
	})
	return &p2pCommitEnv{
		baseAddr:    "http://" + proxy.addr.String(),
		prefillHost: prefillBackend.URL[len("http://"):],
	}
}

func (env *p2pCommitEnv) send(clientTimeout time.Duration) (int, http.Header, string, error) {
	req, err := http.NewRequest(http.MethodPost, env.baseAddr+ChatCompletionsPath,
		strings.NewReader(chatCompletionsRequestBody))
	Expect(err).ToNot(HaveOccurred())
	req.Header.Add(routing.PrefillEndpointHeader, env.prefillHost)

	rp, err := (&http.Client{Timeout: clientTimeout}).Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer rp.Body.Close()
	body, _ := io.ReadAll(rp.Body) //nolint:errcheck
	return rp.StatusCode, rp.Header, string(body), nil
}

var _ = Describe("P2P Connector concurrent dispatch commit point", func() {
	It("returns the prefill error and discards decode output", func() {
		prefill := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"prefill boom"}`))
		})
		decode := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":"decode-should-be-discarded"}`))
		})
		env := startP2PCommitProxy(prefill, decode, time.Second)

		status, _, body, err := env.send(5 * time.Second)
		Expect(err).ToNot(HaveOccurred())
		Expect(status).To(Equal(http.StatusInternalServerError))
		Expect(body).To(ContainSubstring("prefill boom"))
		Expect(body).ToNot(ContainSubstring("decode-should-be-discarded"))
	})

	It("returns 504 when both upstream requests block", func() {
		stop := make(chan struct{})
		block := func(_ http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-stop:
			}
		}
		env := startP2PCommitProxy(http.HandlerFunc(block), http.HandlerFunc(block), 100*time.Millisecond)
		DeferCleanup(func() { close(stop) })

		start := time.Now()
		status, _, _, err := env.send(5 * time.Second)
		Expect(err).ToNot(HaveOccurred())
		Expect(status).To(Equal(http.StatusGatewayTimeout))
		Expect(time.Since(start)).To(BeNumerically("<", time.Second))
	})

	It("commits the decode response after prefill succeeds", func() {
		prefill := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		})
		decode := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", eventStreamContentType)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("data: {\"choices\":[]}\n\ndata: [DONE]\n\n"))
		})
		env := startP2PCommitProxy(prefill, decode, time.Second)

		status, header, body, err := env.send(5 * time.Second)
		Expect(err).ToNot(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		Expect(header.Get("Content-Type")).To(ContainSubstring(eventStreamContentType))
		Expect(body).To(ContainSubstring("[DONE]"))
	})
})
