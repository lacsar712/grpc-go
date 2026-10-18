/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package test

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/internal/balancer/stub"
	"google.golang.org/grpc/internal/stubserver"
	"google.golang.org/grpc/internal/testutils"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	testgrpc "google.golang.org/grpc/interop/grpc_testing"
	testpb "google.golang.org/grpc/interop/grpc_testing"
)

const pinTestBalancerName = "test_subchannel_pin_balancer"

// pinBalancerState is shared between the test balancer and the pickers it
// produces. It models either a round-robin policy over ready subconns, or a
// policy that forces every pick to one particular subconn.
type pinBalancerState struct {
	mu     sync.Mutex
	subs   []balancer.SubConn
	ready  []bool
	chosen int // index of the subconn every pick is forced to; -1 for round-robin
	nextRR int
}

type pinBalancerPicker struct {
	st *pinBalancerState
}

func (p *pinBalancerPicker) Pick(balancer.PickInfo) (balancer.PickResult, error) {
	st := p.st
	st.mu.Lock()
	defer st.mu.Unlock()

	if st.chosen >= 0 {
		if st.chosen < len(st.subs) && st.ready[st.chosen] {
			return balancer.PickResult{SubConn: st.subs[st.chosen]}, nil
		}
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	for n := 0; n < len(st.subs); n++ {
		i := (st.nextRR + n) % len(st.subs)
		if st.ready[i] {
			st.nextRR = i + 1
			return balancer.PickResult{SubConn: st.subs[i]}, nil
		}
	}
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

func (st *pinBalancerState) connectivityAndPickerLocked() (connectivity.State, balancer.Picker) {
	state := connectivity.Connecting
	for _, ready := range st.ready {
		if ready {
			state = connectivity.Ready
			break
		}
	}
	return state, &pinBalancerPicker{st: st}
}

func (st *pinBalancerState) setChosen(i int) {
	st.mu.Lock()
	st.chosen = i
	st.mu.Unlock()
}

func (st *pinBalancerState) shutdownSubConn(i int) {
	st.mu.Lock()
	sc := st.subs[i]
	st.mu.Unlock()
	sc.Shutdown()
}

// pinBalancerStates stores one pinBalancerState per ClientConn target, so that
// tests can drive the balancer after the channel is created.
var pinBalancerStates sync.Map

// pinBalancerData maps each balancer instance (keyed by its BalancerData
// pointer) to its state. The stub balancer API version used here has no Data
// field on BalancerData.
var (
	pinBalancerDataMu sync.Mutex
	pinBalancerData   = map[*stub.BalancerData]*pinBalancerState{}
)

func init() {
	stub.Register(pinTestBalancerName, stub.BalancerFuncs{
		Init: func(bd *stub.BalancerData) {
			st := &pinBalancerState{chosen: -1}
			pinBalancerDataMu.Lock()
			pinBalancerData[bd] = st
			pinBalancerDataMu.Unlock()
			pinBalancerStates.Store(bd.BuildOptions.Target.String(), st)
		},
		Close: func(bd *stub.BalancerData) {
			pinBalancerStates.Delete(bd.BuildOptions.Target.String())
			pinBalancerDataMu.Lock()
			delete(pinBalancerData, bd)
			pinBalancerDataMu.Unlock()
		},
		UpdateClientConnState: func(bd *stub.BalancerData, ccs balancer.ClientConnState) error {
			pinBalancerDataMu.Lock()
			st := pinBalancerData[bd]
			pinBalancerDataMu.Unlock()
			st.mu.Lock()
			defer st.mu.Unlock()

			if len(st.subs) != 0 {
				// Tests never change the address set.
				return nil
			}
			addrs := ccs.ResolverState.Addresses
			st.subs = make([]balancer.SubConn, len(addrs))
			st.ready = make([]bool, len(addrs))
			for i, addr := range addrs {
				i := i
				var sc balancer.SubConn
				listener := func(scs balancer.SubConnState) {
					st.mu.Lock()
					st.ready[i] = scs.ConnectivityState == connectivity.Ready
					connect := scs.ConnectivityState == connectivity.Idle
					state, picker := st.connectivityAndPickerLocked()
					st.mu.Unlock()

					// Re-request a connection whenever the subconn goes
					// idle (e.g. after a GoAway or connection loss), so
					// the same subchannel reconnects on its own.
					if connect {
						sc.Connect()
					}
					bd.ClientConn.UpdateState(balancer.State{
						ConnectivityState: state,
						Picker:            picker,
					})
				}
				var err error
				sc, err = bd.ClientConn.NewSubConn([]resolver.Address{addr}, balancer.NewSubConnOptions{
					StateListener: listener,
				})
				if err != nil {
					return err
				}
				st.subs[i] = sc
			}
			for _, sc := range st.subs {
				sc.Connect()
			}
			state, picker := st.connectivityAndPickerLocked()
			bd.ClientConn.UpdateState(balancer.State{
				ConnectivityState: state,
				Picker:            picker,
			})
			return nil
		},
	})
}

func pinBalancerStateForTarget(t *testing.T, target string) *pinBalancerState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := pinBalancerStates.Load(target); ok {
			return v.(*pinBalancerState)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("balancer state for target %q was not registered", target)
	return nil
}

func startUnavailableBackend(t *testing.T, calls *atomic.Int32) *stubserver.StubServer {
	t.Helper()
	ss := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			calls.Add(1)
			return nil, status.Error(codes.Unavailable, "retryable error")
		},
		FullDuplexCallF: func(_ testgrpc.TestService_FullDuplexCallServer) error {
			calls.Add(1)
			return status.Error(codes.Unavailable, "retryable error")
		},
	}
	if err := ss.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(ss.Stop)
	return ss
}

func pinRetryServiceConfig(t *testing.T, balancerCfg string, waitForReady bool) string {
	t.Helper()
	wfr := ""
	if waitForReady {
		wfr = `"waitForReady": true,`
	}
	return `{
		"loadBalancingConfig": [` + balancerCfg + `],
		"methodConfig": [{
			"name": [{"service": "grpc.testing.TestService"}],
			` + wfr + `
			"retryPolicy": {
				"maxAttempts": 3,
				"initialBackoff": "0.01s",
				"maxBackoff": "0.01s",
				"backoffMultiplier": 1.0,
				"retryableStatusCodes": ["UNAVAILABLE"]
			}
		}]
	}`
}

func dialPinTestClient(t *testing.T, target string, r *manual.Resolver, addrs []resolver.Address, scJSON string, extra ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(scJSON),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.0, MaxDelay: 200 * time.Millisecond},
		}),
	}, extra...)
	cc, err := grpc.NewClient(target, opts...)
	if err != nil {
		t.Fatalf("grpc.NewClient(): %v", err)
	}
	t.Cleanup(func() { cc.Close() })
	cc.Connect()
	r.UpdateState(resolver.State{Addresses: addrs})
	return cc
}

func await(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(defaultTestTimeout):
		t.Fatal("timed out waiting for condition")
	}
}

// TestPinSubchannel_RetriesStayOnSameSubConn verifies that, with
// PinSubchannel, every retry of a call is sent to the subchannel selected by
// the first attempt, even though a round-robin picker would otherwise rotate
// the subchannel. Both unary and streaming calls are covered.
func (s) TestPinSubchannel_RetriesStayOnSameSubConn(t *testing.T) {
	for _, rpcType := range []string{"unary", "streaming"} {
		t.Run(rpcType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
			defer cancel()

			var calls [2]atomic.Int32
			backends := []*stubserver.StubServer{
				startUnavailableBackend(t, &calls[0]),
				startUnavailableBackend(t, &calls[1]),
			}
			addrs := []resolver.Address{
				{Addr: backends[0].Address},
				{Addr: backends[1].Address},
			}

			r := manual.NewBuilderWithScheme("whatever")
			scJSON := pinRetryServiceConfig(t, `{"round_robin": {}}`, false)
			cc := dialPinTestClient(t, "whatever:///pin-retries-"+rpcType, r, addrs, scJSON)
			testutils.AwaitState(ctx, t, cc, connectivity.Ready)
			tc := testgrpc.NewTestServiceClient(cc)

			// Pinned call: three attempts, all served by whichever backend
			// the first pick selected.
			if rpcType == "unary" {
				_, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel())
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("EmptyCall() err = %v, want Unavailable", err)
				}
			} else {
				stream, err := tc.FullDuplexCall(ctx, grpc.PinSubchannel())
				if err != nil {
					t.Fatalf("FullDuplexCall() = %v", err)
				}
				if err := stream.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte{1}}}); err != nil {
					t.Fatalf("Send(): %v", err)
				}
				stream.CloseSend()
				if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
					t.Fatalf("Recv() err = %v, want Unavailable", err)
				}
			}

			c0, c1 := calls[0].Load(), calls[1].Load()
			if c0+c1 != 3 {
				t.Fatalf("pinned call produced %d server attempts, want 3", c0+c1)
			}
			if (c0 != 3 || c1 != 0) && (c1 != 3 || c0 != 0) {
				t.Fatalf("pinned call attempts = [%d, %d], want all on one backend", c0, c1)
			}

			// Control: without the option, retries rotate across backends.
			if rpcType == "unary" {
				_, err := tc.EmptyCall(ctx, &testpb.Empty{})
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("EmptyCall() err = %v, want Unavailable", err)
				}
			} else {
				stream, err := tc.FullDuplexCall(ctx)
				if err != nil {
					t.Fatalf("FullDuplexCall() = %v", err)
				}
				if err := stream.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte{1}}}); err != nil {
					t.Fatalf("Send(): %v", err)
				}
				stream.CloseSend()
				if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
					t.Fatalf("Recv() err = %v, want Unavailable", err)
				}
			}
			d0, d1 := calls[0].Load()-c0, calls[1].Load()-c1
			if d0+d1 != 3 {
				t.Fatalf("control call produced %d server attempts, want 3", d0+d1)
			}
			if d0 == 0 || d1 == 0 {
				t.Fatalf("control call attempts = [%d, %d], want retries spread across backends", d0, d1)
			}
		})
	}
}

// TestPinSubchannel_BindingReleasedBetweenCalls verifies that a binding does
// not leak into later, unrelated RPCs: sequential pinned calls each pick
// normally and get round-robined across the two backends.
func (s) TestPinSubchannel_BindingReleasedBetweenCalls(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	var calls [2]atomic.Int32
	backends := make([]*stubserver.StubServer, 2)
	for i := range backends {
		ss := &stubserver.StubServer{
			EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
				calls[i].Add(1)
				return &testpb.Empty{}, nil
			},
		}
		if err := ss.StartServer(); err != nil {
			t.Fatalf("StartServer(): %v", err)
		}
		t.Cleanup(ss.Stop)
		backends[i] = ss
	}
	addrs := []resolver.Address{{Addr: backends[0].Address}, {Addr: backends[1].Address}}

	r := manual.NewBuilderWithScheme("whatever")
	cc := dialPinTestClient(t, "whatever:///pin-release", r, addrs, `{"loadBalancingConfig": [{"round_robin": {}}]}`)
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	tc := testgrpc.NewTestServiceClient(cc)

	for i := 0; i < 4; i++ {
		if _, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel()); err != nil {
			t.Fatalf("EmptyCall #%d: %v", i, err)
		}
	}
	if c0, c1 := calls[0].Load(), calls[1].Load(); c0 != 2 || c1 != 2 {
		t.Fatalf("attempts per backend = [%d, %d], want [2, 2]", c0, c1)
	}
}

// TestPinSubchannel_FailFastWhenPinnedSubConnUnavailable verifies that a
// pinned fail-fast call fails instead of migrating when its subchannel goes
// away after a GoAway, while another healthy subchannel exists.
func (s) TestPinSubchannel_FailFastWhenPinnedSubConnUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	var calls [2]atomic.Int32
	be0 := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			calls[0].Add(1)
			return nil, status.Error(codes.Unavailable, "retryable error")
		},
	}
	if err := be0.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be0.Stop)
	be1 := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			calls[1].Add(1)
			return &testpb.Empty{}, nil
		},
	}
	if err := be1.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be1.Stop)

	target := "whatever:///pin-failfast"
	r := manual.NewBuilderWithScheme("whatever")
	// Long backoff so the first retry is still pending when the pinned
	// subchannel receives the GoAway.
	scJSON := `{
		"loadBalancingConfig": [{"` + pinTestBalancerName + `": {}}],
		"methodConfig": [{
			"name": [{"service": "grpc.testing.TestService"}],
			"retryPolicy": {
				"maxAttempts": 3,
				"initialBackoff": "0.5s",
				"maxBackoff": "0.5s",
				"backoffMultiplier": 1.0,
				"retryableStatusCodes": ["UNAVAILABLE"]
			}
		}]
	}`
	addrs := []resolver.Address{{Addr: be0.Address}, {Addr: be1.Address}}
	cc := dialPinTestClient(t, target, r, addrs, scJSON)
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	tc := testgrpc.NewTestServiceClient(cc)

	st := pinBalancerStateForTarget(t, target)
	st.setChosen(0)

	errCh := make(chan error, 1)
	go func() {
		_, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel())
		errCh <- err
	}()

	// Wait for the first attempt to land on backend 0, then take that
	// subchannel down with a GoAway.
	deadline := time.Now().Add(defaultTestTimeout)
	for calls[0].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls[0].Load() == 0 {
		t.Fatal("first attempt never reached the pinned backend")
	}
	be0.S.GracefulStop()

	select {
	case err := <-errCh:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("pinned call err = %v, want Unavailable", err)
		}
	case <-time.After(defaultTestTimeout):
		t.Fatal("pinned call did not fail after the pinned subchannel went away")
	}

	// The healthy second backend must never have served any attempt.
	if c1 := calls[1].Load(); c1 != 0 {
		t.Fatalf("healthy other backend served %d attempts of the pinned call", c1)
	}
	// Retries never created a stream: the subchannel was not ready.
	if c0 := calls[0].Load(); c0 != 1 {
		t.Fatalf("pinned backend served %d attempts, want 1 (no retries reached it)", c0)
	}

	// A non-pinned call is free to use the other healthy backend.
	st.setChosen(-1)
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}); err != nil {
		t.Fatalf("control EmptyCall() = %v, want nil", err)
	}
	if c1 := calls[1].Load(); c1 != 1 {
		t.Fatalf("other backend served %d control calls, want 1", c1)
	}
}

// TestPinSubchannel_WaitForReadyStaysOnReconnectingSubConn verifies that a
// pinned wait-for-ready call whose subchannel drops keeps waiting for the
// same subchannel, never uses another ready one, and completes once the
// pinned subchannel reconnects (at the same address).
func (s) TestPinSubchannel_WaitForReadyStaysOnReconnectingSubConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	// Own the listener so a new server can re-listen on the same address.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(): %v", err)
	}
	pinnedAddr := lis.Addr().String()

	entered := make(chan struct{})
	var oldBackendCalls atomic.Int32
	be0 := &stubserver.StubServer{
		Listener: lis,
		EmptyCallF: func(reqCtx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			oldBackendCalls.Add(1)
			close(entered)
			<-reqCtx.Done() // aborted when the server is hard-stopped
			return nil, status.Error(codes.Unavailable, "backend stopped mid-call")
		},
	}
	if err := be0.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be0.Stop)

	var otherBackendCalls atomic.Int32
	be1 := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			otherBackendCalls.Add(1)
			return &testpb.Empty{}, nil
		},
	}
	if err := be1.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be1.Stop)

	target := "whatever:///pin-wfr"
	r := manual.NewBuilderWithScheme("whatever")
	// The hard-stop abort is retried through the retry policy; the pinned
	// wait-for-ready attempt then blocks on this subchannel while it
	// reconnects.
	scJSON := pinRetryServiceConfig(t, `{"`+pinTestBalancerName+`": {}}`, true)
	addrs := []resolver.Address{{Addr: pinnedAddr}, {Addr: be1.Address}}
	cc := dialPinTestClient(t, target, r, addrs, scJSON)
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	tc := testgrpc.NewTestServiceClient(cc)

	st := pinBalancerStateForTarget(t, target)
	st.setChosen(0)

	errCh := make(chan error, 1)
	go func() {
		_, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel())
		errCh <- err
	}()
	await(t, entered)

	// Hard-stop the pinned backend: the in-flight attempt fails, and the
	// pinned subchannel starts reconnecting.
	be0.S.Stop()

	// The call must stay blocked (waiting on the pinned subchannel) and must
	// not migrate to the other healthy backend.
	select {
	case err := <-errCh:
		t.Fatalf("pinned WFR call finished unexpectedly while subchannel reconnects: %v", err)
	case <-time.After(400 * time.Millisecond):
	}
	if c := otherBackendCalls.Load(); c != 0 {
		t.Fatalf("other backend served %d attempts while pinned subchannel was down", c)
	}

	// Restart a backend on the same address; the pinned subchannel reconnects
	// and the transparent retry completes there.
	var newBackendCalls atomic.Int32
	lis2, lerr := listenWithRetry(t, pinnedAddr)
	if lerr != nil {
		t.Fatalf("re-listen on %q: %v", pinnedAddr, lerr)
	}
	be0b := &stubserver.StubServer{
		Listener: lis2,
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			newBackendCalls.Add(1)
			return &testpb.Empty{}, nil
		},
	}
	if err := be0b.StartServer(); err != nil {
		t.Fatalf("restart StartServer(): %v", err)
	}
	t.Cleanup(be0b.Stop)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pinned WFR call = %v, want nil after reconnect", err)
		}
	case <-time.After(defaultTestTimeout):
		t.Fatal("pinned WFR call did not complete after subchannel reconnect")
	}
	if c := otherBackendCalls.Load(); c != 0 {
		t.Fatalf("other backend served %d attempts of the pinned call", c)
	}
	if c := newBackendCalls.Load(); c != 1 {
		t.Fatalf("restarted pinned backend served %d attempts, want 1", c)
	}
}

// TestPinSubchannel_ShutdownFailsWaitForReady verifies that a wait-for-ready
// call pinned to a subchannel that is shut down (removed) fails immediately
// instead of waiting or moving to another ready subchannel.
func (s) TestPinSubchannel_ShutdownFailsWaitForReady(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	entered := make(chan struct{})
	var otherBackendCalls atomic.Int32
	be0 := &stubserver.StubServer{
		EmptyCallF: func(reqCtx context.Context, _ *testpb.Empty) (*testpb.Empty, error) {
			close(entered)
			// Give the test time to shut down the subchannel while this
			// attempt is in flight. The server will keep the stream until
			// then; the GoAway starts draining, and the failed retry below
			// happens against the shut-down subchannel.
			time.Sleep(300 * time.Millisecond)
			return nil, status.Error(codes.Unavailable, "retryable error")
		},
	}
	if err := be0.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be0.Stop)
	be1 := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			otherBackendCalls.Add(1)
			return &testpb.Empty{}, nil
		},
	}
	if err := be1.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be1.Stop)

	target := "whatever:///pin-shutdown"
	r := manual.NewBuilderWithScheme("whatever")
	scJSON := pinRetryServiceConfig(t, `{"`+pinTestBalancerName+`": {}}`, true)
	addrs := []resolver.Address{{Addr: be0.Address}, {Addr: be1.Address}}
	cc := dialPinTestClient(t, target, r, addrs, scJSON)
	testutils.AwaitState(ctx, t, cc, connectivity.Ready)
	tc := testgrpc.NewTestServiceClient(cc)

	st := pinBalancerStateForTarget(t, target)
	st.setChosen(0)

	errCh := make(chan error, 1)
	go func() {
		_, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel())
		errCh <- err
	}()
	await(t, entered)

	// Remove the pinned subchannel entirely.
	st.shutdownSubConn(0)

	start := time.Now()
	select {
	case err := <-errCh:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("pinned call err = %v, want Unavailable", err)
		}
		if !containsShutDownMessage(err) {
			t.Fatalf("pinned call err = %v, want it to report the pinned SubConn as shut down", err)
		}
	case <-time.After(defaultTestTimeout):
		t.Fatal("pinned WFR call did not fail after subchannel shutdown")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("pinned call took %v after subchannel shutdown, want immediate failure", d)
	}
	if c := otherBackendCalls.Load(); c != 0 {
		t.Fatalf("other backend served %d attempts of a call pinned to a shut-down subchannel", c)
	}
}

// TestPinSubchannel_FailedFirstPickLeavesNoBinding verifies that if the first
// pick never yields a subchannel, no binding is created: a later call with the
// option picks normally.
func (s) TestPinSubchannel_FailedFirstPickLeavesNoBinding(t *testing.T) {
	r := manual.NewBuilderWithScheme("whatever")
	target := "whatever:///pin-empty"
	be1 := &stubserver.StubServer{
		EmptyCallF: func(context.Context, *testpb.Empty) (*testpb.Empty, error) {
			return &testpb.Empty{}, nil
		},
	}
	if err := be1.StartServer(); err != nil {
		t.Fatalf("StartServer(): %v", err)
	}
	t.Cleanup(be1.Stop)
	addrs := []resolver.Address{{Addr: "127.0.0.1:1"}, {Addr: be1.Address}}

	scJSON := `{"loadBalancingConfig": [{"` + pinTestBalancerName + `": {}}]}`
	cc := dialPinTestClient(t, target, r, addrs, scJSON)
	tc := testgrpc.NewTestServiceClient(cc)
	st := pinBalancerStateForTarget(t, target)
	st.setChosen(0) // forced to a backend that is never started

	sCtx, sCancel := context.WithTimeout(context.Background(), defaultTestShortTimeout)
	defer sCancel()
	if _, err := tc.EmptyCall(sCtx, &testpb.Empty{}, grpc.PinSubchannel()); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("EmptyCall() err = %v, want DeadlineExceeded", err)
	}

	// A later call, with the forced choice removed, picks the ready backend.
	st.setChosen(-1)
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()
	if _, err := tc.EmptyCall(ctx, &testpb.Empty{}, grpc.PinSubchannel()); err != nil {
		t.Fatalf("EmptyCall() after failed first pick = %v, want nil", err)
	}
}

// TestPinSubchannel_IdleReapingDoesNotCloseBoundSubConn verifies that while a
// pinned call is in flight, channel idleness cannot reap its subchannel: the
// stream keeps working and stays on the same transport long past the idle
// timeout.
func (s) TestPinSubchannel_IdleReapingDoesNotCloseBoundSubConn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	ss := &stubserver.StubServer{
		FullDuplexCallF: func(stream testgrpc.TestService_FullDuplexCallServer) error {
			for {
				in, err := stream.Recv()
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err := stream.Send(&testpb.StreamingOutputCallResponse{Payload: in.Payload}); err != nil {
					return err
				}
			}
		},
	}
	if err := ss.Start([]grpc.ServerOption{},
		grpc.WithIdleTimeout(100*time.Millisecond),
	); err != nil {
		t.Fatalf("Error starting endpoint server: %v", err)
	}
	defer ss.Stop()

	stream, err := ss.Client.FullDuplexCall(ctx, grpc.PinSubchannel())
	if err != nil {
		t.Fatalf("FullDuplexCall(): %v", err)
	}

	roundTrip := func() string {
		if err := stream.Send(&testpb.StreamingOutputCallRequest{Payload: &testpb.Payload{Body: []byte{1}}}); err != nil {
			t.Fatalf("Send(): %v", err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("Recv(): %v", err)
		}
		pr, ok := peer.FromContext(stream.Context())
		if !ok {
			t.Fatal("no peer in stream context")
		}
		return pr.Addr.String()
	}

	addr1 := roundTrip()

	// Wait many idle-timeout intervals with the pinned call still open.
	time.Sleep(600 * time.Millisecond)
	if state := ss.CC.GetState(); state != connectivity.Ready {
		t.Fatalf("channel state = %v, want Ready (idle reaping occurred)", state)
	}
	if addr2 := roundTrip(); addr2 != addr1 {
		t.Fatalf("pinned call migrated from %q to %q while idle reaping ran", addr1, addr2)
	}
}

func listenWithRetry(t *testing.T, addr string) (net.Listener, error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		l, err := net.Listen("tcp", addr)
		if err == nil {
			return l, nil
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	return nil, lastErr
}

func containsShutDownMessage(err error) bool {
	return strings.Contains(status.Convert(err).Message(), "shut down")
}
