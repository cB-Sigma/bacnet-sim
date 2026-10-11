// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

// syncBuf is a goroutine-safe output buffer.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }

func TestRunServesDemoObjects(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan *sim.Device, 1)
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-listen", "127.0.0.1:0", "-instance", "77", "-report", "10ms"}, &syncBuf{}, func(s *sim.Device) { got <- s })
	}()
	var s *sim.Device
	select {
	case s = <-got:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(5 * time.Second):
		t.Fatal("not started")
	}
	c, err := bacnet.NewClient("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	go c.Serve()
	addr, _ := net.ResolveUDPAddr("udp", s.Addr)
	tg := bacnet.Target{Dest: bacnet.Dest{Addr: addr}, Peer: bacnet.Peer{MaxAPDU: 1476}, Timeout: time.Second, Retries: 2}
	v, err := c.ReadProperty(ctx, tg, Mode, bacnet.PropPresentValue)
	if err != nil || len(v) != 1 || v[0] != uint64(2) {
		t.Fatalf("mode = %v, %v", v, err)
	}
	if err := c.WriteProperty(ctx, tg, Set, bacnet.PropPresentValue, float32(22.5), 8); err != nil {
		t.Fatal(err)
	}
	v, err = c.ReadProperty(ctx, tg, Set, bacnet.PropPresentValue)
	if err != nil || len(v) != 1 || v[0] != float32(22.5) {
		t.Fatalf("set point = %v, %v", v, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsBadInstance(t *testing.T) {
	if err := run(context.Background(), []string{"-instance", "4194303"}, &syncBuf{}, nil); err == nil {
		t.Fatal("accepted")
	}
}
