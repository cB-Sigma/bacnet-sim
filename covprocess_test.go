// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package bacnet_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

// A COV notification carries the subscriber's process identifier, its
// lifetime and the device's own instance. The simulator used to send
// process 1, 60 seconds and device 1234 whatever was asked; bacnet-stack's
// bacscov showed the mismatch.
func TestSimCOVNotificationEchoesSubscription(t *testing.T) {
	d := sim.New()
	d.Iam.Device.Instance = 4321
	ai := bacnet.ObjectID{Type: bacnet.AnalogInput, Instance: 1}
	d.Add(ai, &sim.Object{Pv: float32(1)})
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	defer d.Stop()

	c, err := bacnet.NewClient("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got := make(chan bacnet.APDU, 4)
	c.OnUnconfirmed = func(_ bacnet.Dest, a bacnet.APDU) {
		if a.Service == bacnet.SvcUnconfirmedCOV {
			got <- a
		}
	}
	go c.Serve()
	addr, _ := net.ResolveUDPAddr("udp", d.Addr)
	tg := bacnet.Target{Dest: bacnet.Dest{Addr: addr}, Peer: bacnet.Peer{MaxAPDU: 1476}, Timeout: time.Second, Retries: 1}
	if err := c.SubscribeCOV(context.Background(), tg, 77, ai, false, 15); err != nil {
		t.Fatal(err)
	}
	d.Set(ai, float32(2))
	select {
	case a := <-got:
		n, err := bacnet.DecodeCOVNotification(a.Data)
		if err != nil {
			t.Fatal(err)
		}
		if n.Process != 77 || n.Device.Instance != 4321 || n.Object != ai {
			t.Errorf("notification %+v", n)
		}
		// Time remaining follows the process, device and object (13.7).
		rest := a.Data
		for i := 0; i < 4; i++ {
			tag, r, err := bacnet.ReadTag(rest)
			if err != nil {
				t.Fatal(err)
			}
			if i == 3 {
				if v, _ := bacnet.BeUnsigned(tag.Data); v != 15 {
					t.Errorf("time remaining %d, want 15", v)
				}
			}
			rest = r
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no COV notification")
	}
}
