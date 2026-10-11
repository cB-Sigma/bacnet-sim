// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package bacnet_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

// Read present-value of an analog input from a simulated device over
// BACnet/IP.
func ExampleClient_ReadProperty() {
	temp := bacnet.ObjectID{Type: bacnet.AnalogInput, Instance: 1}
	dev := sim.New()
	dev.Add(temp, &sim.Object{Pv: float32(21.5), Units: 62, HasUnits: true})
	if err := dev.Start(); err != nil {
		panic(err)
	}
	defer dev.Stop()

	c, err := bacnet.NewClient("127.0.0.1:0", slog.New(slog.DiscardHandler))
	if err != nil {
		panic(err)
	}
	defer c.Close()
	go c.Serve()

	addr, _ := net.ResolveUDPAddr("udp", dev.Addr)
	target := bacnet.Target{Dest: bacnet.Dest{Addr: addr}, Peer: bacnet.Peer{MaxAPDU: 1476}, Timeout: time.Second, Retries: 2}
	v, err := c.ReadProperty(context.Background(), target, temp, bacnet.PropPresentValue)
	fmt.Println(v, err)
	// Output: [21.5] <nil>
}
