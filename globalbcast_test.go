// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

package bacnet_test

import (
	"net"
	"testing"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

// A Who-Is sent as a global broadcast (DNET 0xFFFF), as bacnet-stack's
// tools send it, is answered: the simulator used to drop every NPDU with
// a destination network, so bacwi, bacrr and bacscov never bound to it.
func TestSimAnswersGlobalBroadcastWhoIs(t *testing.T) {
	d := sim.New()
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	defer d.Stop()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	whoIs := append([]byte{bacnet.PDUUnconfirmed << 4, bacnet.SvcWhoIs}, bacnet.EncodeWhoIs(1234)...)
	frame := bacnet.EncodeNPDU(bacnet.BVLCOriginalUnicast, bacnet.NPDU{Dnet: 0xffff, Hop: 255, Data: whoIs})
	if _, err := c.WriteToUDP(frame, d.UDPAddr()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1500)
	n, _, err := c.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("no I-Am to a global-broadcast Who-Is: %v", err)
	}
	f, err := bacnet.DecodeFrame(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	a, err := bacnet.ParseAPDU(f.Data)
	if err != nil || a.Typ != bacnet.PDUUnconfirmed || a.Service != bacnet.SvcIAm {
		t.Fatalf("reply %+v, %v", a, err)
	}
	i, err := bacnet.DecodeIAm(a.Data)
	if err != nil || i.Device.Instance != 1234 {
		t.Errorf("I-Am %+v, %v", i, err)
	}

	// A Who-Is for another remote network is still not answered.
	frame = bacnet.EncodeNPDU(bacnet.BVLCOriginalUnicast, bacnet.NPDU{Dnet: 7, DADR: []byte{1}, Hop: 255, Data: whoIs})
	if _, err := c.WriteToUDP(frame, d.UDPAddr()); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if _, _, err := c.ReadFromUDP(buf); err == nil {
		t.Error("a Who-Is for network 7 was answered")
	}
}
