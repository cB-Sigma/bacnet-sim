// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Command bacnetsim serves a simulated BACnet/IP device, so a BACnet
// Client can be tried without a building controller. It listens on UDP
// (default 127.0.0.1:47808), answers Who-Is as device -instance, and
// holds a small demo object table:
//
//	analog-input 1       supply temperature, degrees Celsius (moves every -report)
//	analog-value 1       set point, degrees Celsius (writable, priority array)
//	binary-value 1       fan command (writable)
//	multistate-value 1   mode: Off, Auto, On (writable)
//
// For example:
//
//	bacnetsim -listen 127.0.0.1:47808 -instance 1234
//
// It is a test device: see the sim package for what it serves.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, nil); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "bacnetsim:", err)
		os.Exit(1)
	}
}

// Demo objects.
var (
	Temp = bacnet.ObjectID{Type: bacnet.AnalogInput, Instance: 1}
	Set  = bacnet.ObjectID{Type: bacnet.AnalogValue, Instance: 1}
	Fan  = bacnet.ObjectID{Type: bacnet.BinaryValue, Instance: 1}
	Mode = bacnet.ObjectID{Type: bacnet.MultiStateValue, Instance: 1}
)

// run serves until ctx ends. ready, when not nil, is called once the
// Device listens.
func run(ctx context.Context, args []string, out io.Writer, ready func(*sim.Device)) error {
	fs := flag.NewFlagSet("bacnetsim", flag.ContinueOnError)
	fs.SetOutput(out)
	listen := fs.String("listen", "127.0.0.1:47808", "UDP address to listen on")
	instance := fs.Uint("instance", 1234, "device instance (0 to 4194302)")
	report := fs.Duration("report", 5*time.Second, "how often the temperature changes (0: never)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if *instance > 4194302 {
		return errors.New("-instance is 0 to 4194302")
	}
	s := sim.New()
	s.Addr = *listen
	s.Iam.Device.Instance = uint32(*instance)
	s.Add(Temp, &sim.Object{Pv: float32(18.5), Units: 62, HasUnits: true})
	s.Add(Set, &sim.Object{Relinq: float32(21), Units: 62, HasUnits: true})
	s.Add(Fan, &sim.Object{Relinq: bacnet.Enumerated(0)})
	s.Add(Mode, &sim.Object{Relinq: uint64(2), States: 3, Texts: []string{"Off", "Auto", "On"}})
	if err := s.Start(); err != nil {
		return err
	}
	defer s.Stop()
	fmt.Fprintf(out, "bacnetsim: device %d at %s\n", *instance, s.Addr)
	if ready != nil {
		ready(s)
	}
	var tick <-chan time.Time
	if *report > 0 {
		t := time.NewTicker(*report)
		defer t.Stop()
		tick = t.C
	}
	for n := 1; ; n++ {
		select {
		case <-ctx.Done():
			return nil
		case <-tick:
			s.Set(Temp, float32(18.5+float64(n%10)/2))
		}
	}
}
