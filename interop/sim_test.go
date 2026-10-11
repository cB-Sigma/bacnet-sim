// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package interop runs bacnet-stack's client tools (bacrp, bacwp, bacrpm,
// bacwi, bacscov, bacrr; built in a container from peer/Dockerfile)
// against the simulated device of package sim over BACnet/IP, directly
// and as foreign devices registered with the simulator's BBMD mode. The
// tests need Docker and are skipped unless SIGMA_BACNET_SIM_INTEROP=1.
package interop

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/cB-Sigma/bacnet"
	"github.com/cB-Sigma/bacnet-sim/sim"
)

var (
	temp  = bacnet.ObjectID{Type: bacnet.AnalogInput, Instance: 1}
	setpt = bacnet.ObjectID{Type: bacnet.AnalogValue, Instance: 1}
	mode  = bacnet.ObjectID{Type: bacnet.MultiStateValue, Instance: 1}
	trend = bacnet.ObjectID{Type: bacnet.TrendLog, Instance: 1}
)

// hostIPv4 is the host's IPv4 address as containers see it.
func hostIPv4(t *testing.T) string {
	t.Helper()
	out := run(t, image, "getent", "ahostsv4", "host.docker.internal")
	f := strings.Fields(out)
	if len(f) == 0 || net.ParseIP(f[0]) == nil {
		t.Fatalf("host address: %q", out)
	}
	return f[0]
}

// device starts the simulator on every interface (so containers reach
// it) as device 1234, a BBMD, with a few objects, and returns it with the
// address containers use for it.
func device(t *testing.T) (*sim.Device, string) {
	t.Helper()
	d := sim.New()
	d.Addr = "0.0.0.0:0"
	d.BBMD = true
	d.Add(temp, &sim.Object{Pv: float32(21.5), Units: 62, HasUnits: true})
	d.Add(setpt, &sim.Object{Relinq: float32(20), Units: 62, HasUnits: true})
	d.Add(mode, &sim.Object{Relinq: uint64(2), States: 3, Texts: []string{"Off", "Auto", "On"}})
	d.Add(trend, &sim.Object{})
	d.LogBurst(trend, time.Date(2026, 10, 11, 8, 0, 0, 0, time.UTC), 0, 20)
	if err := d.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Stop)
	return d, fmt.Sprintf("%s:%d", hostIPv4(t), d.UDPAddr().Port)
}

// tool runs one bacnet-stack command against the simulator.
func tool(t *testing.T, env []string, args ...string) string {
	t.Helper()
	full := append(append([]string{}, env...), image)
	full = append(full, args...)
	return run(t, full...)
}

func TestBacnetStackToolsAgainstSimulator(t *testing.T) {
	needDocker(t)
	d, addr := device(t)
	host, port, _ := net.SplitHostPort(addr)
	timeouts := []string{"-e", "BACNET_APDU_TIMEOUT=2000", "-e", "BACNET_APDU_RETRIES=2"}

	t.Run("ReadProperty", func(t *testing.T) {
		out := tool(t, timeouts, "/opt/bacnet/bip/bacrp", "--mac", addr, "--dnet", "0", "1234", "analog-input", "1", "present-value")
		if !strings.Contains(out, "21.5") {
			t.Errorf("bacrp present-value:\n%s", out)
		}
		out = tool(t, timeouts, "/opt/bacnet/bip/bacrp", "--mac", addr, "--dnet", "0", "1234", "multi-state-value", "1", "state-text")
		if !strings.Contains(out, "Off") || !strings.Contains(out, "Auto") || !strings.Contains(out, "On") {
			t.Errorf("bacrp state-text:\n%s", out)
		}
		// bacrp exits 1 after printing the error.
		full := append(append([]string{"run", "--rm"}, timeouts...), image, "/opt/bacnet/bip/bacrp", "--mac", addr, "--dnet", "0", "1234", "analog-input", "7", "present-value")
		b, _ := exec.Command("docker", full...).CombinedOutput()
		if out := string(b); !strings.Contains(strings.ToLower(out), "unknown-object") {
			t.Errorf("bacrp of an unknown object:\n%s", out)
		}
	})

	t.Run("ReadPropertyMultiple", func(t *testing.T) {
		out := tool(t, timeouts, "/opt/bacnet/bip/bacrpm", "--mac", addr, "--dnet", "0", "1234", "analog-input", "1", "present-value,units,status-flags", "analog-value", "1", "present-value")
		for _, want := range []string{"21.5", "degrees-celsius", "20"} {
			if !strings.Contains(out, want) {
				t.Errorf("bacrpm output without %q:\n%s", want, out)
			}
		}
	})

	t.Run("WriteProperty", func(t *testing.T) {
		// Tag 4 is REAL; priority 8, no array index.
		out := tool(t, timeouts, "/opt/bacnet/bip/bacwp", "--mac", addr, "--dnet", "0", "1234", "analog-value", "1", "present-value", "8", "-1", "4", "42.5")
		if !strings.Contains(out, "Acknowledged") {
			t.Errorf("bacwp:\n%s", out)
		}
		d.Mu.Lock()
		writes := append([]sim.Write(nil), d.Writes...)
		d.Mu.Unlock()
		if len(writes) != 1 || writes[0].Object != setpt || writes[0].Value != float32(42.5) || writes[0].Priority != 8 {
			t.Errorf("writes the simulator received: %+v", writes)
		}
		out = tool(t, timeouts, "/opt/bacnet/bip/bacrp", "--mac", addr, "--dnet", "0", "1234", "analog-value", "1", "present-value")
		if !strings.Contains(out, "42.5") {
			t.Errorf("bacrp after the write:\n%s", out)
		}
	})

	// As a foreign device of the simulator's BBMD: bacnet-stack registers,
	// broadcasts Who-Is through it and binds with the I-Am.
	fd := append([]string{"-e", "BACNET_BBMD_ADDRESS=" + host, "-e", "BACNET_BBMD_PORT=" + port, "-e", "BACNET_IP_PORT=47809", "-e", "BACNET_IP_DEBUG=1"}, timeouts...)

	t.Run("WhoIsThroughBBMD", func(t *testing.T) {
		out := tool(t, fd, "/opt/bacnet/bip/bacwi", "1234")
		if !strings.Contains(out, "1234") || !strings.Contains(out, "1476") {
			t.Errorf("bacwi:\n%s", out)
		}
		d.Mu.Lock()
		regs, dist := len(d.Registrations), d.Distributed
		d.Mu.Unlock()
		if regs == 0 || dist == 0 {
			t.Errorf("registrations %d, distributed broadcasts %d", regs, dist)
		}
	})

	t.Run("ReadRange", func(t *testing.T) {
		out := tool(t, fd, "/opt/bacnet/bip/bacrr", "1234", "trend-log", "1", "log-buffer", "1", "1", "5")
		if !strings.Contains(out, "2026") {
			t.Errorf("bacrr:\n%s", out)
		}
	})

	t.Run("SubscribeCOV", func(t *testing.T) {
		p := startPeer(t, append(append([]string{}, fd...), image, "stdbuf", "-oL", "/opt/bacnet/bip/bacscov", "1234", "analog-input", "1", "7", "unconfirmed", "15")...)
		p.expect(30*time.Second, "Sent SubscribeCOV")
		deadline := time.Now().Add(10 * time.Second)
		for {
			d.Mu.Lock()
			_, ok := d.Subs[temp]
			d.Mu.Unlock()
			if ok {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the simulator did not receive the subscription")
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(500 * time.Millisecond)
		d.Set(temp, float32(30.25))
		p.expect(10*time.Second, "UCOV: PID=7 instance=1234 analog-input 1 time remaining=15")
	})
}
