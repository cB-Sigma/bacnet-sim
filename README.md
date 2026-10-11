# bacnet-sim

A simulated BACnet/IP device in Go, for testing BACnet clients without a building controller.

[![Go Reference](https://pkg.go.dev/badge/github.com/cB-Sigma/bacnet-sim.svg)](https://pkg.go.dev/github.com/cB-Sigma/bacnet-sim) [![test](https://github.com/cB-Sigma/bacnet-sim/actions/workflows/test.yml/badge.svg)](https://github.com/cB-Sigma/bacnet-sim/actions/workflows/test.yml) [![Licence: Apache-2.0](https://img.shields.io/badge/licence-Apache--2.0-blue.svg)](LICENSE) [![Go](https://img.shields.io/badge/go-1.27.1-00ADD8.svg)](go.mod)

The simulator answers Who-Is and Who-Has, serves an object table to
ReadProperty, ReadPropertyMultiple, WriteProperty (with a priority
array) and WritePropertyMultiple, sends COV notifications, keeps trend
logs for ReadRange, holds files, takes alarm, event and device
management requests, and segments replies and reassembles requests. It
can stand behind a simulated router or act as a BBMD that takes foreign
devices, and has switches that make it misbehave (refuse services, drop
or stall segments). It is built on the codec of
[bacnet](https://github.com/cB-Sigma/bacnet). Written for SigmaM2M, a
oneM2M platform; it has no dependency on it. It is a test device, not a
conformant BACnet server.

## What is simulated

| Package | What it does |
|---|---|
| `sim` | `New`, `Add` objects (`Object`: present value, priority array and relinquish default, units, status flags, multistate states and texts, accumulator scale and pulse rate, trend log records), `Start` and `Stop`; `Set` (changes a value and notifies COV subscribers), `LogAdd`, `LogBurst`; files, notification class recipients, event information and alarm summaries; router mode (`Network`, `MAC`), BBMD mode (`BBMD`, `FDNAK`), Forwarded-NPDU replies (`Forward`); segmentation knobs (`MaxAPDU`, `Window`, `DropSeg`, `StallSeg`); refusals (`RejectRPM`, `RejectWPM`, `RejectSeq`); what the client sent (`Writes`, `Ranges`, `Mgmt`, `Unconf`, `Acks`, `SegACKs`, `Registrations`, ...) |
| `cmd/bacnetsim` | Serves the simulator with a small demo object table |

Not simulated: a Device object, BACnet/IPv6, BACnet/SC, MS/TP.

## Install

```
go get github.com/cB-Sigma/bacnet-sim@latest
```

Requires Go 1.27.1 or later. The command:

```
go install github.com/cB-Sigma/bacnet-sim/cmd/bacnetsim@latest
```

## Command line

```
bacnetsim -listen 127.0.0.1:47808 -instance 1234
```

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `127.0.0.1:47808` | UDP address to listen on |
| `-instance` | `1234` | device instance |
| `-report` | `5s` | how often the temperature changes (0: never) |

The demo objects: analog input 1 (a temperature that moves), analog
value 1 (a writable set point), binary value 1 (a fan command) and
multistate value 1 (Off, Auto, On).

## In Go tests

From `ExampleClient_ReadProperty`:

```go
dev := sim.New()
dev.Add(temp, &sim.Object{Pv: float32(21.5), Units: 62, HasUnits: true})
if err := dev.Start(); err != nil {
	panic(err)
}
defer dev.Stop()
c, _ := bacnet.NewClient("127.0.0.1:0", slog.Default())
go c.Serve()
addr, _ := net.ResolveUDPAddr("udp", dev.Addr)
v, err := c.ReadProperty(ctx, bacnet.Target{Dest: bacnet.Dest{Addr: addr}, Peer: bacnet.Peer{MaxAPDU: 1476}, Timeout: time.Second, Retries: 2}, temp, bacnet.PropPresentValue)
```

## Demo values

Every address, device instance and value in this module is a made-up
demo value.

## Documentation

Reference: https://pkg.go.dev/github.com/cB-Sigma/bacnet-sim

## Testing

```
go vet ./...
go test -race ./...
```

The simulator and the bacnet client are written by the same author from
the same standard, so tests between them show that the two agree, not
that either is right. The independent check is `interop/`, a nested
test-only module that builds bacnet-stack 1.6.1 (GPL, used only as a
test peer) in Docker and runs its client tools against the simulator.
It is skipped unless `SIGMA_BACNET_SIM_INTEROP=1` is set and Docker is
running:

```
cd interop && SIGMA_BACNET_SIM_INTEROP=1 go test -race -v ./...
```

### Interop tested against

Run on 2026-10-11 on macOS with Docker Desktop; every item passed.
bacnet-stack 1.6.1's tools against this simulator:

| Tool | What was checked |
|---|---|
| `bacrp` | an analog input's present-value, a multistate value's state-text array, an unknown object refused with object / unknown-object |
| `bacrpm` | present-value, units and status-flags of one object and present-value of another in one request |
| `bacwp` | REAL written at priority 8, acknowledged, seen by the simulator and read back |
| `bacwi` | registered as a foreign device with the simulator's BBMD, Who-Is through Distribute-Broadcast-To-Network, I-Am bound |
| `bacrr` | a trend log's log-buffer by position, through the BBMD |
| `bacscov` | SubscribeCOV acknowledged; the notification after a change carries bacscov's process identifier and lifetime |

Defects of the simulator found by these tests and fixed in v0.1.0 are in
CHANGELOG.md. Not tested against an independent peer: segmented replies
and requests (bacnet-stack 1.6.1 has no segmentation), router mode,
confirmed COV notifications, files, alarm and event services.

## Related repositories

- [bacnet](https://github.com/cB-Sigma/bacnet): the BACnet client.

## Status

v0.x. The API may change before v1.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security

See [SECURITY.md](SECURITY.md).

## Licence

Apache-2.0 (see [LICENSE](LICENSE)). Copyright 2026 Abhinav Gautam.
[NOTICE](NOTICE) names the trademark used.
