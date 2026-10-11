# Provenance

## How the code was written

Written for SigmaM2M, a oneM2M platform, in Go, by the author of the
bacnet module and from the same standard (ANSI/ASHRAE Standard 135-2024,
read in a licensed copy). No code was ported or copied from another
implementation. The simulated device was first written next to
SigmaM2M's BACnet adapter to test it without controllers. The simulator,
its command and the example that drives it moved out of the bacnet
module into this one on 2026-10-11 (with `git mv`), so the library does
not depend on its simulator.

## Sources

| Area (files) | Followed |
|---|---|
| Simulator (sim/sim.go) | clauses 5, 6, 12, 13, 15 and 16 and Annex J, as the bacnet module reads them; it reuses that module's codec |
| Command (cmd/bacnetsim) | none (flags are our own) |
| Interop peer (interop/peer) | bacnet-stack 1.6.1's build files and its tools' usage text and environment variables, read to build and drive them |

No specification text is reproduced; comments cite clause numbers in our
own words.

## Tests

| Test | Source |
|---|---|
| globalbcast_test.go, covprocess_test.go | what bacnet-stack 1.6.1's tools sent in the interop tests (a Who-Is to the global broadcast network; a subscription with its own process identifier and lifetime), rebuilt by hand |
| example_test.go, cmd/bacnetsim/main_test.go | the bacnet client against this simulator, written from the same standard; not a real device |
| interop/ | bacnet-stack 1.6.1's client tools |

All addresses and values are made-up demo values.
