# Changelog

## v0.1.0 (2026-10-11)

First release, checked with bacnet-stack 1.6.1's client tools (see
README.md, "Interop tested against").

Fixed while preparing it:

- An NPDU addressed to the global broadcast network (DNET 0xFFFF) was
  dropped, so a Who-Is sent that way (as bacnet-stack sends it) got no
  I-Am and bacnet-stack's tools could not bind to the simulator. Global
  broadcasts are now served; other remote networks are still ignored
  (`TestSimAnswersGlobalBroadcastWhoIs`).
- COV notifications carried process identifier 1, 60 seconds remaining
  and device 1234 whatever the subscription said. They now carry the
  subscriber's process identifier and lifetime and the simulator's own
  device instance (`TestSimCOVNotificationEchoesSubscription`).
