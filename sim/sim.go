// Copyright 2026 Abhinav Gautam
// SPDX-License-Identifier: Apache-2.0

// Package sim is a simulated BACnet/IP device for tests and trials: an
// Object table (analog, binary and multistate objects, accumulators,
// trend logs, files, notification classes) served over UDP with the
// Confirmed and unconfirmed services the bacnet client uses, segmentation
// both ways, a router and BBMD mode, and knobs to refuse services or drop
// segments. It is a test device, not a conformant BACnet server.
package sim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/cB-Sigma/bacnet"
)

// Device is an in-process BACnet/IP device on 127.0.0.1: a table of analog
// and binary objects answering ReadProperty, ReadPropertyMultiple,
// WriteProperty (with a priority array for outputs and values),
// SubscribeCOV (unconfirmed notifications on every change) and Who-Is.
// It segments replies longer than maxAPDU and reassembles segmented
// requests. With network set it is a router with the device behind it on
// that network at mac; with bbmd set it is also a BBMD taking foreign
// devices.
type Device struct {
	Mu        sync.Mutex
	Conn      *net.UDPConn
	Addr      string
	Objs      map[bacnet.ObjectID]*Object
	Writes    []Write
	Rp, RPM   int
	RejectRPM bool
	Subs      map[bacnet.ObjectID]*net.UDPAddr
	SubConf   map[bacnet.ObjectID]bool // subscriptions asking for confirmed notifications
	subProc   map[bacnet.ObjectID]uint32
	subLife   map[bacnet.ObjectID]uint32
	COVReply  []bacnet.APDU // the IPE's answers to ConfirmedCOVNotification
	covID     byte
	RejectSeq bool // refuse ReadRange by sequence number
	Ranges    []bacnet.ReadRange
	done      chan struct{}
	Iam       *bacnet.IAm // answer to Who-Is; nil for none
	Acks      [][]byte    // AcknowledgeAlarm requests received
	LogCap    int         // trend log buffer size; 0 for 50
	// services of mgmt.go
	Mgmt       []bacnet.APDU     // DCC, ReinitializeDevice and the like, as received
	Unconf     []bacnet.APDU     // unconfirmed requests received (time sync, Who-Has)
	Files      map[uint32][]byte // File objects (stream access)
	Recipients map[uint32][]byte // Notification Class Recipient_List, encoded
	EventInfo  []byte            // GetEventInformation-ACK body; nil: refused
	AlarmSum   []byte            // GetAlarmSummary-ACK body
	RejectWPM  bool
	WPM        int
	COVProp    []bacnet.APDU
	LastFrom   *net.UDPAddr // the last requester

	// segmentation
	MaxAPDU  int  // largest reply sent unsegmented; 0 for 1476
	Window   byte // proposed window of segmented replies
	rxWindow byte // actual window granted to segmented requests
	DropSeg  int  // reply segment left out once; -1 for none
	StallSeg int  // reply segment before which the reply stops once; -1 for none
	SegACKs  []bacnet.APDU
	Aborts   []bacnet.APDU
	SegReqs  int    // segmented requests reassembled
	Proposed []byte // windows the requester proposed
	Rx       map[bacnet.PendingKey]*RxState
	Txs      map[bacnet.PendingKey]chan bacnet.APDU

	// routing
	Network     uint16
	MAC         []byte
	Routed      []bacnet.NPDU // requests received for the device behind the router
	WhoIsRouter int
	whoIs       int // Who-Is requests answered

	// BBMD
	BBMD          bool
	FDNAK         bool
	Registrations []uint16 // time-to-live of each registration
	registered    bool
	Distributed   int
	Forward       bool // send replies as Forwarded-NPDU
}

type RxState struct {
	data          []byte
	next, initial byte
}

type Object struct {
	Units    uint32
	Flags    [4]bool
	Pv       any     // inputs
	prio     [16]any // outputs and values: nil means relinquished
	Relinq   any
	HasUnits bool

	States uint64   // multistate: number-of-states
	Texts  []string // multistate: state-text
	Scale  any      // accumulator: ContextValue
	Pulse  any      // accumulator: pulse-rate
	log    []simRec // trend log buffer, oldest first
	Total  uint64   // trend log: total-record-count
}

type simRec struct {
	seq uint64
	rec bacnet.LogRecord
}

// LogAdd appends a record to a trend log (the sim keeps at most 50).
func (s *Device) LogAdd(o bacnet.ObjectID, r bacnet.LogRecord) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	obj := s.Objs[o]
	obj.Total = bacnet.SeqNext(obj.Total)
	obj.log = append(obj.log, simRec{obj.Total, r})
	if len(obj.log) > s.logMax() {
		obj.log = obj.log[1:]
	}
}

// encodeSimValue writes a property value: a list for arrays (state-text),
// a context-tagged primitive for a ContextValue.
func encodeSimValue(e *bacnet.Enc, v any) {
	switch x := v.(type) {
	case []string:
		for _, t := range x {
			_ = e.App(t)
		}
	case bacnet.ContextValue:
		e.Header(x.Num, true, len(x.Data))
		e.B = append(e.B, x.Data...)
	default:
		_ = e.App(v)
	}
}

type Write struct {
	Object   bacnet.ObjectID
	Value    any
	Priority int // 0 when the request had none
}

// New is a simulated device listening, once started, on 127.0.0.1 (any
// port; set Addr before Start for another) as device 1234 of vendor 999,
// with no objects (Add them).
func New() *Device {
	return &Device{
		Objs: map[bacnet.ObjectID]*Object{}, Subs: map[bacnet.ObjectID]*net.UDPAddr{}, SubConf: map[bacnet.ObjectID]bool{}, Addr: "127.0.0.1:0",
		Iam:    &bacnet.IAm{Device: bacnet.ObjectID{Type: 8, Instance: 1234}, MaxAPDU: 1476, Segmentation: bacnet.SegBoth, Vendor: 999},
		Window: 4, rxWindow: 2, DropSeg: -1, StallSeg: -1,
		Rx: map[bacnet.PendingKey]*RxState{}, Txs: map[bacnet.PendingKey]chan bacnet.APDU{},
		Files: map[uint32][]byte{}, Recipients: map[uint32][]byte{},
	}
}

// Start binds Addr (the address it was bound to is then in Addr) and
// serves until Stop. A stopped device can be started again.
func (s *Device) Start() error {
	la, err := net.ResolveUDPAddr("udp", s.Addr)
	if err != nil {
		return err
	}
	conn, err := net.ListenUDP("udp", la)
	if err != nil {
		return err
	}
	s.Mu.Lock()
	s.Conn = conn
	s.Addr = conn.LocalAddr().String()
	s.done = make(chan struct{})
	done := s.done
	s.Mu.Unlock()
	go func() {
		defer close(done)
		s.serve(conn)
	}()
	return nil
}

func (s *Device) Stop() {
	s.Mu.Lock()
	conn, done := s.Conn, s.done
	s.Conn = nil
	s.Mu.Unlock()
	if conn != nil {
		_ = conn.Close()
		<-done
	}
}

func (s *Device) Add(o bacnet.ObjectID, obj *Object) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Objs[o] = obj
}

// Set changes an input's present-value (or a value object's relinquish
// default) and notifies COV subscribers.
func (s *Device) Set(o bacnet.ObjectID, v any) {
	s.Mu.Lock()
	obj := s.Objs[o]
	if simIsInput(o.Type) {
		obj.Pv = v
	} else {
		obj.Relinq = v
	}
	s.Mu.Unlock()
	s.Notify(o)
}

func (obj *Object) present(typ uint16) any {
	if simIsInput(typ) {
		return obj.Pv
	}
	for _, v := range obj.prio {
		if v != nil {
			return v
		}
	}
	return obj.Relinq
}

// Prop returns a property value, or an error class and code.
func (s *Device) prop(o bacnet.ObjectID, p uint32) (any, uint32, uint32) {
	obj := s.Objs[o]
	if obj == nil {
		return nil, 1, 31 // object, unknown-object
	}
	switch p {
	case bacnet.PropPresentValue:
		if o.Type == bacnet.TrendLog {
			break
		}
		return obj.present(o.Type), 0, 0
	case bacnet.PropStatusFlags:
		return bacnet.BitString{Len: 4, Bits: obj.Flags[:]}, 0, 0
	case bacnet.PropUnits:
		if obj.HasUnits {
			return bacnet.Enumerated(obj.Units), 0, 0
		}
	case bacnet.PropOutOfService:
		return obj.Flags[3], 0, 0
	case bacnet.PropNumberOfStates:
		if obj.States > 0 {
			return obj.States, 0, 0
		}
	case bacnet.PropStateText:
		if obj.Texts != nil {
			return obj.Texts, 0, 0
		}
	case bacnet.PropScale:
		if obj.Scale != nil {
			return obj.Scale, 0, 0
		}
	case bacnet.PropPulseRate:
		if obj.Pulse != nil {
			return obj.Pulse, 0, 0
		}
	case bacnet.PropRecordCount:
		if o.Type == bacnet.TrendLog {
			return uint64(len(obj.log)), 0, 0
		}
	case bacnet.PropTotalRecordCount:
		if o.Type == bacnet.TrendLog {
			return obj.Total, 0, 0
		}
	}
	return nil, 2, 32 // property, unknown-property
}

func (s *Device) serve(conn *net.UDPConn) {
	buf := make([]byte, 1500)
	for {
		n, from, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		f, err := bacnet.DecodeFrame(append([]byte(nil), buf[:n]...))
		if err != nil {
			continue
		}
		s.frameIn(conn, from, f)
	}
}

func (s *Device) UDPAddr() *net.UDPAddr {
	a, _ := net.ResolveUDPAddr("udp", s.Addr)
	return a
}

// Out frames an APDU from the device: with the router's SNET/SADR in
// router mode, as a Forwarded-NPDU when forward is set.
func (s *Device) Out(a []byte) []byte {
	s.Mu.Lock()
	n := bacnet.NPDU{Data: a, Snet: s.Network, SADR: s.MAC}
	fwd := s.Forward
	s.Mu.Unlock()
	if fwd {
		n.Origin = s.UDPAddr()
		return bacnet.EncodeNPDU(bacnet.BVLCForwardedNPDU, n)
	}
	return bacnet.EncodeNPDU(bacnet.BVLCOriginalUnicast, n)
}

func (s *Device) frameIn(conn *net.UDPConn, from *net.UDPAddr, f bacnet.NPDU) {
	result := func(code uint16) {
		_, _ = conn.WriteToUDP(bacnet.BVLC(bacnet.BVLCResult, binary.BigEndian.AppendUint16(nil, code)), from)
	}
	s.Mu.Lock()
	bbmd, network := s.BBMD, s.Network
	s.Mu.Unlock()
	switch f.Fn {
	case bacnet.BVLCRegisterFD:
		if !bbmd || len(f.Data) != 2 {
			return
		}
		s.Mu.Lock()
		s.Registrations = append(s.Registrations, binary.BigEndian.Uint16(f.Data))
		nak := s.FDNAK
		s.registered = !nak
		s.Mu.Unlock()
		if nak {
			result(bacnet.ResultRegisterFDNAK)
		} else {
			result(bacnet.ResultOK)
		}
		return
	case bacnet.BVLCDistributeBcast:
		if !bbmd {
			return
		}
		s.Mu.Lock()
		s.Distributed++
		ok := s.registered
		s.Mu.Unlock()
		if !ok {
			result(bacnet.ResultDistributeNAK)
			return
		}
	case bacnet.BVLCOriginalUnicast, bacnet.BVLCOriginalBcast:
	default:
		return
	}
	if f.NetMsg {
		if f.MsgType == bacnet.NlWhoIsRouter && network != 0 && (len(f.Data) < 2 || binary.BigEndian.Uint16(f.Data) == network) {
			s.Mu.Lock()
			s.WhoIsRouter++
			s.Mu.Unlock()
			n := bacnet.NPDU{NetMsg: true, MsgType: bacnet.NlIAmRouter, Data: binary.BigEndian.AppendUint16(nil, network)}
			if f.Fn == bacnet.BVLCDistributeBcast { // the BBMD forwards the router's broadcast
				n.Origin = s.UDPAddr()
				_, _ = conn.WriteToUDP(bacnet.EncodeNPDU(bacnet.BVLCForwardedNPDU, n), from)
			} else {
				_, _ = conn.WriteToUDP(bacnet.EncodeNPDU(bacnet.BVLCOriginalUnicast, n), from)
			}
		}
		return
	}
	if network != 0 {
		s.Mu.Lock()
		mine := f.Dnet == network && bytes.Equal(f.DADR, s.MAC)
		if mine {
			s.Routed = append(s.Routed, f)
		}
		s.Mu.Unlock()
		if !mine {
			return
		}
	} else if f.Dnet != 0 && f.Dnet != 0xffff {
		// A global broadcast (DNET 0xFFFF, clause 6.2.2) is for every
		// device, this one included; bacnet-stack sends its Who-Is so.
		return
	}
	a, err := bacnet.ParseAPDU(f.Data)
	if err != nil {
		return
	}
	send := func(b []byte) { _, _ = conn.WriteToUDP(s.Out(b), from) }
	key := bacnet.PendingKey{Addr: from.String(), ID: a.InvokeID}
	switch a.Typ {
	case bacnet.PDUUnconfirmed:
		s.Mu.Lock()
		i := s.Iam
		s.Mu.Unlock()
		s.Mu.Lock()
		s.Unconf = append(s.Unconf, a)
		s.Mu.Unlock()
		if a.Service == bacnet.SvcWhoHas && i != nil {
			// Answer for the object asked by name "OATemp" or as AI 3.
			if bytes.Contains(a.Data, []byte("OATemp")) || bytes.Contains(a.Data, []byte{0x2c, 0, 0, 0, 3}) {
				var e bacnet.Enc
				_ = e.App(i.Device)
				_ = e.App(bacnet.ObjectID{Type: bacnet.AnalogInput, Instance: 3})
				_ = e.App("OATemp")
				send(append([]byte{bacnet.PDUUnconfirmed << 4, bacnet.SvcIHave}, e.B...))
			}
		}
		if a.Service == bacnet.SvcWhoIs && i != nil && WhoIsIncludes(a.Data, i.Device.Instance) {
			s.Mu.Lock()
			s.whoIs++
			s.Mu.Unlock()
			send(append([]byte{bacnet.PDUUnconfirmed << 4, bacnet.SvcIAm}, bacnet.EncodeIAm(*i)...))
		}
	case bacnet.PDUSimpleACK, bacnet.PDUError, bacnet.PDUReject:
		s.Mu.Lock()
		s.COVReply = append(s.COVReply, a)
		s.Mu.Unlock()
	case bacnet.PDUSegmentACK, bacnet.PDUAbort:
		s.Mu.Lock()
		if a.Typ == bacnet.PDUAbort {
			s.Aborts = append(s.Aborts, a)
		} else {
			s.SegACKs = append(s.SegACKs, a)
		}
		ch := s.Txs[key]
		s.Mu.Unlock()
		if ch != nil {
			select {
			case ch <- a:
			default:
			}
		}
	case bacnet.PDUConfirmed:
		if a.Seg {
			s.segmentIn(a, key, send)
			return
		}
		s.respond(a, from, send)
	}
}

// segmentIn reassembles a segmented request, acknowledging the first
// segment, each full window and the last.
func (s *Device) segmentIn(a bacnet.APDU, key bacnet.PendingKey, send func([]byte)) {
	s.Mu.Lock()
	rx := s.Rx[key]
	if a.Seq == 0 {
		rx = &RxState{}
		s.Rx[key] = rx
		s.Proposed = append(s.Proposed, a.Window)
	}
	w := s.rxWindow
	s.Mu.Unlock()
	if rx == nil {
		return
	}
	if a.Seq != rx.next {
		send(bacnet.SegmentACK(a.InvokeID, rx.next-1, w, true, true))
		rx.initial = rx.next - 1
		return
	}
	rx.data = append(rx.data, a.Data...)
	rx.next++
	if a.Seq == 0 || !a.More || a.Seq-rx.initial == w {
		send(bacnet.SegmentACK(a.InvokeID, a.Seq, w, false, true))
		rx.initial = a.Seq
	}
	if a.More {
		return
	}
	s.Mu.Lock()
	s.SegReqs++
	delete(s.Rx, key)
	s.Mu.Unlock()
	full := a
	full.Seg, full.Data = false, rx.data
	from, _ := net.ResolveUDPAddr("udp", key.Addr)
	s.respond(full, from, send)
}

// respond answers a request, segmenting a Complex-ACK longer than the
// sim's max APDU (or the requester's) when the requester accepts that.
func (s *Device) respond(a bacnet.APDU, from *net.UDPAddr, send func([]byte)) {
	s.Mu.Lock()
	s.LastFrom = from
	s.Mu.Unlock()
	reply := s.Handle(a, from)
	if reply == nil {
		return
	}
	s.Mu.Lock()
	limit := s.MaxAPDU
	if limit == 0 {
		limit = 1476
	}
	limit = min(limit, a.MaxResp)
	s.Mu.Unlock()
	if len(reply) <= limit || reply[0]>>4 != bacnet.PDUComplexACK {
		send(reply)
		return
	}
	if !a.Sa {
		send(bacnet.EncodeAbort(a.InvokeID, 4, true)) // segmentation-not-supported
		return
	}
	key := bacnet.PendingKey{Addr: from.String(), ID: a.InvokeID}
	ch := make(chan bacnet.APDU, 64)
	s.Mu.Lock()
	s.Txs[key] = ch
	s.Mu.Unlock()
	go s.sendSegments(key, ch, a.InvokeID, reply[2], reply[3:], limit-5, send)
}

// sendSegments sends a segmented Complex-ACK: the first segment alone,
// then windows of the size the requester's Segment-ACK grants.
func (s *Device) sendSegments(key bacnet.PendingKey, ch chan bacnet.APDU, id, svc byte, body []byte, size int, send func([]byte)) {
	defer func() {
		s.Mu.Lock()
		delete(s.Txs, key)
		s.Mu.Unlock()
	}()
	var parts [][]byte
	for len(body) > 0 {
		n := min(size, len(body))
		parts = append(parts, body[:n])
		body = body[n:]
	}
	s.Mu.Lock()
	window := s.Window
	s.Mu.Unlock()
	acked, next, actual := -1, 0, 1
	for acked < len(parts)-1 {
		for end := min(acked+1+actual, len(parts)); next < end; next++ {
			s.Mu.Lock()
			drop, stall := s.DropSeg == next, s.StallSeg == next
			if drop {
				s.DropSeg = -1
			}
			if stall {
				s.StallSeg = -1
			}
			s.Mu.Unlock()
			if stall {
				return
			}
			if !drop {
				send(bacnet.ComplexACKSegment(id, svc, byte(next), window, next < len(parts)-1, parts[next]))
			}
		}
		select {
		case a := <-ch:
			if a.Typ == bacnet.PDUAbort {
				return
			}
			j := -1
			for k := next - 1; k > acked; k-- {
				if byte(k) == a.Seq {
					j = k
					break
				}
			}
			if j < 0 && a.NAK && acked >= 0 && byte(acked) == a.Seq {
				j = acked
			}
			if j < 0 {
				continue
			}
			acked, next, actual = j, j+1, int(a.Window)
		case <-time.After(2 * time.Second):
			return
		}
	}
}

func (s *Device) Handle(a bacnet.APDU, from *net.UDPAddr) []byte {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	id, svc := a.InvokeID, a.Service
	switch svc {
	case bacnet.SvcReadProperty:
		s.Rp++
		o, p, rest, err := ObjProp(a.Data)
		if err != nil || len(rest) != 0 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		if o.Type == bacnet.ObjNotificationCl && p == bacnet.PropRecipientList {
			list, ok := s.Recipients[o.Instance]
			if !ok {
				return bacnet.ErrorAPDU(id, svc, 1, 31)
			}
			var e bacnet.Enc
			e.CtxObjectID(0, o)
			e.CtxUnsigned(1, uint64(p))
			e.Open(3)
			e.B = append(e.B, list...)
			e.Close(3)
			return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, e.B...)
		}
		v, class, code := s.prop(o, p)
		if class != 0 {
			return bacnet.ErrorAPDU(id, svc, class, code)
		}
		var e bacnet.Enc
		e.CtxObjectID(0, o)
		e.CtxUnsigned(1, uint64(p))
		e.Open(3)
		encodeSimValue(&e, v)
		e.Close(3)
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, e.B...)
	case bacnet.SvcReadPropertyMultiple:
		if s.RejectRPM {
			return []byte{bacnet.PDUReject << 4, id, 9} // unrecognized-service
		}
		s.RPM++
		var e bacnet.Enc
		b := a.Data
		for len(b) > 0 {
			t, rest, err := bacnet.ReadTag(b)
			if err != nil || t.Num != 0 || len(t.Data) != 4 {
				return []byte{bacnet.PDUReject << 4, id, 0}
			}
			o := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
			if t, rest, err = bacnet.ReadTag(rest); err != nil || !t.Open {
				return []byte{bacnet.PDUReject << 4, id, 0}
			}
			e.CtxObjectID(0, o)
			e.Open(1)
			for {
				t, rest, err = bacnet.ReadTag(rest)
				if err != nil {
					return []byte{bacnet.PDUReject << 4, id, 0}
				}
				if t.Close {
					break
				}
				p, _ := bacnet.BeUnsigned(t.Data)
				e.CtxUnsigned(2, p)
				v, class, code := s.prop(o, uint32(p))
				if class != 0 {
					e.Open(5)
					_ = e.App(bacnet.Enumerated(class))
					_ = e.App(bacnet.Enumerated(code))
					e.Close(5)
				} else {
					e.Open(4)
					encodeSimValue(&e, v)
					e.Close(4)
				}
			}
			e.Close(1)
			b = rest
		}
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, e.B...)
	case bacnet.SvcWriteProperty:
		o, p, rest, err := ObjProp(a.Data)
		if err != nil {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		t, rest, err := bacnet.ReadTag(rest)
		if err != nil || !t.Open || t.Num != 3 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		vals, rest, err := bacnet.ValuesUntilClose(rest, 3)
		if err != nil || len(vals) != 1 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		prio := 0
		if len(rest) > 0 {
			t, _, err := bacnet.ReadTag(rest)
			if err != nil || t.Num != 4 {
				return []byte{bacnet.PDUReject << 4, id, 0}
			}
			v, _ := bacnet.BeUnsigned(t.Data)
			prio = int(v)
		}
		obj := s.Objs[o]
		if obj == nil {
			return bacnet.ErrorAPDU(id, svc, 1, 31)
		}
		if p != bacnet.PropPresentValue || !simCommandable(o.Type) {
			return bacnet.ErrorAPDU(id, svc, 2, 40) // property, write-access-denied
		}
		if n, ok := vals[0].(uint64); ok && simIsMultiState(o.Type) && (n < 1 || n > obj.States) {
			return bacnet.ErrorAPDU(id, svc, 2, 37) // property, value-out-of-range
		}
		s.Writes = append(s.Writes, Write{o, vals[0], prio})
		slot := prio
		if slot == 0 {
			slot = 16
		}
		obj.prio[slot-1] = vals[0]
		go s.Notify(o)
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcSubscribeCOV:
		t, rest, _ := bacnet.ReadTag(a.Data)
		process, _ := bacnet.BeUnsigned(t.Data)
		t, rest, err := bacnet.ReadTag(rest)
		if err != nil || len(t.Data) != 4 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		o := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
		s.Subs[o] = from
		if s.subProc == nil {
			s.subProc = map[bacnet.ObjectID]uint32{}
			s.subLife = map[bacnet.ObjectID]uint32{}
		}
		// The notifications carry the subscriber's process identifier
		// and lifetime (clause 13.14).
		s.subProc[o] = uint32(process)
		delete(s.subLife, o)
		for len(rest) > 0 {
			c, r, err := bacnet.ReadTag(rest)
			if err != nil || !c.Context {
				break
			}
			switch c.Num {
			case 2:
				s.SubConf[o] = len(c.Data) == 1 && c.Data[0] == 1
			case 3:
				if v, err := bacnet.BeUnsigned(c.Data); err == nil {
					s.subLife[o] = uint32(v)
				}
			}
			rest = r
		}
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcDeviceCommControl, bacnet.SvcReinitializeDevice:
		s.Mgmt = append(s.Mgmt, a)
		if bytes.Contains(a.Data, []byte("wrong")) {
			return bacnet.ErrorAPDU(id, svc, 4, 26) // security, password-failure
		}
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcSubscribeCOVProperty:
		s.COVProp = append(s.COVProp, a)
		_, rest, _ := bacnet.ReadTag(a.Data)
		t, _, err := bacnet.ReadTag(rest)
		if err != nil || len(t.Data) != 4 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		s.Subs[bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))] = from
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcWritePropertyMulti:
		if s.RejectWPM {
			return []byte{bacnet.PDUReject << 4, id, bacnet.RejectUnrecognizedService}
		}
		s.WPM++
		b := a.Data
		for len(b) > 0 {
			t, rest, err := bacnet.ReadTag(b)
			if err != nil || len(t.Data) != 4 {
				return []byte{bacnet.PDUReject << 4, id, 0}
			}
			o := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
			if _, rest, err = bacnet.ReadTag(rest); err != nil { // open 1
				return []byte{bacnet.PDUReject << 4, id, 0}
			}
			for {
				t, r2, err := bacnet.ReadTag(rest)
				if err != nil {
					return []byte{bacnet.PDUReject << 4, id, 0}
				}
				if t.Close && t.Num == 1 {
					rest = r2
					break
				}
				_, r2, _ = bacnet.ReadTag(r2) // open 2
				vals, r3, err := bacnet.ValuesUntilClose(r2, 2)
				if err != nil || len(vals) != 1 {
					return []byte{bacnet.PDUReject << 4, id, 0}
				}
				prio := 0
				if p, r4, err := bacnet.ReadTag(r3); err == nil && p.Context && p.Num == 3 {
					v, _ := bacnet.BeUnsigned(p.Data)
					prio, r3 = int(v), r4
				}
				obj := s.Objs[o]
				if obj == nil || !simCommandable(o.Type) {
					var e bacnet.Enc
					e.Open(0)
					_ = e.App(bacnet.Enumerated(2))
					_ = e.App(bacnet.Enumerated(40))
					e.Close(0)
					e.Open(1)
					e.CtxObjectID(0, o)
					e.CtxUnsigned(1, bacnet.PropPresentValue)
					e.Close(1)
					return append([]byte{bacnet.PDUError << 4, id, svc}, e.B...)
				}
				s.Writes = append(s.Writes, Write{o, vals[0], prio})
				slot := prio
				if slot == 0 {
					slot = 16
				}
				obj.prio[slot-1] = vals[0]
				rest = r3
			}
			b = rest
		}
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcAtomicReadFile:
		t, rest, _ := bacnet.ReadTag(a.Data)
		f := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
		data, ok := s.Files[f.Instance]
		if !ok || f.Type != bacnet.ObjFile {
			return bacnet.ErrorAPDU(id, svc, 1, 31)
		}
		t, rest, _ = bacnet.ReadTag(rest)
		vals, _, _ := bacnet.ValuesUntilClose(rest, t.Num)
		start, _ := vals[0].(int64)
		n, _ := vals[1].(uint64)
		end := min(int(start)+int(n), len(data))
		var e bacnet.Enc
		_ = e.App(end == len(data))
		e.Open(0)
		_ = e.App(start)
		e.Octets(data[start:end])
		e.Close(0)
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, e.B...)
	case bacnet.SvcAtomicWriteFile:
		t, rest, _ := bacnet.ReadTag(a.Data)
		f := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
		t, rest, _ = bacnet.ReadTag(rest)
		vals, _, _ := bacnet.ValuesUntilClose(rest, t.Num)
		start, _ := vals[0].(int64)
		data, _ := vals[1].([]byte)
		cur := s.Files[f.Instance]
		if start < 0 {
			start = int64(len(cur))
		}
		for int64(len(cur)) < start+int64(len(data)) {
			cur = append(cur, 0)
		}
		copy(cur[start:], data)
		s.Files[f.Instance] = cur
		var e bacnet.Enc
		e.CtxUnsigned(0, uint64(start))
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, e.B...)
	case bacnet.SvcGetEventInformation:
		if s.EventInfo == nil {
			return []byte{bacnet.PDUReject << 4, id, bacnet.RejectUnrecognizedService}
		}
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, s.EventInfo...)
	case bacnet.SvcGetAlarmSummary:
		return append([]byte{bacnet.PDUComplexACK << 4, id, svc}, s.AlarmSum...)
	case bacnet.SvcAddListElement:
		o, p, rest, err := ObjProp(a.Data)
		if err != nil || p != bacnet.PropRecipientList || o.Type != bacnet.ObjNotificationCl {
			return bacnet.ErrorAPDU(id, svc, 2, 32)
		}
		t, rest, _ := bacnet.ReadTag(rest)
		raw, _, err := bacnet.RawConstructed(rest, t.Num)
		if err != nil {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		s.Recipients[o.Instance] = append(s.Recipients[o.Instance], raw...)
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcAcknowledgeAlarm:
		s.Acks = append(s.Acks, append([]byte(nil), a.Data...))
		return []byte{bacnet.PDUSimpleACK << 4, id, svc}
	case bacnet.SvcReadRange:
		return s.readRange(id, a.Data)
	}
	return []byte{bacnet.PDUReject << 4, id, 9}
}

func (s *Device) Notify(o bacnet.ObjectID) {
	s.Mu.Lock()
	to, conn, obj, conf := s.Subs[o], s.Conn, s.Objs[o], s.SubConf[o]
	if to == nil || conn == nil || obj == nil {
		s.Mu.Unlock()
		return
	}
	process, life := uint32(simCOVProcess), uint32(60)
	if p, ok := s.subProc[o]; ok {
		process = p
	}
	if l, ok := s.subLife[o]; ok {
		life = l
	}
	dev := bacnet.ObjectID{Type: 8, Instance: 1234}
	if s.Iam != nil {
		dev = s.Iam.Device
	}
	n := bacnet.COVNotification{Process: process, Device: dev, Object: o, Values: map[uint32][]any{
		bacnet.PropPresentValue: {obj.present(o.Type)},
		bacnet.PropStatusFlags:  {bacnet.BitString{Len: 4, Bits: append([]bool(nil), obj.Flags[:]...)}},
	}}
	s.Mu.Unlock()
	data, err := bacnet.EncodeCOVNotification(n, life)
	if err != nil {
		return
	}
	if conf {
		s.SendConfirmedCOV(data)
		return
	}
	_, _ = conn.WriteToUDP(s.Out(append([]byte{bacnet.PDUUnconfirmed << 4, bacnet.SvcUnconfirmedCOV}, data...)), to)
}

// SendConfirmedCOV sends a ConfirmedCOVNotification with the given body
// to the subscriber of any object (they are all the same IPE).
func (s *Device) SendConfirmedCOV(body []byte) {
	s.Mu.Lock()
	var to *net.UDPAddr
	for _, a := range s.Subs {
		to = a
	}
	conn := s.Conn
	s.covID++
	id := s.covID
	s.Mu.Unlock()
	if to == nil || conn == nil {
		return
	}
	req := append([]byte{bacnet.PDUConfirmed << 4, 5, id, bacnet.SvcConfirmedCOV}, body...)
	_, _ = conn.WriteToUDP(s.Out(req), to)
}

// ReadRange answers ReadRange on a trend log's log-buffer.
func (s *Device) readRange(id byte, b []byte) []byte {
	o, p, rest, err := ObjProp(b)
	if err != nil {
		return []byte{bacnet.PDUReject << 4, id, 0}
	}
	obj := s.Objs[o]
	if obj == nil {
		return bacnet.ErrorAPDU(id, bacnet.SvcReadRange, 1, 31)
	}
	if o.Type != bacnet.TrendLog || p != bacnet.PropLogBuffer {
		return bacnet.ErrorAPDU(id, bacnet.SvcReadRange, 2, 32)
	}
	r := bacnet.ReadRange{Kind: bacnet.RangeAll}
	var vals []any
	if len(rest) > 0 {
		t, rest2, err := bacnet.ReadTag(rest)
		if err != nil || !t.Open {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		r.Kind = t.Num
		if vals, _, err = bacnet.ValuesUntilClose(rest2, t.Num); err != nil || len(vals) < 2 {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		c, _ := vals[len(vals)-1].(int64)
		r.Count = int32(c)
	}
	if r.Kind == bacnet.RangeBySequence && s.RejectSeq {
		return bacnet.ErrorAPDU(id, bacnet.SvcReadRange, 5, 45) // services, optional-functionality-not-supported
	}
	logs := obj.log
	lo, hi := 0, len(logs) // the records returned: logs[lo:hi]
	pick := func(match func(simRec) bool) {
		var idx []int
		for i, rec := range logs {
			if match(rec) {
				idx = append(idx, i)
			}
		}
		if len(idx) == 0 {
			lo, hi = 0, 0
			return
		}
		if r.Count > 0 {
			lo, hi = idx[0], min(idx[0]+int(r.Count), idx[len(idx)-1]+1)
		} else {
			hi = idx[len(idx)-1] + 1
			lo = max(idx[0], hi+int(r.Count))
		}
	}
	switch r.Kind {
	case bacnet.RangeByPosition:
		ref, _ := vals[0].(uint64)
		pick(func(x simRec) bool { return false })
		i := int(ref) - 1
		if r.Count > 0 {
			lo, hi = max(i, 0), min(i+int(r.Count), len(logs))
		} else {
			lo, hi = max(i+int(r.Count)+1, 0), min(i+1, len(logs))
		}
	case bacnet.RangeBySequence:
		// Records follow in sequence (wrapping after 2^32-1): find ref,
		// or the first record after it.
		ref, _ := vals[0].(uint64)
		r.Ref = ref
		i := -1
		for k, x := range logs {
			if x.seq == ref || r.Count > 0 && uint32(x.seq-ref) < 1<<31 && i < 0 {
				i = k
				if x.seq == ref {
					break
				}
			}
		}
		switch {
		case i < 0:
			lo, hi = 0, 0
		case r.Count > 0:
			lo, hi = i, min(i+int(r.Count), len(logs))
		default:
			lo, hi = max(i+int(r.Count)+1, 0), i+1
		}
	case bacnet.RangeByTime:
		d, _ := vals[0].([]byte)
		tm, _ := vals[1].([]byte)
		at, err := bacnet.DateTimeFrom(d, tm, time.UTC)
		if err != nil {
			return []byte{bacnet.PDUReject << 4, id, 0}
		}
		r.At = at
		pick(func(x simRec) bool {
			if r.Count > 0 {
				return x.rec.At.After(at)
			}
			return x.rec.At.Before(at)
		})
	}
	if lo > hi {
		lo = hi
	}
	s.Ranges = append(s.Ranges, r)
	var e bacnet.Enc
	e.CtxObjectID(0, o)
	e.CtxUnsigned(1, uint64(p))
	more := r.Count > 0 && hi < len(logs) || r.Count < 0 && lo > 0
	fl := bacnet.BitContent(bacnet.BitString{Len: 3, Bits: []bool{hi > lo && lo == 0, hi > lo && hi == len(logs), more}})
	e.Header(3, true, len(fl))
	e.B = append(e.B, fl...)
	e.CtxUnsigned(4, uint64(hi-lo))
	e.Open(5)
	for _, rec := range logs[lo:hi] {
		b, err := bacnet.EncodeLogRecord(rec.rec, time.UTC)
		if err != nil {
			return []byte{bacnet.PDUAbort<<4 | 1, id, 0}
		}
		e.B = append(e.B, b...)
	}
	e.Close(5)
	if r.Kind != bacnet.RangeByPosition && r.Kind != bacnet.RangeAll && hi > lo {
		e.CtxUnsigned(6, logs[lo].seq)
	}
	return append([]byte{bacnet.PDUComplexACK << 4, id, bacnet.SvcReadRange}, e.B...)
}

func ObjProp(b []byte) (bacnet.ObjectID, uint32, []byte, error) {
	t, rest, err := bacnet.ReadTag(b)
	if err != nil || !t.Context || t.Num != 0 || len(t.Data) != 4 {
		return bacnet.ObjectID{}, 0, nil, errors.New("object identifier expected")
	}
	o := bacnet.ObjectIDFrom(binary.BigEndian.Uint32(t.Data))
	t, rest, err = bacnet.ReadTag(rest)
	if err != nil || !t.Context || t.Num != 1 {
		return bacnet.ObjectID{}, 0, nil, errors.New("property expected")
	}
	p, _ := bacnet.BeUnsigned(t.Data)
	return o, uint32(p), rest, nil
}

// WhoIsIncludes reads a Who-Is range (context tags 0 and 1, unsigned) and
// says whether instance is in it; no range includes every device.
func WhoIsIncludes(b []byte, instance uint32) bool {
	var v [2]uint64
	for i := 0; i < 2; i++ {
		if len(b) == 0 {
			return i == 0
		}
		n := int(b[0] & 7)
		if len(b) < 1+n || b[0]>>4 != byte(i) {
			return false
		}
		for _, x := range b[1 : 1+n] {
			v[i] = v[i]<<8 | uint64(x)
		}
		b = b[1+n:]
	}
	return uint64(instance) >= v[0] && uint64(instance) <= v[1]
}

func (s *Device) logMax() int {
	if s.LogCap > 0 {
		return s.LogCap
	}
	return 50
}

// simCOVProcess is the subscriber process identifier the simulator puts
// in the COV notifications it sends (SigmaM2M's IPE subscribes with 1).
const simCOVProcess = 1

func simIsInput(t uint16) bool {
	return t == bacnet.AnalogInput || t == bacnet.BinaryInput || t == bacnet.MultiStateInput
}

func simIsMultiState(t uint16) bool {
	return t == bacnet.MultiStateInput || t == bacnet.MultiStateOutput || t == bacnet.MultiStateValue
}

// simCommandable reports whether the simulator accepts writes to
// present-value of the type: analog, binary and multistate outputs and
// Values.
func simCommandable(t uint16) bool {
	switch t {
	case bacnet.AnalogOutput, bacnet.AnalogValue, bacnet.BinaryOutput, bacnet.BinaryValue, bacnet.MultiStateOutput, bacnet.MultiStateValue:
		return true
	}
	return false
}

// LogBurst adds n records at once (between two polls).
func (s *Device) LogBurst(o bacnet.ObjectID, start time.Time, from, n int) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	obj := s.Objs[o]
	for i := from; i < from+n; i++ {
		obj.Total = bacnet.SeqNext(obj.Total)
		obj.log = append(obj.log, simRec{obj.Total, bacnet.LogRecord{At: start.Add(time.Duration(i) * time.Minute), Kind: "unsigned", Value: uint64(i)}})
	}
	if len(obj.log) > s.logMax() {
		obj.log = obj.log[len(obj.log)-s.logMax():]
	}
}
