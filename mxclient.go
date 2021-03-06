// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// DNS client: see RFC 1035.
// Has to be linked into package net for Dial.

// TODO(rsc):
//	Check periodically whether /etc/resolv.conf has changed.
//	Could potentially handle many outstanding lookups faster.
//	Could have a small cache.
//	Random UDP source port (net.Dial should do that for us).
//	Random request IDs.

package main

import (
	"once"
	"os"
	"rand"
	"time"
	"net"
)

// DNSError represents a DNS lookup error.
type DNSError struct {
	Error     string // description of the error
	Name      string // name looked for
	Server    string // server used
	IsTimeout bool
}

func (e *DNSError) String() string {
	s := "lookup " + e.Name
	if e.Server != "" {
		s += " on " + e.Server
	}
	s += ": " + e.Error
	return s
}

func (e *DNSError) Timeout() bool   { return e.IsTimeout }
func (e *DNSError) Temporary() bool { return e.IsTimeout }

const noSuchHost = "no such host"

// Send a request on the connection and hope for a reply.
// Up to cfg.attempts attempts.
func exchange(cfg *dnsConfig, c net.Conn, name string) (*dnsMsg, os.Error) {
	if len(name) >= 256 {
		return nil, &DNSError{Error: "name too long", Name: name}
	}
	out := new(dnsMsg)
	out.id = uint16(rand.Int()) ^ uint16(time.Nanoseconds())
	out.question = []dnsQuestion{
		dnsQuestion{name, dnsTypeMX, dnsClassINET},
	}
	out.recursion_desired = true
	msg, ok := out.Pack()
	if !ok {
		return nil, &DNSError{Error: "internal error - cannot pack message", Name: name}
	}

	for attempt := 0; attempt < cfg.attempts; attempt++ {
		n, err := c.Write(msg)
		if err != nil {
			return nil, err
		}

		c.SetReadTimeout(int64(cfg.timeout) * 1e9) // nanoseconds

		buf := make([]byte, 2000) // More than enough.
		n, err = c.Read(buf)
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				continue
			}
			return nil, err
		}
		buf = buf[0:n]
		in := new(dnsMsg)
		if !in.Unpack(buf) || in.id != out.id {
			continue
		}
		return in, nil
	}
	var server string
	if a := c.RemoteAddr(); a != nil {
		server = a.String()
	}
	return nil, &DNSError{Error: "no answer from server", Name: name, Server: server, IsTimeout: true}
}


// Find answer for name in dns message.
// On return, if err == nil, addrs != nil.
func answer(name, server string, dns *dnsMsg) (addrs []string, prefs[]uint16, err os.Error) {
	addrs = make([]string, 0, len(dns.answer))
	prefs = make([]uint16, 0, len(dns.answer))

	if dns.rcode == dnsRcodeNameError && dns.recursion_available {
		return nil, nil, &DNSError{Error: noSuchHost, Name: name}
	}
	if dns.rcode != dnsRcodeSuccess {
		// None of the error codes make sense
		// for the query we sent.  If we didn't get
		// a name error and we didn't get success,
		// the server is behaving incorrectly.
		return nil, nil, &DNSError{Error: "server misbehaving", Name: name, Server: server}
	}

	for mxloop := 0; mxloop < 10; mxloop++ {
		addrs = addrs[0:0]
		prefs = prefs[0:0]
		for i := 0; i < len(dns.answer); i++ {
			rr := dns.answer[i]
			h := rr.Header()
			if h.Class == dnsClassINET && h.Name == name {
				switch h.Rrtype {
					case dnsTypeMX:
						n := len(addrs)
						addrs = addrs[0 : n+1]
						addrs[n] = rr.(*dnsRR_MX).Mx
						prefs = prefs[0 : n+1]
						prefs[n] = rr.(*dnsRR_MX).Pref
						break
				}
			}
		}
		if len(addrs) == 0 {
			return nil, nil, &DNSError{Error: noSuchHost, Name: name, Server: server}
		}
		return addrs, prefs, nil
	}

	return nil, nil, &DNSError{Error: "too many redirects", Name: name, Server: server}
}

// Do a lookup for a single name, which must be rooted
// (otherwise answer will not find the answers).
func tryOneName(cfg *dnsConfig, name string) (addrs []string, prefs []uint16, err os.Error) {
	if len(cfg.servers) == 0 {
		return nil, nil, &DNSError{Error: "no DNS servers", Name: name}
	}
	for i := 0; i < len(cfg.servers); i++ {
		// Calling Dial here is scary -- we have to be sure
		// not to dial a name that will require a DNS lookup,
		// or Dial will call back here to translate it.
		// The DNS config parser has already checked that
		// all the cfg.servers[i] are IP addresses, which
		// Dial will use without a DNS lookup.
		server := cfg.servers[i] + ":53"
		c, cerr := net.Dial("udp", "", server)
		if cerr != nil {
			err = cerr
			continue
		}
		msg, merr := exchange(cfg, c, name)
		c.Close()
		if merr != nil {
			err = merr
			continue
		}
		addrs, prefs, err = answer(name, server, msg)
		if err == nil || err.(*DNSError).Error == noSuchHost {
			break
		}
	}
	return
}

var cfg *dnsConfig
var dnserr os.Error

func loadConfig() { cfg, dnserr = dnsReadConfig() }

func isDomainName(s string) bool {
	// Requirements on DNS name:
	//	* must not be empty.
	//	* must be alphanumeric plus - and .
	//	* each of the dot-separated elements must begin
	//	  and end with a letter or digit.
	//	  RFC 1035 required the element to begin with a letter,
	//	  but RFC 3696 says this has been relaxed to allow digits too.
	//	  still, there must be a letter somewhere in the entire name.
	if len(s) == 0 {
		return false
	}
	if s[len(s)-1] != '.' { // simplify checking loop: make name end in dot
		s += "."
	}

	last := byte('.')
	ok := false // ok once we've seen a letter
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z':
			ok = true
		case '0' <= c && c <= '9':
			// fine
		case c == '-':
			// byte before dash cannot be dot
			if last == '.' {
				return false
			}
		case c == '.':
			// byte before dot cannot be dot, dash
			if last == '.' || last == '-' {
				return false
			}
		}
		last = c
	}

	return ok
}

// LookupMX looks for name using the local hosts file and DNS resolver.
// It returns the canonical name for the host and an array of that
// host's addresses.
func LookupMX(name string) (addrs []string, prefs []uint16, err os.Error) {
	if !isDomainName(name) {
		return nil, nil, &DNSError{Error: "invalid domain name", Name: name}
	}
	once.Do(loadConfig)
	if dnserr != nil || cfg == nil {
		err = dnserr
		return
	}

	// If name is rooted (trailing dot) or has enough dots,
	// try it by itself first.
	rooted := len(name) > 0 && name[len(name)-1] == '.'
	if rooted || count(name, '.') >= cfg.ndots {
		rname := name
		if !rooted {
			rname += "."
		}
		// Can try as ordinary name.
		addrs, prefs, err = tryOneName(cfg, rname)
		if err == nil {
			return
		}
	}
	if rooted {
		return
	}

	// Otherwise, try suffixes.
	for i := 0; i < len(cfg.search); i++ {
		rname := name + "." + cfg.search[i]
		if rname[len(rname)-1] != '.' {
			rname += "."
		}
		addrs, prefs, err = tryOneName(cfg, rname)
		if err == nil {
			return
		}
	}

	// Last ditch effort: try unsuffixed.
	rname := name
	if !rooted {
		rname += "."
	}
	addrs, prefs, err = tryOneName(cfg, rname)
	if err == nil {
		return
	}
	return
}
