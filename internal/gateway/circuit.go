package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Circuit states.
const (
	stateClosed = "closed"
	stateOpen   = "open"
	stateHalf   = "half"
)

// cbFailWindow is how long a failure keeps counting towards opening a circuit.
// Without it a couple of errors from last week would still be on the tally
// today, and one more would trip a circuit over nothing.
const cbFailWindow = 30 * time.Second

// breaker decides whether an upstream should be spared further traffic.
//
// Three states, the usual ones: closed passes everything, open refuses
// everything, and half lets exactly one probe through. Half is the point of the
// whole thing — without it an open circuit expires by dumping the entire
// traffic spike onto an upstream that may not have recovered, which usually
// re-opens it immediately and settles into a flap.
type breaker interface {
	// allow reports whether this request may go through. It is not a read-only
	// query: reaching a circuit whose open period has elapsed is what promotes
	// it to half and spends the probe.
	allow(ctx context.Context, upstream string) bool
	success(ctx context.Context, upstream string)
	failure(ctx context.Context, upstream string)
	// state names the current state for the console. Unlike allow it never
	// advances the machine, so polling the dashboard cannot spend a probe.
	state(ctx context.Context, upstream string) string
	// closeOnWitnessedSuccess closes an open or half-open circuit because an
	// out-of-band health probe just watched the upstream answer — evidence
	// that did not cost a user request. Returns whether it closed anything.
	// A closed circuit is left untouched, so the failure tally keeps its
	// meaning for real traffic.
	closeOnWitnessedSuccess(ctx context.Context, upstream string) bool
}

// newBreaker picks the Redis-backed implementation when a client is available
// and the in-process one otherwise. Both behave identically; the difference is
// only whether several replicas share the same failure history.
func newBreaker(rdb *redis.Client, threshold int, openFor, probeFor time.Duration) breaker {
	if rdb == nil {
		return newMemBreaker(threshold, openFor, probeFor)
	}
	return &redisBreaker{rdb: rdb, threshold: threshold, openFor: openFor, probeFor: probeFor}
}

// ---------------------------------------------------------------- in-process

// memBreaker keeps circuit state in the process. Correct for a single replica,
// and what tests drive the state machine through without a Redis server.
type memBreaker struct {
	mu        sync.Mutex
	threshold int
	openFor   time.Duration
	probeFor  time.Duration
	// window is how long a failure keeps counting. A field rather than the
	// package constant so tests can age a tally out without sleeping 30s.
	window   time.Duration
	circuits map[string]*memCircuit
}

type memCircuit struct {
	state     string
	fails     int
	until     time.Time // end of the open period, or of the probe's grace
	failUntil time.Time // when the failure tally is forgotten
}

func newMemBreaker(threshold int, openFor, probeFor time.Duration) *memBreaker {
	return &memBreaker{
		threshold: threshold,
		openFor:   openFor,
		probeFor:  probeFor,
		window:    cbFailWindow,
		circuits:  map[string]*memCircuit{},
	}
}

func (b *memBreaker) get(up string) *memCircuit {
	c := b.circuits[up]
	if c == nil {
		c = &memCircuit{state: stateClosed}
		b.circuits[up] = c
	}
	return c
}

func (b *memBreaker) allow(ctx context.Context, up string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.get(up)
	now := time.Now()
	switch c.state {
	case stateOpen:
		if now.Before(c.until) {
			return false
		}
		// The open period is over. Let one request through as the probe.
		c.state = stateHalf
		c.until = now.Add(b.probeFor)
		return true
	case stateHalf:
		if now.Before(c.until) {
			return false // a probe is already in flight
		}
		// The probe never reported back. Allow another rather than wedging
		// the circuit half-open forever.
		c.until = now.Add(b.probeFor)
		return true
	}
	return true
}

func (b *memBreaker) failure(ctx context.Context, up string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.get(up)
	now := time.Now()
	if c.state == stateHalf {
		// The probe failed: the upstream is not back yet, start over.
		b.trip(c, now)
		return
	}
	if now.After(c.failUntil) {
		c.fails = 0
	}
	c.fails++
	c.failUntil = now.Add(b.window)
	if c.fails >= b.threshold {
		b.trip(c, now)
	}
}

func (b *memBreaker) trip(c *memCircuit, now time.Time) {
	c.state = stateOpen
	c.until = now.Add(b.openFor)
}

// success deliberately does not clear the failure tally. Zeroing it on every
// good response meant an upstream failing half the time never accumulated
// enough failures to trip at all — each success wiped the count, so the breaker
// sat idle through precisely the partial outage it exists to catch. A success
// now cancels out exactly one failure.
func (b *memBreaker) success(ctx context.Context, up string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.get(up)
	if c.state == stateHalf {
		// The probe came back healthy, so the upstream really is back.
		c.state = stateClosed
		c.fails = 0
		c.until = time.Time{}
		c.failUntil = time.Time{}
		return
	}
	if c.fails > 0 {
		c.fails--
	}
}

func (b *memBreaker) state(ctx context.Context, up string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.get(up)
	if c.state == stateOpen && !time.Now().Before(c.until) {
		// Past its open period but not yet probed.
		return stateHalf
	}
	return c.state
}

func (b *memBreaker) closeOnWitnessedSuccess(ctx context.Context, up string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	c := b.get(up)
	if c.state == stateClosed {
		return false
	}
	c.state = stateClosed
	c.fails = 0
	c.until = time.Time{}
	c.failUntil = time.Time{}
	return true
}

// --------------------------------------------------------------------- redis

// redisBreaker keeps circuit state in Redis so every replica sees the same
// history. Each transition runs as a Lua script: allow-then-probe has to be
// atomic, or a dozen concurrent requests would each hand out their own probe
// and half-open would be no different from closed.
type redisBreaker struct {
	rdb       *redis.Client
	threshold int
	openFor   time.Duration
	probeFor  time.Duration
}

func (b *redisBreaker) key(up string) string { return "cb:" + up }

// cbAllowScript hands out at most one probe per probe window. Returns 1 to
// allow, 0 to refuse.
//
// KEYS[1] = circuit hash. ARGV: now_ms, open_ms, probe_ms.
var cbAllowScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local st = redis.call('HGET', KEYS[1], 'state')

if st == 'open' then
  if now < tonumber(redis.call('HGET', KEYS[1], 'until') or '0') then
    return 0
  end
  redis.call('HSET', KEYS[1], 'state', 'half', 'until', now + tonumber(ARGV[3]))
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) + 1000)
  return 1
end

if st == 'half' then
  if now < tonumber(redis.call('HGET', KEYS[1], 'until') or '0') then
    return 0
  end
  redis.call('HSET', KEYS[1], 'until', now + tonumber(ARGV[3]))
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) + 1000)
  return 1
end

return 1
`)

// cbRecordScript folds one outcome into the circuit. Returns the new state.
//
// KEYS[1] = circuit hash. ARGV: ok(1|0), now_ms, threshold, open_ms, window_ms.
var cbRecordScript = redis.NewScript(`
local ok = ARGV[1] == '1'
local now = tonumber(ARGV[2])
local st = redis.call('HGET', KEYS[1], 'state')

if ok then
  if st == 'half' then
    redis.call('DEL', KEYS[1])
    return 'closed'
  end
  local f = tonumber(redis.call('HGET', KEYS[1], 'fails') or '0')
  if f > 0 then
    redis.call('HSET', KEYS[1], 'fails', f - 1)
  end
  return 'closed'
end

if st == 'half' then
  redis.call('HSET', KEYS[1], 'state', 'open', 'until', now + tonumber(ARGV[4]))
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + 1000)
  return 'open'
end

local f = redis.call('HINCRBY', KEYS[1], 'fails', 1)
-- Refreshed on every failure, so the tally ages out on its own once the
-- upstream stops failing.
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[5]))

if f >= tonumber(ARGV[3]) then
  redis.call('HSET', KEYS[1], 'state', 'open', 'until', now + tonumber(ARGV[4]))
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[4]) + 1000)
  return 'open'
end

return 'closed'
`)

// cbStateScript reports the state without advancing it.
//
// KEYS[1] = circuit hash. ARGV: now_ms.
var cbStateScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local st = redis.call('HGET', KEYS[1], 'state')
if st == 'open' and now >= tonumber(redis.call('HGET', KEYS[1], 'until') or '0') then
  return 'half'
end
return st or 'closed'
`)

// cbCloseScript closes a circuit that is standing open (in either sense:
// inside its cool-down or awaiting a probe) because a health probe just
// watched the upstream answer. It returns 1 when a circuit was closed, 0
// when there was nothing to close — so the caller can log only real
// recoveries, not every healthy sweep.
//
// KEYS[1] = circuit hash.
var cbCloseScript = redis.NewScript(`
local st = redis.call('HGET', KEYS[1], 'state')
if st == 'open' or st == 'half' then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`)

func (b *redisBreaker) allow(ctx context.Context, up string) bool {
	n, err := cbAllowScript.Run(ctx, b.rdb, []string{b.key(up)},
		time.Now().UnixMilli(),
		b.openFor.Milliseconds(),
		b.probeFor.Milliseconds(),
	).Int()
	if err != nil {
		// A breaker is a safety net, not a gate: if its own state cannot be
		// read, refusing traffic would turn a Redis blip into a total outage.
		return true
	}
	return n == 1
}

func (b *redisBreaker) failure(ctx context.Context, up string) {
	_, _ = cbRecordScript.Run(ctx, b.rdb, []string{b.key(up)},
		0,
		time.Now().UnixMilli(),
		b.threshold,
		b.openFor.Milliseconds(),
		cbFailWindow.Milliseconds(),
	).Result()
}

func (b *redisBreaker) success(ctx context.Context, up string) {
	_, _ = cbRecordScript.Run(ctx, b.rdb, []string{b.key(up)},
		1,
		time.Now().UnixMilli(),
		b.threshold,
		b.openFor.Milliseconds(),
		cbFailWindow.Milliseconds(),
	).Result()
}

func (b *redisBreaker) state(ctx context.Context, up string) string {
	v, err := cbStateScript.Run(ctx, b.rdb, []string{b.key(up)},
		time.Now().UnixMilli(),
	).Text()
	if err != nil {
		return stateClosed
	}
	return v
}

func (b *redisBreaker) closeOnWitnessedSuccess(ctx context.Context, up string) bool {
	n, err := cbCloseScript.Run(ctx, b.rdb, []string{b.key(up)}).Int()
	if err != nil {
		return false
	}
	return n == 1
}

// ------------------------------------------------------------------- server

// circuit returns the breaker, building the default one on first use. Tests
// inject their own by setting Server.Breaker.
func (s *Server) circuit() breaker {
	if s.Breaker != nil {
		return s.Breaker
	}
	s.breakerOnce.Do(func() {
		s.breakerInst = newBreaker(s.RDB, s.cbThreshold(), s.cbOpenFor(), s.cbProbeFor())
	})
	return s.breakerInst
}

func (s *Server) cbThreshold() int {
	if s.Cfg == nil || s.Cfg.CBFailThreshold <= 0 {
		return 5
	}
	return s.Cfg.CBFailThreshold
}

func (s *Server) cbOpenFor() time.Duration {
	if s.Cfg == nil || s.Cfg.CBOpenSec <= 0 {
		return 10 * time.Second
	}
	return time.Duration(s.Cfg.CBOpenSec) * time.Second
}

func (s *Server) cbProbeFor() time.Duration {
	if s.Cfg == nil || s.Cfg.CBProbeSec <= 0 {
		return 5 * time.Second
	}
	return time.Duration(s.Cfg.CBProbeSec) * time.Second
}

// isCircuitOpen reports whether a channel must be skipped. It spends the
// half-open probe when the open period has elapsed, so it may only be called
// once per attempt — use circuitState for read-only questions such as
// ordering candidates or rendering the console.
func (s *Server) isCircuitOpen(ctx context.Context, upstream string) bool {
	return !s.circuit().allow(ctx, upstream)
}

// circuitState is the read-only view used by the console and by channel
// ordering. It never advances the machine, so asking cannot spend a probe.
func (s *Server) circuitState(ctx context.Context, upstream string) string {
	return s.circuit().state(ctx, upstream)
}

func (s *Server) recordFailure(ctx context.Context, upstream string) {
	s.circuit().failure(ctx, upstream)
}

func (s *Server) recordSuccess(ctx context.Context, upstream string) {
	s.circuit().success(ctx, upstream)
}

// noteOutcome feeds one attempt's result to the breaker, unless this route has
// opted out of circuit breaking. The opt-out has to be honoured on the way in
// as well as on the way out: recording failures for a route that never consults
// the breaker only builds up state nobody reads, and quietly shares that
// upstream's failure history with every other route using it.
func (s *Server) noteOutcome(ctx context.Context, route *Route, ch Channel, ok bool) {
	if !route.CBEnabled {
		return
	}
	if ok {
		s.recordSuccess(ctx, ch.upstreamKey(route.Name))
		return
	}
	s.recordFailure(ctx, ch.upstreamKey(route.Name))
}
