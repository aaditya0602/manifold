package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fixture reads a testdata file. Fixtures are real YAML rather than Go string
// literals so that indentation mistakes surface the same way they would in a
// user's config file.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return data
}

func parseFixture(t *testing.T, name string) *Config {
	t.Helper()
	c, err := Parse(fixture(t, name))
	if err != nil {
		t.Fatalf("Parse(%s) = error %v, want success", name, err)
	}
	return c
}

// joinedCount reports how many individual problems an errors.Join error holds.
func joinedCount(err error) int {
	var multi interface{ Unwrap() []error }
	if errors.As(err, &multi) {
		return len(multi.Unwrap())
	}
	if err == nil {
		return 0
	}
	return 1
}

func TestParseValidConfigs(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"valid_minimal.yaml", "valid_full.yaml", "explicit_zeros.yaml"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			parseFixture(t, name)
		})
	}
}

// TestDefaultsForOmittedKeys proves that a config carrying only the required
// fields comes out of Parse fully populated: no component downstream should
// ever need a fallback of its own.
func TestDefaultsForOmittedKeys(t *testing.T) {
	t.Parallel()
	c := parseFixture(t, "valid_minimal.yaml")

	p := c.Pools[0]
	a, pa, b, tr := p.Health.Active, p.Health.Passive, p.Breaker, p.Transport

	checks := []struct {
		field string
		got   any
		want  any
	}{
		{"admin", c.Admin, ":9090"},
		{"server.read_header_timeout", c.Server.ReadHeaderTimeout.D(), 5 * time.Second},
		{"server.read_timeout", c.Server.ReadTimeout.D(), 30 * time.Second},
		{"server.write_timeout", c.Server.WriteTimeout.D(), 30 * time.Second},
		{"server.idle_timeout", c.Server.IdleTimeout.D(), 90 * time.Second},
		{"server.drain_timeout", c.Server.DrainTimeout.D(), 15 * time.Second},

		{"pools[0].strategy", p.Strategy, StrategyRoundRobin},
		{"pools[0].upstreams[0].weight", p.Upstreams[0].Weight, 1},

		{"health.active.enabled", a.Enabled, true},
		{"health.active.path", a.Path, "/healthz"},
		{"health.active.method", a.Method, "GET"},
		{"health.active.interval", a.Interval.D(), 2 * time.Second},
		{"health.active.timeout", a.Timeout.D(), 500 * time.Millisecond},
		{"health.active.healthy_threshold", a.HealthyThreshold, 2},
		{"health.active.unhealthy_threshold", a.UnhealthyThreshold, 3},

		{"health.passive.enabled", pa.Enabled, true},
		{"health.passive.window", pa.Window.D(), 10 * time.Second},
		{"health.passive.min_requests", pa.MinRequests, 20},
		{"health.passive.error_rate", pa.ErrorRate, 0.5},
		{"health.passive.eject_for", pa.EjectFor.D(), 30 * time.Second},

		{"breaker.enabled", b.Enabled, true},
		{"breaker.failure_threshold", b.FailureThreshold, 5},
		{"breaker.open_for", b.OpenFor.D(), 5 * time.Second},
		{"breaker.half_open_probes", b.HalfOpenProbes, 1},

		{"limits.max_in_flight", p.Limits.MaxInFlight, 1024},
		{"limits.queue_timeout", p.Limits.QueueTimeout.D(), time.Duration(0)},

		{"retry.max_attempts", p.Retry.MaxAttempts, 2},
		{"retry.idempotent_only", p.Retry.IdempotentOnly, true},
		{"retry.per_try_timeout", p.Retry.PerTryTimeout.D(), time.Duration(0)},

		{"transport.max_idle_conns", tr.MaxIdleConns, 2048},
		{"transport.max_idle_conns_per_host", tr.MaxIdleConnsPerHost, 256},
		{"transport.max_conns_per_host", tr.MaxConnsPerHost, 0},
		{"transport.idle_conn_timeout", tr.IdleConnTimeout.D(), 90 * time.Second},
		{"transport.dial_timeout", tr.DialTimeout.D(), 2 * time.Second},
		{"transport.response_header_timeout", tr.ResponseHeaderTimeout.D(), 10 * time.Second},
		{"transport.disable_compression", tr.DisableCompression, true},

		{"routes[0].match.path_prefix", c.Routes[0].Match.PathPrefix, "/"},
	}

	for _, tc := range checks {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.field, tc.got, tc.want)
		}
	}
}

// TestExplicitZeroAndFalseSurvive is the other half of presence-aware
// defaulting, and the reason defaults are seeded before decoding rather than
// patched in afterwards. A post-decode pass would silently replace every value
// below with its default.
func TestExplicitZeroAndFalseSurvive(t *testing.T) {
	t.Parallel()
	c := parseFixture(t, "explicit_zeros.yaml")

	p := c.Pools[0]
	checks := []struct {
		field string
		got   any
		want  any
	}{
		{"health.active.enabled", p.Health.Active.Enabled, false},
		{"health.passive.enabled", p.Health.Passive.Enabled, false},
		{"breaker.enabled", p.Breaker.Enabled, false},
		{"retry.idempotent_only", p.Retry.IdempotentOnly, false},
		{"transport.disable_compression", p.Transport.DisableCompression, false},
		{"limits.max_in_flight", p.Limits.MaxInFlight, 0},
		{"transport.max_idle_conns", p.Transport.MaxIdleConns, 0},
	}
	for _, tc := range checks {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v (explicit value was overwritten by a default)", tc.field, tc.got, tc.want)
		}
	}

	// Fields left out of the same document must still be defaulted: seeding
	// must not be all-or-nothing per struct.
	if got := p.Health.Active.Path; got != "/healthz" {
		t.Errorf("health.active.path = %q, want the default /healthz", got)
	}
	if got := p.Transport.MaxIdleConnsPerHost; got != 256 {
		t.Errorf("transport.max_idle_conns_per_host = %d, want the default 256", got)
	}
	if got := p.Limits.QueueTimeout.D(); got != 0 {
		t.Errorf("limits.queue_timeout = %v, want 0", got)
	}
}

// TestExplicitValuesSurviveDefaulting guards non-zero overrides too.
func TestExplicitValuesSurviveDefaulting(t *testing.T) {
	t.Parallel()
	c := parseFixture(t, "valid_full.yaml")

	if got := c.Pools[0].Upstreams[1].Weight; got != 3 {
		t.Errorf("pools[0].upstreams[1].weight = %d, want 3", got)
	}
	if got := c.Pools[0].Retry.MaxAttempts; got != 2 {
		t.Errorf("pools[0].retry.max_attempts = %d, want 2", got)
	}
	if got := c.Pools[1].Strategy; got != StrategyConsistentHash {
		t.Errorf("pools[1].strategy = %q, want %q", got, StrategyConsistentHash)
	}
	if got := c.Routes[0].Match.PathPrefix; got != "/v1" {
		t.Errorf("routes[0].match.path_prefix = %q, want /v1", got)
	}
	if got := c.Admin; got != "127.0.0.1:9090" {
		t.Errorf("admin = %q, want 127.0.0.1:9090", got)
	}
	// Pool 1 and 2 omit every policy block; seeding is per element, not
	// inherited from the first pool.
	if got := c.Pools[2].Limits.MaxInFlight; got != 1024 {
		t.Errorf("pools[2].limits.max_in_flight = %d, want the default 1024", got)
	}
}

func TestValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		file    string
		want    []string // substrings that must all appear
		wantMin int      // minimum number of joined problems
	}{
		{
			name: "listen missing",
			file: "missing_listen.yaml",
			want: []string{"listen: required"},
		},
		{
			name: "listen and admin not host:port",
			file: "bad_listeners.yaml",
			want: []string{
				`listen: not a host:port address: "8080"`,
				`admin: not a host:port address: "not-an-address"`,
			},
			wantMin: 2,
		},
		{
			name: "no pools",
			file: "no_pools.yaml",
			want: []string{"pools: at least one pool is required"},
		},
		{
			name: "pool names empty and duplicated",
			file: "bad_pool_names.yaml",
			want: []string{
				"pools[0].name: required",
				`pools[2].name: duplicate pool name "api", already declared at pools[1]`,
			},
			wantMin: 2,
		},
		{
			name: "unknown strategy",
			file: "bad_strategy.yaml",
			want: []string{`pools[0].strategy: must be one of "round_robin", "least_conn", "consistent_hash", got "random"`},
		},
		{
			name: "pool with no upstreams",
			file: "no_upstreams.yaml",
			want: []string{"pools[0].upstreams: at least one upstream is required"},
		},
		{
			name: "upstream url problems",
			file: "bad_upstreams.yaml",
			want: []string{
				`pools[0].upstreams[0].url: scheme must be http or https, got "tcp"`,
				`pools[0].upstreams[1].url: must not contain a path, got "/api"`,
				`pools[0].upstreams[2].url: missing host in "http:///nohost"`,
				`pools[0].upstreams[3].url: must not contain a query, got "a=b"`,
				`pools[0].upstreams[4].url: must not contain a fragment, got "frag"`,
				`pools[0].upstreams[6].url: duplicate upstream "http://127.0.0.1:9005", already declared at pools[0].upstreams[5]`,
				"pools[0].upstreams[7].weight: must not be negative, got -2",
			},
			wantMin: 7,
		},
		{
			name: "consistent_hash without hash_on",
			file: "hash_on_missing.yaml",
			want: []string{`pools[0].hash_on: required when strategy is "consistent_hash"`},
		},
		{
			name: "hash_on malformed",
			file: "hash_on_bad_form.yaml",
			want: []string{
				`pools[0].hash_on: must be client_ip, path, header:NAME or cookie:NAME, got "header:"`,
				`pools[1].hash_on: must be client_ip, path, header:NAME or cookie:NAME, got "query:id"`,
			},
			wantMin: 2,
		},
		{
			name: "hash_on set on non-hashing strategy",
			file: "hash_on_wrong_strategy.yaml",
			want: []string{`pools[0].hash_on: only valid when strategy is "consistent_hash", but strategy is "round_robin"`},
		},
		{
			name: "active health zeros written explicitly",
			file: "bad_active_health.yaml",
			want: []string{
				`pools[0].health.active.path: must start with a slash, got "healthz"`,
				"pools[0].health.active.interval: must be > 0, got 0s",
				"pools[0].health.active.timeout: must be > 0, got 0s",
				"pools[0].health.active.expect_status[1]: must be a status code in [100,599], got 99",
				"pools[0].health.active.expect_status[2]: must be a status code in [100,599], got 600",
				"pools[0].health.active.healthy_threshold: must be >= 1, got 0",
				"pools[0].health.active.unhealthy_threshold: must be >= 1, got 0",
			},
			wantMin: 7,
		},
		{
			name: "active probe uses a body-bearing method",
			file: "bad_active_method.yaml",
			want: []string{`pools[0].health.active.method: must be GET, HEAD or OPTIONS, got "POST"`},
		},
		{
			name: "active probe timeout outlives its interval",
			file: "bad_active_timeout.yaml",
			want: []string{"pools[0].health.active.timeout: must be less than interval (1s), got 2s"},
		},
		{
			name: "passive health zeros written explicitly",
			file: "bad_passive_health.yaml",
			want: []string{
				"pools[0].health.passive.window: must be > 0, got 0s",
				"pools[0].health.passive.min_requests: must be >= 1, got 0",
				"pools[0].health.passive.error_rate: must be in (0,1], got 0",
				"pools[0].health.passive.eject_for: must be > 0, got 0s",
			},
			wantMin: 4,
		},
		{
			name: "breaker zeros written explicitly",
			file: "bad_breaker.yaml",
			want: []string{
				"pools[0].breaker.failure_threshold: must be >= 1, got 0",
				"pools[0].breaker.open_for: must be > 0, got 0s",
				"pools[0].breaker.half_open_probes: must be >= 1, got 0",
			},
			wantMin: 3,
		},
		{
			name: "limits negative",
			file: "bad_limits.yaml",
			want: []string{
				"pools[0].limits.max_in_flight: must be >= 0, got -1",
				"pools[0].limits.queue_timeout: must be >= 0",
			},
			wantMin: 2,
		},
		{
			name: "retry out of range",
			file: "bad_retry.yaml",
			want: []string{
				"pools[0].retry.max_attempts: must be in [1,5], got 9",
				"pools[0].retry.per_try_timeout: must be >= 0",
			},
			wantMin: 2,
		},
		{
			name: "transport negative",
			file: "bad_transport.yaml",
			want: []string{
				"pools[0].transport.max_idle_conns: must be >= 0, got -1",
				"pools[0].transport.max_idle_conns_per_host: must be >= 0, got -2",
				"pools[0].transport.max_conns_per_host: must be >= 0, got -3",
				"pools[0].transport.idle_conn_timeout: must be > 0",
				"pools[0].transport.dial_timeout: must be > 0",
				"pools[0].transport.response_header_timeout: must be >= 0",
			},
			wantMin: 6,
		},
		{
			name: "no routes",
			file: "no_routes.yaml",
			want: []string{"routes: at least one route is required"},
		},
		{
			name: "route names an undeclared pool",
			file: "unknown_route_pool.yaml",
			want: []string{`routes[1].pool: no pool named "nope" is declared`},
		},
		{
			name: "path_prefix without leading slash",
			file: "bad_path_prefix.yaml",
			want: []string{`routes[0].match.path_prefix: must start with a slash, got "v1/items"`},
		},
		{
			name: "methods not valid uppercase tokens",
			file: "bad_methods.yaml",
			want: []string{
				`routes[0].match.methods[1]: not a valid uppercase HTTP method, got "get"`,
				`routes[0].match.methods[2]: not a valid uppercase HTTP method, got "FETCH"`,
			},
			wantMin: 2,
		},
		{
			name: "pool declared but unreachable",
			file: "unused_pool.yaml",
			want: []string{`pools[1].name: pool "orphan" is declared but no route references it`},
		},
		{
			name: "every problem reported at once",
			file: "multi_error.yaml",
			want: []string{
				`listen: not a host:port address: "8080"`,
				`pools[0].strategy: must be one of`,
				`pools[0].upstreams[0].url: scheme must be http or https, got "tcp"`,
				`routes[0].pool: no pool named "missing" is declared`,
				`routes[0].match.path_prefix: must start with a slash, got "nope"`,
				`routes[0].match.methods[0]: not a valid uppercase HTTP method, got "get"`,
				`pools[0].name: pool "api" is declared but no route references it`,
				`pools[1].name: pool "orphan" is declared but no route references it`,
			},
			wantMin: 8,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(fixture(t, tc.file))
			if err == nil {
				t.Fatalf("Parse(%s) = nil error, want validation failure", tc.file)
			}
			msg := err.Error()
			for _, want := range tc.want {
				if !strings.Contains(msg, want) {
					t.Errorf("error missing %q\nfull error:\n%s", want, msg)
				}
			}
			if n := joinedCount(err); n < tc.wantMin {
				t.Errorf("reported %d problems, want at least %d\nfull error:\n%s", n, tc.wantMin, msg)
			}
		})
	}
}

// TestUnknownKeysRejected covers each nesting level. The deeper cases are the
// ones that matter: PoolConfig and RouteConfig have seeding unmarshalers, and
// yaml.Node.Decode does not inherit KnownFields, so without the strict mirror
// pass in load.go a typo inside pools[] would be silently ignored.
func TestUnknownKeysRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		file string
		want string
	}{
		{"top level", "unknown_key.yaml", "lisen"},
		{"inside a pool", "unknown_nested_key.yaml", "max_attemps"},
		{"inside pool health.active", "unknown_health_key.yaml", "intervall"},
		{"inside a pool upstream", "unknown_upstream_key.yaml", "wieght"},
		{"inside a route match", "unknown_route_key.yaml", "path_prefx"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Parse(fixture(t, tc.file))
			if err == nil {
				t.Fatalf("Parse(%s) = nil error, want unknown-field rejection", tc.file)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not name the offending key %q", err, tc.want)
			}
		})
	}
}

// TestStrictMirrorMatchesConfig fails if strictConfig drifts from Config. A
// field added to Config but not to the mirror would make the strict pass
// reject a legitimate document, so this keeps the two in lockstep.
func TestStrictMirrorMatchesConfig(t *testing.T) {
	t.Parallel()

	real, mirror := reflect.TypeOf(Config{}), reflect.TypeOf(strictConfig{})
	if real.NumField() != mirror.NumField() {
		t.Fatalf("Config has %d fields, strictConfig has %d", real.NumField(), mirror.NumField())
	}
	for i := range real.NumField() {
		rf, mf := real.Field(i), mirror.Field(i)
		if rf.Name != mf.Name {
			t.Errorf("field %d: Config has %q, strictConfig has %q", i, rf.Name, mf.Name)
		}
		if rf.Tag.Get("yaml") != mf.Tag.Get("yaml") {
			t.Errorf("field %s: yaml tag %q vs %q", rf.Name, rf.Tag.Get("yaml"), mf.Tag.Get("yaml"))
		}
	}
}

func TestDurationUnmarshal(t *testing.T) {
	t.Parallel()

	t.Run("valid strings parse", func(t *testing.T) {
		t.Parallel()
		c := parseFixture(t, "valid_full.yaml")
		if got := c.Server.ReadHeaderTimeout.D(); got != 5*time.Second {
			t.Errorf("read_header_timeout = %v, want 5s", got)
		}
		if got := c.Pools[0].Health.Active.Timeout.D(); got != 500*time.Millisecond {
			t.Errorf("active.timeout = %v, want 500ms", got)
		}
		if got := c.Pools[0].Retry.PerTryTimeout.String(); got != "100ms" {
			t.Errorf("per_try_timeout.String() = %q, want 100ms", got)
		}
	})

	t.Run("unparseable string", func(t *testing.T) {
		t.Parallel()
		_, err := Parse(fixture(t, "bad_duration.yaml"))
		if err == nil {
			t.Fatal("Parse = nil error, want duration parse failure")
		}
		if !strings.Contains(err.Error(), `invalid duration "30 seconds"`) {
			t.Errorf("error %q does not explain the bad duration", err)
		}
	})

	t.Run("wrong YAML type", func(t *testing.T) {
		t.Parallel()
		_, err := Parse(fixture(t, "duration_not_string.yaml"))
		if err == nil {
			t.Fatal("Parse = nil error, want duration type failure")
		}
		if !strings.Contains(err.Error(), "duration must be a string") {
			t.Errorf("error %q does not explain the expected type", err)
		}
	})
}

func TestParseEmptyDocument(t *testing.T) {
	t.Parallel()

	_, err := Parse(fixture(t, "empty.yaml"))
	if err == nil || !strings.Contains(err.Error(), "empty config document") {
		t.Fatalf("Parse(empty) = %v, want an empty-document error", err)
	}
}

func TestLoad(t *testing.T) {
	t.Parallel()

	t.Run("reads and validates a file", func(t *testing.T) {
		t.Parallel()
		c, err := Load(filepath.Join("testdata", "valid_minimal.yaml"))
		if err != nil {
			t.Fatalf("Load = %v, want success", err)
		}
		if c.Listen != ":8080" {
			t.Errorf("listen = %q, want :8080", c.Listen)
		}
	})

	t.Run("missing file names the path", func(t *testing.T) {
		t.Parallel()
		_, err := Load(filepath.Join("testdata", "does_not_exist.yaml"))
		if err == nil {
			t.Fatal("Load = nil error, want read failure")
		}
		if !strings.Contains(err.Error(), "does_not_exist.yaml") {
			t.Errorf("error %q does not name the file", err)
		}
	})

	t.Run("invalid file names the path", func(t *testing.T) {
		t.Parallel()
		_, err := Load(filepath.Join("testdata", "multi_error.yaml"))
		if err == nil {
			t.Fatal("Load = nil error, want validation failure")
		}
		if !strings.Contains(err.Error(), "multi_error.yaml") {
			t.Errorf("error %q does not name the file", err)
		}
	})
}

// TestExampleConfigIsValid keeps config.example.yaml honest: the file is the
// user-facing reference for the schema and its defaults, so a field renamed or
// removed in types.go must fail here rather than silently leave the example
// documenting a config manifold can no longer load.
func TestExampleConfigIsValid(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "config.example.yaml")
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load(%s) = %v, want the shipped example to validate clean", path, err)
	}
	if len(c.Pools) == 0 || len(c.Routes) == 0 {
		t.Fatalf("example config parsed to %d pools and %d routes, want both non-empty",
			len(c.Pools), len(c.Routes))
	}

	// The example claims the values it shows are the defaults. Booleans are
	// where that claim used to be false, so assert it directly: what the file
	// writes must equal what omitting the key produces.
	p, def := c.Pools[0], defaultPool()
	checks := []struct {
		field string
		got   bool
		want  bool
	}{
		{"health.active.enabled", p.Health.Active.Enabled, def.Health.Active.Enabled},
		{"health.passive.enabled", p.Health.Passive.Enabled, def.Health.Passive.Enabled},
		{"breaker.enabled", p.Breaker.Enabled, def.Breaker.Enabled},
		{"retry.idempotent_only", p.Retry.IdempotentOnly, def.Retry.IdempotentOnly},
		{"transport.disable_compression", p.Transport.DisableCompression, def.Transport.DisableCompression},
	}
	for _, tc := range checks {
		if tc.got != tc.want {
			t.Errorf("example pools[0].%s = %v, but the default is %v", tc.field, tc.got, tc.want)
		}
	}
}

// TestDuplicateUpstreamsAreComparedCanonically pins the fix for a real gap:
// validation deduped on the literal URL string while upstream.NewPool dedupes
// on the lower-cased scheme+host, so a config differing only in case validated
// clean and then failed to build. `manifold -check` reported ok for a config
// that could not start.
func TestDuplicateUpstreamsAreComparedCanonically(t *testing.T) {
	const doc = `
listen: ":8080"
pools:
  - name: api
    upstreams:
      - url: http://127.0.0.1:9001
      - url: HTTP://127.0.0.1:9001
routes:
  - match: {path_prefix: /}
    pool: api
`
	_, err := Parse([]byte(doc))
	if err == nil {
		t.Fatal("two upstreams differing only in scheme case were accepted; upstream.NewPool would reject them")
	}
	if !strings.Contains(err.Error(), "duplicate upstream") {
		t.Fatalf("rejected, but not as a duplicate: %v", err)
	}
}
