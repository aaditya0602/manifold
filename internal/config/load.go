package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// Load reads a YAML config file from disk and returns a fully defaulted,
// validated *Config. Any error already names the file it came from.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	c, err := Parse(data)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return c, nil
}

// Parse decodes YAML, then validates. It decodes the document twice, and the
// second pass is not redundant:
//
//  1. Strict pass — KnownFields(true) into a throwaway destination, purely to
//     reject unknown or misspelled keys. A setting that silently does nothing
//     is the most common way a load balancer ends up running a policy nobody
//     intended, so a typo has to be a hard error.
//
//  2. Real pass — into a defaults-seeded structure, so that omitted keys keep
//     their defaults while keys written as 0 or false win (see defaults.go).
//
// The passes cannot be merged. Seeding slice elements requires an
// UnmarshalYAML on PoolConfig and RouteConfig, and yaml.Node.Decode builds a
// fresh decoder that does not inherit KnownFields — so the moment a custom
// unmarshaler is involved, strictness stops propagating into it. The strict
// pass therefore runs against mirror types that shed those methods. Parsing a
// small file twice at startup costs nothing and buys both properties.
func Parse(data []byte) (*Config, error) {
	var probe strictConfig
	if err := decode(data, &probe); err != nil {
		return nil, err
	}

	c := defaultConfig()
	// KnownFields stays on here too. It is redundant with the strict pass for
	// the fields it can still reach, but it costs nothing and means a future
	// root-level field is covered even if the mirror below is not updated.
	if err := decode(data, &c); err != nil {
		return nil, err
	}

	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func decode(data []byte, into any) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(into); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("empty config document")
		}
		return fmt.Errorf("parse: %w", err)
	}
	return nil
}

// strictConfig mirrors Config for the unknown-key pass only. Its two slice
// fields use element types that shed the UnmarshalYAML methods from
// defaults.go, which is what lets KnownFields(true) reach inside `pools:` and
// `routes:`. Every other nested type is free of custom unmarshalers, so
// strictness propagates the rest of the way on its own.
//
// TestStrictMirrorMatchesConfig fails if this drifts from Config.
type strictConfig struct {
	Listen string        `yaml:"listen"`
	Admin  string        `yaml:"admin"`
	Server ServerConfig  `yaml:"server"`
	Pools  []strictPool  `yaml:"pools"`
	Routes []strictRoute `yaml:"routes"`
}

// A defined type has the fields of its underlying type but none of its
// methods, so these stay structurally identical to the real thing by
// construction while dropping the seeding unmarshalers.
type (
	strictPool  PoolConfig
	strictRoute RouteConfig
)
