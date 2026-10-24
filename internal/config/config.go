package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// NodeConfig specifies configuration for a single RaftKV server instance.
type NodeConfig struct {
	ID                 string            `yaml:"id"`
	DataDir            string            `yaml:"data_dir"`
	Address            string            `yaml:"address"`
	Peers              map[string]string `yaml:"peers"` // peerID -> address
	MinElectionTimeout string            `yaml:"min_election_timeout"`
	MaxElectionTimeout string            `yaml:"max_election_timeout"`
	HeartbeatInterval  string            `yaml:"heartbeat_interval"`
}

// ParsedNodeConfig holds typed configuration after validation.
type ParsedNodeConfig struct {
	ID                 string
	DataDir            string
	Address            string
	Peers              map[string]string
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration
	HeartbeatInterval  time.Duration
}

// LoadConfig loads and parses a YAML configuration file.
func LoadConfig(path string) (*ParsedNodeConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var raw NodeConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse yaml config: %w", err)
	}

	return raw.Parse()
}

// Parse validates and converts the raw configuration.
func (c *NodeConfig) Parse() (*ParsedNodeConfig, error) {
	if c.ID == "" {
		return nil, fmt.Errorf("config error: id is required")
	}
	if c.DataDir == "" {
		return nil, fmt.Errorf("config error: data_dir is required")
	}
	if c.Address == "" {
		return nil, fmt.Errorf("config error: address is required")
	}

	minTimeout := 150 * time.Millisecond
	if c.MinElectionTimeout != "" {
		d, err := time.ParseDuration(c.MinElectionTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid min_election_timeout: %w", err)
		}
		minTimeout = d
	}

	maxTimeout := 300 * time.Millisecond
	if c.MaxElectionTimeout != "" {
		d, err := time.ParseDuration(c.MaxElectionTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid max_election_timeout: %w", err)
		}
		maxTimeout = d
	}

	hbInterval := 50 * time.Millisecond
	if c.HeartbeatInterval != "" {
		d, err := time.ParseDuration(c.HeartbeatInterval)
		if err != nil {
			return nil, fmt.Errorf("invalid heartbeat_interval: %w", err)
		}
		hbInterval = d
	}

	return &ParsedNodeConfig{
		ID:                 c.ID,
		DataDir:            c.DataDir,
		Address:            c.Address,
		Peers:              c.Peers,
		MinElectionTimeout: minTimeout,
		MaxElectionTimeout: maxTimeout,
		HeartbeatInterval:  hbInterval,
	}, nil
}
