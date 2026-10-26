// Package config handles cluster configuration loading for GhostKV.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// ClusterConfig is the top-level configuration for a GhostKV cluster.
type ClusterConfig struct {
	Cluster ClusterSettings `yaml:"cluster"`
	Nodes   []NodeConfig    `yaml:"nodes"`
}

// ClusterSettings holds cluster-wide parameters.
type ClusterSettings struct {
	Name              string `yaml:"name"`
	ReplicationFactor int    `yaml:"replication_factor"`
	WriteQuorum       int    `yaml:"write_quorum"`
	ReadQuorum        int    `yaml:"read_quorum"`
	VirtualNodes      int    `yaml:"virtual_nodes"`
}

// NodeConfig describes a single node in the cluster.
type NodeConfig struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
	DataDir string `yaml:"data_dir"`
}

// Load reads and parses a cluster configuration from the given YAML file path.
func Load(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var cfg ClusterConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	return &cfg, nil
}

// FindNode returns the NodeConfig for the given node ID, or an error if not found.
func (c *ClusterConfig) FindNode(id string) (*NodeConfig, error) {
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			return &c.Nodes[i], nil
		}
	}
	return nil, fmt.Errorf("node %q not found in cluster config", id)
}
