package main

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Tables    []Table `yaml:"tables"`
	BatchSize int     `yaml:"batch_size"`
}

func (c *Config) Parse(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(b, c)
}

func (c *Config) Save(path string) error {
	b, err := yaml.Marshal(&c)
	if err != nil {
		return err
	}

	return os.WriteFile(path, b, 0644)
}

type Table struct {
	Source      string   `yaml:"source"`
	Destination string   `yaml:"destination"`
	Indexes     []Index  `yaml:"indexes"`
	Columns     []Column `yaml:"columns"`
	Cursor      Cursor   `yaml:"cursor"`
}

func (t *Table) GetSourceColumns() []string {
	names := []string{}
	for _, column := range t.Columns {
		names = append(names, column.Source)
	}
	return names
}

func (t *Table) GetDestinationColumns() []string {
	names := []string{}
	for _, column := range t.Columns {
		names = append(names, column.Destination)
	}
	return names
}

func (t *Table) GetPrimaryKey() []string {
	names := []string{}
	for _, column := range t.Columns {
		if column.Primary {
			names = append(names, column.Destination)
		}
	}
	return names
}

type Column struct {
	Source      string `yaml:"source"`
	Destination string `yaml:"destination"`
	Type        string `yaml:"type"`
	Primary     bool   `yaml:"primary"`
}

type Cursor struct {
	Column   string    `yaml:"column"`
	LastSync time.Time `yaml:"last_sync"`
}

type Index struct {
	Name    string   `yaml:"name"`
	Columns []string `yaml:"columns"`
}
