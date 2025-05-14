package csvparser

import (
	"fmt"
	"path/filepath"
)

// Config defines configuration for CSV Parser extension.
type Config struct {
	// FilePath is the path to the CSV file
	FilePath string `mapstructure:"file_path"`
	
	// RefreshInterval is the interval at which the CSV file should be reloaded (in seconds)
	RefreshInterval int `mapstructure:"refresh_interval"`
	
	// HasHeader indicates if the CSV file has a header row
	HasHeader bool `mapstructure:"has_header"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.FilePath == "" {
		return fmt.Errorf("file_path cannot be empty")
	}
	
	ext := filepath.Ext(c.FilePath)
	if ext != ".csv" {
		return fmt.Errorf("file must have .csv extension, got %s", ext)
	}
	
	if c.RefreshInterval < 0 {
		return fmt.Errorf("refresh_interval cannot be negative")
	}
	
	return nil
}
