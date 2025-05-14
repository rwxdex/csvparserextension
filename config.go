package csvparser

import (
	"fmt"
	"path/filepath"

	"go.opentelemetry.io/collector/component"
)

// Config определяет конфигурацию для расширения CSV Parser.
type Config struct {
	// FilePath - путь к CSV файлу
	FilePath string `mapstructure:"file_path"`
	
	// RefreshInterval - интервал в секундах, через который CSV файл будет перезагружаться
	RefreshInterval int `mapstructure:"refresh_interval"`
	
	// HasHeader - указывает, содержит ли CSV файл заголовок
	HasHeader bool `mapstructure:"has_header"`
}

// Validate проверяет конфигурацию
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
