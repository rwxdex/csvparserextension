package csvparser

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

const (
	// The value of extension "type" in configuration.
	typeStr = "csv_parser"
)

// NewFactory creates a factory for the CSV parser extension.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RefreshInterval: 300, // Default refresh every 5 minutes
		HasHeader:       true,
	}
}

func createExtension(
	_ context.Context,
	params extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	config := cfg.(*Config)
	return newCSVParserExtension(config, params.Logger), nil
}