package csvparser

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// CSVParserExtension implements component.Extension to provide CSV parsing functionality
type CSVParserExtension struct {
	logger *zap.Logger
	config *Config
	parser *CSVParser
}

// NewCSVParserExtension creates a new CSVParserExtension
func NewCSVParserExtension(config *Config, logger *zap.Logger) *CSVParserExtension {
	return &CSVParserExtension{
		logger: logger,
		config: config,
		parser: NewCSVParser(logger, config),
	}
}

// Start starts the extension
func (e *CSVParserExtension) Start(ctx context.Context, host component.Host) error {
	e.logger.Info("Starting CSV Parser extension")
	return e.parser.Start(ctx)
}

// Shutdown stops the extension
func (e *CSVParserExtension) Shutdown(ctx context.Context) error {
	e.logger.Info("Shutting down CSV Parser extension")
	return e.parser.Stop(ctx)
}

// GetParser returns the CSV parser
func (e *CSVParserExtension) GetParser() *CSVParser {
	return e.parser
}

