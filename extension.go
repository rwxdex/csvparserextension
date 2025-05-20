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

// newCSVParserExtension creates a new csvParserExtension
func NewCSVParserExtension(config *Config, logger *zap.Logger) *csvParserExtension {
	return &csvParserExtension{
		logger: logger,
		config: config,
		parser: newCSVParser(logger, config),
	}
}

// Start starts the extension
func (e *CSVParserExtension) Start(ctx context.Context, host component.Host) error {
	return e.parser.start(ctx)
}

// Shutdown stops the extension
func (e *CSVParserExtension) Shutdown(ctx context.Context) error {
	return e.parser.stop(ctx)
}

// GetParser returns the CSV parser
func (e *CSVParserExtension) GetParser() *CSVParser {
	return e.parser
}

// LookupValue looks up a value in the CSV data by ID and field
func (e *CSVParserExtension) LookupValue(id, field string) (string, bool) {
	values, exists := e.parser.GetValueByID(id)
	if !exists {
		return "", false
	}
	
	value, exists := values[field]
	return value, exists
}