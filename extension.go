package csvparser

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// csvParserExtension implements component.Extension to provide CSV parsing functionality
type csvParserExtension struct {
	logger *zap.Logger
	config *Config
	parser *csvParser
}

// newCSVParserExtension creates a new csvParserExtension
func newCSVParserExtension(config *Config, logger *zap.Logger) *csvParserExtension {
	return &csvParserExtension{
		logger: logger,
		config: config,
		parser: newCSVParser(logger, config),
	}
}

// Start starts the extension
func (e *csvParserExtension) Start(ctx context.Context, host component.Host) error {
	return e.parser.start(ctx)
}

// Shutdown stops the extension
func (e *csvParserExtension) Shutdown(ctx context.Context) error {
	return e.parser.stop(ctx)
}

// GetParser returns the CSV parser
func (e *csvParserExtension) GetParser() *csvParser {
	return e.parser
}

// GetCSVData возвращает данные CSV для указанного ID
func (e *csvParserExtension) GetCSVData(id string) (map[string]string, bool) {
	return e.parser.GetValueByID(id)
}