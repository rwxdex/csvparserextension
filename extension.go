package csvparser

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// TypeStr - тип расширения, используемый в конфигурации
const TypeStr = "csv_parser"

// CSVParserExtension реализует component.Extension для обеспечения функциональности парсинга CSV
type CSVParserExtension struct {
	logger *zap.Logger
	config *Config
	parser *CSVParser
}

// NewCSVParserExtension создает новый экземпляр CSVParserExtension
func NewCSVParserExtension(config *Config, logger *zap.Logger) *CSVParserExtension {
	return &CSVParserExtension{
		logger: logger,
		config: config,
		parser: NewCSVParser(logger, config),
	}
}

// Start запускает расширение
func (e *CSVParserExtension) Start(ctx context.Context, host component.Host) error {
	return e.parser.Start(ctx)
}

// Shutdown останавливает расширение
func (e *CSVParserExtension) Shutdown(ctx context.Context) error {
	return e.parser.Stop(ctx)
}

// GetParser возвращает CSV парсер
func (e *CSVParserExtension) GetParser() *CSVParser {
	return e.parser
}

