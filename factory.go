package csvparser

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

// NewFactory создает фабрику для расширения CSV parser.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		component.MustNewType(TypeStr),
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		RefreshInterval: 300, // Обновление каждые 5 минут по умолчанию
		HasHeader:       true,
	}
}

func createExtension(
	ctx context.Context,
	set extension.CreateSettings,
	cfg component.Config,
) (extension.Extension, error) {
	config := cfg.(*Config)
	return NewCSVParserExtension(config, set.Logger), nil
}

