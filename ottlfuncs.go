package csvparser

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor"
)

// RegisterFunctions регистрирует функции OTTL для использования в transform processor
func RegisterFunctions() {
	// Регистрация функции CsvLookup для использования в transform processor
	transformprocessor.RegisterSpanFunctions(map[string]interface{}{
		"CsvLookup": csvLookupFactory,
	})
}

// csvLookupFactory создает функцию CsvLookup для OTTL
func csvLookupFactory(createSettings ottl.CreateSettings) (ottlspan.SpanFunction, error) {
	return func(ctx context.Context, tCtx ottlspan.TransformContext, args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("CsvLookup requires 2 arguments: id and field")
		}

		// Получаем ID
		id, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("first argument (id) must be a string, got %T", args[0])
		}

		// Получаем имя поля
		field, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("second argument (field) must be a string, got %T", args[1])
		}

		// Получаем хост из контекста
		host, ok := tCtx.GetHostFromContext()
		if !ok {
			return nil, fmt.Errorf("host not available in context")
		}

		// Получаем расширение csv_parser
		ext, ok := host.GetExtensions()[component.MustNewType("csv_parser")]
		if !ok {
			return nil, fmt.Errorf("csv_parser extension not found")
		}

		// Приводим к типу нашего расширения
		csvExt, ok := ext.(extension.Extension)
		if !ok {
			return nil, fmt.Errorf("extension is not a valid extension")
		}

		// Приводим к нашему типу расширения
		csvParserExt, ok := csvExt.(*csvParserExtension)
		if !ok {
			return nil, fmt.Errorf("extension is not a csvParserExtension")
		}

		// Получаем значение из CSV данных
		values, exists := csvParserExt.GetParser().GetValueByID(id)
		if !exists {
			return "", nil // ID не найден, возвращаем пустую строку
		}

		value, exists := values[field]
		if !exists {
			return "", nil // Поле не найдено, возвращаем пустую строку
		}

		return value, nil
	}, nil
}

func init() {
	// Регистрируем функции при загрузке пакета
	RegisterFunctions()
}