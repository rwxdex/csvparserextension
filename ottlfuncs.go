package csvparser

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlcommon"
)

// LookupCSVValue - функция для поиска значений в CSV данных по ID
func LookupCSVValue(ctx context.Context, host component.Host, id string, field string) (string, error) {
	extension, ok := host.GetExtensions()[component.NewID(component.Type(TypeStr))]
	if !ok {
		return "", fmt.Errorf("csv_parser extension not found")
	}
	
	csvExt, ok := extension.(*CSVParserExtension)
	if !ok {
		return "", fmt.Errorf("extension is not a CSVParserExtension")
	}
	
	values, exists := csvExt.GetParser().GetValueByID(id)
	if !exists {
		return "", nil
	}
	
	value, exists := values[field]
	if !exists {
		return "", fmt.Errorf("field %s not found for ID %s", field, id)
	}
	
	return value, nil
}

// RegisterFunctions регистрирует функции для transform processor
func RegisterFunctions() {
	// Регистрация функции csv_lookup для transform processor
	ottl.RegisterFunction("csv_lookup", csvLookupFactory)
}

// csvLookupFactory создает функцию csv_lookup для OTTL
func csvLookupFactory(createSettings ottl.CreateSettings) (ottl.Factory, error) {
	return func(args []ottl.Argument) (ottl.ExprFunc, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("csv_lookup function requires exactly 2 arguments (id, field), got %d", len(args))
		}

		return func(ctx context.Context, tCtx ottlcommon.TransformContext) (interface{}, error) {
			// Получаем ID
			idArg, err := args[0].Value(ctx, tCtx)
			if err != nil {
				return nil, fmt.Errorf("error getting id argument: %w", err)
			}
			id, ok := idArg.(string)
			if !ok {
				return nil, fmt.Errorf("id argument must be a string, got %T", idArg)
			}

			// Получаем имя поля
			fieldArg, err := args[1].Value(ctx, tCtx)
			if err != nil {
				return nil, fmt.Errorf("error getting field argument: %w", err)
			}
			field, ok := fieldArg.(string)
			if !ok {
				return nil, fmt.Errorf("field argument must be a string, got %T", fieldArg)
			}

			// Получаем хост из контекста
			host, ok := tCtx.GetHost()
			if !ok {
				return nil, fmt.Errorf("host not available in context")
			}

			// Ищем значение в CSV данных
			value, err := LookupCSVValue(ctx, host, id, field)
			if err != nil {
				return nil, err
			}

			return value, nil
		}, nil
	}, nil
}

// Инициализация
func init() {
	RegisterFunctions()
}
