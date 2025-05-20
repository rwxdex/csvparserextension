package csvparser

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/transformprocessor/internal/common"
)

// Регистрируем функцию с именем, начинающимся с заглавной буквы
func RegisterFunctions() {
	// Регистрация функций для transform processor
	common.RegisterFunction("CsvLookup", CsvLookupFunction)
}

// CsvLookupFunction - функция для использования в transform processor
func CsvLookupFunction(ctx context.Context, tCtx common.TransformContext, args ...interface{}) (interface{}, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("CsvLookup function requires exactly 2 arguments (id, field), got %d", len(args))
	}
	
	id, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("first argument must be a string (id), got %T", args[0])
	}
	
	field, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("second argument must be a string (field), got %T", args[1])
	}
	
	// Получаем хост из контекста
	host, ok := tCtx.GetHost()
	if !ok {
		return nil, fmt.Errorf("host not available in context")
	}
	
	// Получаем расширение
	ext := host.GetExtension(component.MustNewID(typeStr))
	if ext == nil {
		return nil, fmt.Errorf("csv_parser extension not found")
	}
	
	csvExt, ok := ext.(*csvParserExtension)
	if !ok {
		return nil, fmt.Errorf("extension is not a CSVParserExtension")
	}
	
	// Получаем значение из CSV
	values, exists := csvExt.GetParser().GetValueByID(id)
	if !exists {
		return nil, nil
	}
	
	value, exists := values[field]
	if !exists {
		return nil, fmt.Errorf("field %s not found for ID %s", field, id)
	}
	
	return value, nil
}

// Инициализация при загрузке пакета
func init() {
	RegisterFunctions()
}