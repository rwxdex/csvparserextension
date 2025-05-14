package csvparser

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// LookupCSVValue - функция для поиска значений в CSV данных по ID
func LookupCSVValue(ctx context.Context, host component.Host, id string, field string) (string, error) {
	extension := host.GetExtension(component.MustNewID(TypeStr))
	if extension == nil {
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

// EnrichAttributes обогащает атрибуты спана данными из CSV
func EnrichAttributes(ctx context.Context, host component.Host, attrs pcommon.Map, idKey string) error {
	idVal, ok := attrs.Get(idKey)
	if !ok {
		return fmt.Errorf("ID key %s not found in attributes", idKey)
	}
	
	id := idVal.AsString()
	if id == "" {
		return fmt.Errorf("ID value is empty")
	}
	
	extension := host.GetExtension(component.MustNewID(TypeStr))
	if extension == nil {
		return fmt.Errorf("csv_parser extension not found")
	}
	
	csvExt, ok := extension.(*CSVParserExtension)
	if !ok {
		return fmt.Errorf("extension is not a CSVParserExtension")
	}
	
	values, exists := csvExt.GetParser().GetValueByID(id)
	if !exists {
		return nil // ID не найден, но это не ошибка
	}
	
	// Добавляем все поля из CSV в атрибуты
	for field, value := range values {
		if field == "id" {
			continue // Пропускаем поле id, так как оно уже есть
		}
		
		attrs.PutStr("enriched_"+field, value)
	}
	
	return nil
}