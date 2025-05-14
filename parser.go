package csvparser

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// CSVData представляет распарсенные данные CSV
type CSVData struct {
	// Data - карта, где ключ - ID, а значение - карта имени столбца и значения
	Data map[string]map[string]string
	// Headers - имена столбцов
	Headers []string
}

// CSVParser обрабатывает парсинг CSV файла
type CSVParser struct {
	logger         *zap.Logger
	config         *Config
	data           *CSVData
	mu             sync.RWMutex
	stopChan       chan struct{}
	refreshTicker  *time.Ticker
}

// NewCSVParser создает новый CSV парсер
func NewCSVParser(logger *zap.Logger, config *Config) *CSVParser {
	return &CSVParser{
		logger:   logger,
		config:   config,
		data:     &CSVData{Data: make(map[string]map[string]string)},
		stopChan: make(chan struct{}),
	}
}

// Start начинает процесс парсинга CSV
func (p *CSVParser) Start(ctx context.Context) error {
	// Изначально парсим CSV файл
	if err := p.parseCSV(); err != nil {
		return err
	}
	
	// Если установлен интервал обновления, запускаем тикер для периодической перезагрузки файла
	if p.config.RefreshInterval > 0 {
		p.refreshTicker = time.NewTicker(time.Duration(p.config.RefreshInterval) * time.Second)
		go func() {
			for {
				select {
				case <-p.refreshTicker.C:
					if err := p.parseCSV(); err != nil {
						p.logger.Error("Failed to refresh CSV data", zap.Error(err))
					} else {
						p.logger.Info("Successfully refreshed CSV data")
					}
				case <-p.stopChan:
					return
				}
			}
		}()
	}
	
	return nil
}

// Stop останавливает процесс парсинга CSV
func (p *CSVParser) Stop(ctx context.Context) error {
	if p.refreshTicker != nil {
		p.refreshTicker.Stop()
	}
	close(p.stopChan)
	return nil
}

// GetData возвращает распарсенные данные CSV
func (p *CSVParser) GetData() *CSVData {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.data
}

// GetValueByID возвращает значения для заданного ID
func (p *CSVParser) GetValueByID(id string) (map[string]string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	values, exists := p.data.Data[id]
	return values, exists
}

// parseCSV читает и парсит CSV файл
func (p *CSVParser) parseCSV() error {
	p.logger.Info("Parsing CSV file", zap.String("file_path", p.config.FilePath))
	
	file, err := os.Open(p.config.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()
	
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read CSV file: %w", err)
	}
	
	if len(records) == 0 {
		return fmt.Errorf("CSV file is empty")
	}
	
	newData := &CSVData{
		Data: make(map[string]map[string]string),
	}
	
	startIdx := 0
	if p.config.HasHeader {
		newData.Headers = records[0]
		startIdx = 1
	} else {
		// Если нет заголовка, создаем имена столбцов по умолчанию
		newData.Headers = make([]string, len(records[0]))
		newData.Headers[0] = "id"
		for i := 1; i < len(records[0]); i++ {
			newData.Headers[i] = fmt.Sprintf("column%d", i)
		}
	}
	
	for i := startIdx; i < len(records); i++ {
		record := records[i]
		if len(record) < 1 {
			p.logger.Warn("Skipping empty record", zap.Int("line", i+1))
			continue
		}
		
		id := record[0]
		if id == "" {
			p.logger.Warn("Skipping record with empty ID", zap.Int("line", i+1))
			continue
		}
		
		values := make(map[string]string)
		for j := 0; j < len(record) && j < len(newData.Headers); j++ {
			values[newData.Headers[j]] = record[j]
		}
		
		newData.Data[id] = values
	}
	
	// Обновляем данные с блокировкой записи
	p.mu.Lock()
	p.data = newData
	p.mu.Unlock()
	
	p.logger.Info("Successfully parsed CSV file", 
		zap.String("file_path", p.config.FilePath),
		zap.Int("records", len(newData.Data)))
	
	return nil
}