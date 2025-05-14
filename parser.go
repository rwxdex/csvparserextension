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

// CSVData represents the parsed CSV data
type CSVData struct {
	// Data is a map where key is the ID and value is a map of column name to value
	Data map[string]map[string]string
	// Headers are the column names
	Headers []string
}

// CSVParser handles the parsing of the CSV file
type CSVParser struct {
	logger         *zap.Logger
	config         *Config
	data           *CSVData
	mu             sync.RWMutex
	stopChan       chan struct{}
	refreshTicker  *time.Ticker
}

// NewCSVParser creates a new CSV parser
func NewCSVParser(logger *zap.Logger, config *Config) *CSVParser {
	return &CSVParser{
		logger:   logger,
		config:   config,
		data:     &CSVData{Data: make(map[string]map[string]string)},
		stopChan: make(chan struct{}),
	}
}

// Start begins the CSV parsing process
func (p *CSVParser) Start(ctx context.Context) error {
	// Parse the CSV file initially
	if err := p.parseCSV(); err != nil {
		return err
	}
	
	// If refresh interval is set, start a ticker to reload the file periodically
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

// Stop stops the CSV parsing process
func (p *CSVParser) Stop(ctx context.Context) error {
	if p.refreshTicker != nil {
		p.refreshTicker.Stop()
	}
	close(p.stopChan)
	return nil
}

// GetData returns the parsed CSV data
func (p *CSVParser) GetData() *CSVData {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.data
}

// GetValueByID returns the values for a given ID
func (p *CSVParser) GetValueByID(id string) (map[string]string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	values, exists := p.data.Data[id]
	return values, exists
}

// parseCSV reads and parses the CSV file
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
		// If no header, create default column names
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
	
	// Update the data with a write lock
	p.mu.Lock()
	p.data = newData
	p.mu.Unlock()
	
	p.logger.Info("Successfully parsed CSV file", 
		zap.String("file_path", p.config.FilePath),
		zap.Int("records", len(newData.Data)))
	
	return nil
}
