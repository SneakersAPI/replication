package main

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	log "github.com/sirupsen/logrus"
)

// Batching reads rows from ClickHouse and sends them to the callback function
func Batching(table Table, conn driver.Conn, batchSize int, onBatch func([][]interface{}) error) (int, error) {
	query := fmt.Sprintf(
		"SELECT %s FROM %s FINAL",
		strings.Join(table.GetSourceColumns(), ", "),
		table.Source,
	)

	if table.Cursor.Column != "" && !table.Cursor.LastSync.IsZero() {
		query = fmt.Sprintf("%s WHERE %s > '%s'", query, table.Cursor.Column, table.Cursor.LastSync.Format(time.DateTime))
	}

	var scannerVal []interface{}
	total := 0

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}

	batch := [][]interface{}{}
	for rows.Next() {
		if scannerVal == nil {
			scannerVal = GetScannerValues(rows.ColumnTypes())
		}

		values := make([]interface{}, len(scannerVal))
		for i := range values {
			values[i] = reflect.New(reflect.TypeOf(scannerVal[i])).Interface()
		}

		if err := rows.Scan(values...); err != nil {
			return 0, err
		}

		batch = append(batch, values)

		if len(batch) == batchSize {
			if err := onBatch(batch); err != nil {
				return 0, err
			}

			total += len(batch)
			batch = [][]interface{}{}
		}
	}

	if len(batch) > 0 {
		total += len(batch)

		if err := onBatch(batch); err != nil {
			return 0, err
		}
	}

	return total, nil
}

// GetScannerValues guesses the scanner values from the column types
func GetScannerValues(columnTypes []driver.ColumnType) []interface{} {
	log.Info("Guessing scanner values")
	scannerVal := make([]interface{}, len(columnTypes))
	for i := range scannerVal {
		scannerVal[i] = reflect.New(columnTypes[i].ScanType()).Interface()

		value := reflect.ValueOf(scannerVal[i]).Elem().Kind()
		if value == reflect.Ptr {
			scannerVal[i] = reflect.New(columnTypes[i].ScanType().Elem()).Interface()
		}

		if value == reflect.Slice {
			scannerVal[i] = reflect.MakeSlice(columnTypes[i].ScanType(), 0, 0).Interface()
		}

		if value == reflect.Map {
			scannerVal[i] = reflect.MakeMap(columnTypes[i].ScanType()).Interface()
		}

		log.WithFields(log.Fields{
			"index": i,
			"name":  columnTypes[i].Name(),
			"type":  columnTypes[i].ScanType(),
			"value": value,
		}).Info("Guessed scanner value")
	}
	return scannerVal
}
