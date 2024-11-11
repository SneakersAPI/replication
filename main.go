package main

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

var ctx = context.Background()

func main() {
	only := flag.String("only", "", "Only replicate one table by name")
	configPath := flag.String("config", "config.yml", "Path to the configuration file")
	flag.Parse()

	var config Config
	if err := config.Parse(*configPath); err != nil {
		log.Fatal("Failed to parse config", err)
	}

	dsn, err := clickhouse.ParseDSN(config.Clickhouse)
	if err != nil {
		log.WithError(err).Fatal("Failed to parse ClickHouse DSN")
	}

	conn, err := clickhouse.Open(dsn)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to ClickHouse")
	}
	defer conn.Close()

	db, err := pgxpool.New(ctx, config.Postgres)
	if err != nil {
		log.WithError(err).Fatal("Failed to connect to Postgres")
	}

	for idx, table := range config.Tables {
		log.WithFields(log.Fields{
			"source":      table.Source,
			"destination": table.Destination,
		}).Info("Replicating table")

		if *only != "" && *only != table.Source {
			log.Warn("Skipping this table")
			continue
		}

		start := time.Now()

		if table.Cursor.Column != "" {
			if table.Cursor.LastSync.IsZero() {
				log.Warn("No last sync date found, resetting cursor")
				config.Tables[idx].Cursor.LastSync = time.Time{}
			}

			log.WithFields(log.Fields{
				"column":   table.Cursor.Column,
				"lastSync": table.Cursor.LastSync,
			}).Info("Resuming from cursor")
		}

		if err := SynchronizeTable(config, table, conn, db); err != nil {
			log.WithError(err).Errorln("Failed to synchronize table")
			continue
		}

		if table.Cursor.Column != "" {
			now := time.Now()
			config.Tables[idx].Cursor.LastSync = now

			log.WithFields(log.Fields{
				"column":   table.Cursor.Column,
				"lastSync": table.Cursor.LastSync,
			}).Info("Updated cursor")
		}

		log.WithFields(log.Fields{
			"source":   table.Source,
			"duration": time.Since(start),
		}).Info("Table synchronized")
	}

	if err := config.Save(*configPath); err != nil {
		log.WithError(err).Fatal("Failed to save config")
	}

	log.Info("Replication completed")
}

// SynchronizeTable synchronizes a table from ClickHouse to Postgres
func SynchronizeTable(config Config, table Table, conn driver.Conn, db *pgxpool.Pool) error {
	if err := CreatePostgresTable(table, db, true); err != nil {
		return err
	}

	columns := table.GetDestinationColumns()

	total, err := Batching(table, conn, config.BatchSize, func(batch [][]interface{}) error {
		log.WithField("batch", len(batch)).Info("Inserting batch")

		_, err := db.CopyFrom(
			ctx,
			pgx.Identifier{table.Destination + "_tmp"},
			columns,
			pgx.CopyFromRows(batch),
		)

		return err
	})
	if err != nil {
		return err
	}

	log.WithField("total", total).Infoln("Batching completed")

	if err := MoveTemporaryTable(table, db); err != nil {
		return err
	}

	log.Infoln("Moved temporary table to main table")

	return nil
}

// MoveTemporaryTable moves the temporary table to the main table
func MoveTemporaryTable(table Table, db *pgxpool.Pool) error {
	updateQuery := []string{}
	for _, column := range table.GetDestinationColumns() {
		updateQuery = append(updateQuery, fmt.Sprintf("%s = EXCLUDED.%s", column, column))
	}

	_, err := db.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		SELECT * FROM %s_tmp
		ON CONFLICT (%s) DO UPDATE SET
		%s;
	`, table.Destination, table.Destination, strings.Join(table.GetPrimaryKey(), ", "), strings.Join(updateQuery, ", ")))

	return err
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

		log.WithFields(log.Fields{
			"index": i,
			"name":  columnTypes[i].Name(),
			"type":  columnTypes[i].ScanType(),
			"value": value,
		}).Info("Guessed scanner value")
	}
	return scannerVal
}

// CreatePostgresTable creates a table in Postgres
func CreatePostgresTable(table Table, db *pgxpool.Pool, temporary bool) error {
	columns := []string{}

	for _, column := range table.Columns {
		columns = append(columns, fmt.Sprintf("%s %s", column.Destination, column.Type))
	}

	_, err := db.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (%s)`,
		table.Destination,
		strings.Join(columns, ", "),
	))
	if err != nil {
		return err
	}

	if len(table.GetPrimaryKey()) > 0 {
		_, err = db.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s ADD PRIMARY KEY (%s)`,
			table.Destination,
			strings.Join(table.GetPrimaryKey(), ", "),
		))

		if err != nil {
			log.WithError(err).Warn("Failed to add primary key")
		}
	}

	for _, index := range table.Indexes {
		_, err = db.Exec(ctx, fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS %s_%s ON %s (%s)`,
			table.Destination,
			index.Name,
			table.Destination,
			strings.Join(index.Columns, ", "),
		))

		if err != nil {
			log.WithError(err).Warn("Failed to create index")
		}
	}

	if temporary {
		_, err = db.Exec(ctx, fmt.Sprintf(
			`CREATE TEMPORARY TABLE %s_tmp (LIKE %s INCLUDING DEFAULTS)`,
			table.Destination,
			table.Destination,
		))

		if err != nil {
			return err
		}
	}

	return nil
}
