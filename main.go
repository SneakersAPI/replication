package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

var ctx = context.Background()

func main() {
	only := flag.String("only", "", "Only replicate one table by name")
	configPath := flag.String("config", "config.yml", "Path to the configuration file")
	drop := flag.String("drop", "", "Drop a table by name")
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
			if table.Cursor.LastSync.IsZero() || *drop == table.Source {
				log.Warn("No last sync date found, resetting cursor")
				table.Cursor.LastSync = time.Time{}
			}

			log.WithFields(log.Fields{
				"column":   table.Cursor.Column,
				"lastSync": table.Cursor.LastSync,
			}).Info("Resuming from cursor")
		}

		if *drop != "" && *drop == table.Source {
			log.WithField("table", table.Source).Info("Dropping table")

			if _, err := db.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table.Destination)); err != nil {
				log.WithError(err).Errorln("Failed to drop table")
			}
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
				"lastSync": config.Tables[idx].Cursor.LastSync,
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
	if err := CreatePostgresTable(table, db); err != nil {
		return err
	}

	columns := table.GetDestinationColumns()
	batches := make(chan [][]interface{})

	go func() {
		defer close(batches)
		total, err := Batching(table, conn, config.BatchSize, func(batch [][]interface{}) error {
			batches <- batch
			return nil
		})

		if err != nil {
			log.WithError(err).Errorln("Failed to batch")
		}

		log.WithField("total", total).Infoln("Selecting data completed")
	}()

	wg := sync.WaitGroup{}
	for batch := range batches {
		wg.Add(1)

		go func(batch [][]interface{}) {
			defer wg.Done()
			log.WithField("batch", len(batch)).Info("Inserting batch")

			conn, err := db.Acquire(ctx)
			if err != nil {
				log.WithError(err).Errorln("Failed to acquire connection")
				return
			}
			defer conn.Release()

			tableName, err := MakeTemporaryTable(table, conn)
			if err != nil {
				log.WithError(err).Errorln("Failed to make temporary table")
				return
			}

			_, err = conn.CopyFrom(
				ctx,
				pgx.Identifier{tableName},
				columns,
				pgx.CopyFromRows(batch),
			)
			if err != nil {
				log.WithError(err).Errorln("Failed to insert batch")
			}

			if err := MoveTemporaryTable(table, conn, tableName); err != nil {
				log.WithError(err).Errorln("Failed to move temporary table")
			}
		}(batch)
	}

	wg.Wait()

	log.Infoln("Data inserted")

	return nil
}

// MoveTemporaryTable moves the temporary table to the main table
func MoveTemporaryTable(table Table, conn *pgxpool.Conn, tableName string) error {
	updateQuery := []string{}
	for _, column := range table.GetDestinationColumns() {
		updateQuery = append(updateQuery, fmt.Sprintf("%s = EXCLUDED.%s", column, column))
	}

	log.WithField("source", tableName).Info("Moving temporary table")
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s
		SELECT DISTINCT ON (%s) * FROM %s
		ON CONFLICT (%s) DO UPDATE SET
		%s;
	`, table.Destination,
		strings.Join(table.GetPrimaryKey(), ", "),
		tableName,
		strings.Join(table.GetPrimaryKey(), ", "),
		strings.Join(updateQuery, ", "),
	))

	if err != nil {
		log.WithError(err).Errorln("Failed to move temporary table")
	}

	log.WithField("table", tableName).Infoln("Moved temporary table")

	return nil
}

// CreatePostgresTable creates a table in Postgres
func CreatePostgresTable(table Table, db *pgxpool.Pool) error {
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

	return nil
}

// MakeTemporaryTable creates a temporary table
func MakeTemporaryTable(table Table, conn *pgxpool.Conn) (string, error) {
	rnd := uuid.New().String()[:8]
	tableName := fmt.Sprintf("%s_%s_tmp", table.Destination, rnd)

	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TEMPORARY TABLE %s (LIKE %s INCLUDING DEFAULTS)`,
		tableName,
		table.Destination,
	))

	return tableName, err
}
