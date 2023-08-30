package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// CreateDB create a database connection
func CreateDB() *sql.DB {
	log.Info("Connecting to database")
	log.WithField("dsn", viper.GetString("database.dsn")).Debug("Connecting...")

	db, err := sql.Open("mysql", viper.GetString("database.dsn"))
	if err != nil {
		panic(err)
	}

	// See "Important settings" section.
	db.SetConnMaxLifetime(time.Minute * 30)
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	return db
}

// SelectAllDevices selects all devices (without errors)
func DumpServicesStops(db *sql.DB, startDate, endDate string) error {
	serviceRows, err := db.Query("SELECT id FROM service WHERE service_date >= ? AND service_date <= ?", startDate, endDate)

	if err != nil {
		log.Fatal(err)
	}
	defer serviceRows.Close()

	for serviceRows.Next() {
		var serviceID int

		if err := serviceRows.Scan(&serviceID); err != nil {
			log.Fatal(err)
		}

		fmt.Println(serviceID)

		stopRows, err := db.Query("SELECT id, stop_code, stop_name FROM stop WHERE service_id = ? ORDER BY stop_index", serviceID)

		if err != nil {
			log.Fatal(err)
		}
		defer stopRows.Close()

		for stopRows.Next() {
			var stopID int
			var stopCode, stopName string

			if err := stopRows.Scan(&stopID, &stopCode, &stopName); err != nil {
				log.Fatal(err)
			}

			fmt.Println("       ", stopID, stopCode)

		}
	}

	if err := serviceRows.Err(); err != nil {
		log.Fatal(err)
	}

	return err
}
