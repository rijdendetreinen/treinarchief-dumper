package dump

import (
	"database/sql"
	"encoding/csv"
	"os"
	"strconv"
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
func DumpServicesStops(db *sql.DB, csvFile *os.File, startDate, endDate string) error {
	w := csv.NewWriter(csvFile)
	defer w.Flush()

	// Write header row:
	w.Write([]string{
		"Service:RDT-ID",
		"Service:Date",
		"Service:Type",
		"Service:Train number",
		"Service:Completely cancelled",
		"Service:Partly cancelled",
		"Service:Maximum delay",
		"Stop:RDT-ID",
		"Stop:Station code",
		"Stop:Station name",
		"Stop:Arrival time",
		"Stop:Arrival delay",
		"Stop:Arrival cancelled",
		"Stop:departure time",
		"Stop:departure delay",
		"Stop:departure cancelled",
	})

	serviceRows, err := db.Query("SELECT id, service_date, type, cancelled_completely, cancelled_partly, max_delay FROM service WHERE service_date >= ? AND service_date <= ?", startDate, endDate)

	if err != nil {
		log.Fatal(err)
	}
	defer serviceRows.Close()

	for serviceRows.Next() {
		var serviceID, maxDelay int
		var completelyCancelled, partlyCancelled bool
		var serviceDate, serviceType string

		if err := serviceRows.Scan(&serviceID, &serviceDate, &serviceType, &completelyCancelled, &partlyCancelled, &maxDelay); err != nil {
			log.Fatal(err)
		}

		stopRows, err := db.Query("SELECT id, service_number, stop_code, stop_name, arrival, arrival_delay, arrival_cancelled, departure, departure_delay, departure_cancelled FROM stop WHERE service_id = ? ORDER BY stop_index", serviceID)

		if err != nil {
			log.Fatal(err)
		}
		defer stopRows.Close()

		dateTimeLayout := "2006-01-02 15:04:05"
		timezone, err := time.LoadLocation("Europe/Amsterdam")

		if err != nil {
			panic(err)
		}

		for stopRows.Next() {
			var serviceNumber, stopCode, stopName string
			var stopID, arrivalDelay, departureDelay int
			var arrivalTime, departureTime sql.NullString
			var arrivalCancelled, departureCancelled bool
			var arrivalTimeCSV, arrivalDelayCSV, arrivalCancelledCSV, departureTimeCSV, departureDelayCSV, departureCancelledCSV string

			if err := stopRows.Scan(&stopID, &serviceNumber, &stopCode, &stopName, &arrivalTime, &arrivalDelay, &arrivalCancelled, &departureTime, &departureDelay, &departureCancelled); err != nil {
				log.Fatal(err)
			}

			// Round max delays from seconds to minutes:
			maxDelay = int(float64(maxDelay) / 60)
			arrivalDelay = int(float64(arrivalDelay) / 60)
			departureDelay = int(float64(departureDelay) / 60)

			// Check for nulls:
			if arrivalTime.Valid {
				arrivalTimeDT, err := time.ParseInLocation(dateTimeLayout, arrivalTime.String, timezone)

				if err == nil {
					arrivalTimeCSV = arrivalTimeDT.Format(time.RFC3339)
				} else {
					arrivalTimeCSV = arrivalTime.String
				}

				arrivalDelayCSV = strconv.Itoa(arrivalDelay)
				arrivalCancelledCSV = strconv.FormatBool(arrivalCancelled)
			} else {
				arrivalTimeCSV = ""
				arrivalDelayCSV = ""
				arrivalCancelledCSV = ""
			}

			if departureTime.Valid {
				departureTimeDT, err := time.ParseInLocation(dateTimeLayout, departureTime.String, timezone)

				if err == nil {
					departureTimeCSV = departureTimeDT.Format(time.RFC3339)
				} else {
					departureTimeCSV = departureTime.String
				}

				departureDelayCSV = strconv.Itoa(departureDelay)
				departureCancelledCSV = strconv.FormatBool(departureCancelled)
			} else {
				departureTimeCSV = ""
				departureDelayCSV = ""
				departureCancelledCSV = ""
			}

			w.Write([]string{
				strconv.Itoa(serviceID),
				serviceDate,
				serviceType,
				serviceNumber,
				strconv.FormatBool(completelyCancelled),
				strconv.FormatBool(partlyCancelled),
				strconv.Itoa(maxDelay),
				strconv.Itoa(stopID),
				stopCode,
				stopName,
				arrivalTimeCSV,
				arrivalDelayCSV,
				arrivalCancelledCSV,
				departureTimeCSV,
				departureDelayCSV,
				departureCancelledCSV,
			})

		}
	}

	if err := serviceRows.Err(); err != nil {
		log.Fatal(err)
	}

	return err
}
