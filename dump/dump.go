package dump

import (
	"compress/gzip"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)

	return db
}

// SelectAllDevices selects all devices (without errors)
func DumpServicesStops(db *sql.DB, csvFile *os.File, gzipCompression bool, startDate, endDate string, includeMaterial bool) error {
	var w *csv.Writer
	var zipWriter *gzip.Writer

	if gzipCompression {
		zipWriter = gzip.NewWriter(csvFile)
		w = csv.NewWriter(zipWriter)

		defer zipWriter.Flush()
		defer zipWriter.Close()
	} else {
		w = csv.NewWriter(csvFile)
	}

	defer w.Flush()

	// Write header row:
	headers := []string{
		"Service:RDT-ID",
		"Service:Date",
		"Service:Type",
		"Service:Company",
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
		"Stop:Departure time",
		"Stop:Departure delay",
		"Stop:Departure cancelled",
	}

	if includeMaterial {
		headers = append(headers, "Stop:Stock types")
		headers = append(headers, "Stop:Unit numbers")
	}

	w.Write(headers)

	var serviceCount int

	serviceCountRow, err := db.Query("SELECT COUNT(id) FROM service WHERE service_date >= ? AND service_date <= ?", startDate, endDate)
	if err != nil {
		log.Fatal(err)
	}

	serviceCountRow.Next()
	serviceCountRow.Scan(&serviceCount)
	serviceCountRow.Close()

	log.WithFields(log.Fields{"from": startDate, "to": endDate}).Info("Selecting ", serviceCount, " services")

	serviceRows, err := db.Query("SELECT id, service_date, type, company, cancelled_completely, cancelled_partly, max_delay FROM service WHERE service_date >= ? AND service_date <= ?", startDate, endDate)

	if err != nil {
		log.Fatal(err)
	}
	defer serviceRows.Close()

	if err := serviceRows.Err(); err != nil {
		log.Fatal(err)
	}

	stopRowsStatement, err := db.Prepare("SELECT id, service_number, stop_code, stop_name, arrival, arrival_delay, arrival_cancelled, departure, departure_delay, departure_cancelled, material FROM stop WHERE service_id = ? ORDER BY stop_index")
	if err != nil {
		log.Fatal(err)
	}

	defer stopRowsStatement.Close()

	serviceCounter := 0
	stopCounter := 0

	// for services:
	var serviceID, maxDelay int
	var completelyCancelled, partlyCancelled bool
	var serviceDate, serviceType, serviceCompany string

	// for stops:
	var serviceNumber, stopCode, stopName string
	var stopID, arrivalDelay, departureDelay int
	var arrivalDelayNullable, departureDelayNullable sql.NullInt64
	var arrivalTime, departureTime sql.NullString
	var arrivalCancelled, departureCancelled *bool
	var arrivalTimeCSV, arrivalDelayCSV, arrivalCancelledCSV, departureTimeCSV, departureDelayCSV, departureCancelledCSV, materialJSON string

	dateTimeLayout := "2006-01-02 15:04:05"
	timezone, err := time.LoadLocation("Europe/Amsterdam")

	for serviceRows.Next() {
		serviceCounter++

		if err := serviceRows.Scan(&serviceID, &serviceDate, &serviceType, &serviceCompany, &completelyCancelled, &partlyCancelled, &maxDelay); err != nil {
			log.Fatal(err)
		}

		func() {

			stopRows, err := stopRowsStatement.Query(serviceID)

			if err != nil {
				log.Fatal(err)
			}

			if err != nil {
				panic(err)
			}

			for stopRows.Next() {
				stopCounter++

				if err := stopRows.Scan(&stopID, &serviceNumber, &stopCode, &stopName, &arrivalTime, &arrivalDelayNullable, &arrivalCancelled, &departureTime, &departureDelayNullable, &departureCancelled, &materialJSON); err != nil {
					log.Fatal(err)
				}

				if arrivalDelayNullable.Valid {
					arrivalDelay = int(arrivalDelayNullable.Int64)
				} else {
					arrivalDelay = 0
				}

				if departureDelayNullable.Valid {
					departureDelay = int(departureDelayNullable.Int64)
				} else {
					departureDelay = 0
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

					if arrivalCancelled == nil {
						*arrivalCancelled = false
					}

					arrivalDelayCSV = strconv.Itoa(arrivalDelay)
					arrivalCancelledCSV = strconv.FormatBool(*arrivalCancelled)
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

					if departureCancelled == nil {
						*departureCancelled = false
					}

					departureDelayCSV = strconv.Itoa(departureDelay)
					departureCancelledCSV = strconv.FormatBool(*departureCancelled)
				} else {
					departureTimeCSV = ""
					departureDelayCSV = ""
					departureCancelledCSV = ""
				}

				row := []string{
					strconv.Itoa(serviceID),
					serviceDate,
					serviceType,
					serviceCompany,
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
				}

				if includeMaterial {
					var material string
					var unitNumbers string

					var materialList []map[string]string
					err := json.Unmarshal([]byte(materialJSON), &materialList)

					if err != nil {
						log.Fatal(err)
					}

					for _, materialItem := range materialList {
						material += materialItem["type"] + " + "

						if len(materialItem["number"]) > 0 {
							unitNumbers += materialItem["number"] + " + "
						}
					}

					// Remove trailing comma:
					if len(material) > 0 {
						material = material[:len(material)-3]
					}

					if len(unitNumbers) > 0 {
						unitNumbers = unitNumbers[:len(unitNumbers)-3]
					}

					row = append(row, material)
					row = append(row, unitNumbers)
				}

				w.Write(row)

				if stopCounter%40000 == 0 {
					progressNumber := float64(serviceCounter) / float64(serviceCount) * 100
					progress := fmt.Sprintf("%.2f", progressNumber)
					log.WithFields(log.Fields{"services": serviceCounter, "stops": stopCounter, "progress": progress}).Info("Dumping...")

					// Take .2s timeout to prevent host from getting overloaded:
					time.Sleep(200 * time.Millisecond)

					// flush csv
					w.Flush()

					if gzipCompression {
						zipWriter.Flush()
					}
				}
			}

			stopRows.Close()
		}()
	}

	progressNumber := float64(serviceCounter) / float64(serviceCount) * 100
	progress := fmt.Sprintf("%.2f", progressNumber)
	log.WithFields(log.Fields{"services": serviceCounter, "stops": stopCounter, "progress": progress}).Info("Dumping complete")

	return err
}
