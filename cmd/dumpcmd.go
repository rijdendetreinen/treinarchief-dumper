package cmd

import (
	"os"
	"strings"

	"github.com/rijdendetreinen/treinarchief-dumper/dump"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var FileName string
var DumpStdOut bool
var GzipCompression bool

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump",
}

var dumpDayCmd = &cobra.Command{
	Use:   "day DATE",
	Short: "Dump a single day",
	Long:  "Dump a single day from the train archive. Specify date as YYYY-MM-DD",
	Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		database := dump.CreateDB()

		csvFile, err := createOutputFile("services-" + args[0])

		if err != nil {
			os.Exit(1)
		}

		defer csvFile.Close()
		dump.DumpServicesStops(database, csvFile, GzipCompression, args[0], args[0])
	},
}

var dumpMonthCmd = &cobra.Command{
	Use:   "month MONTH",
	Short: "Dump a single month",
	Long:  "Dump a single month from the train archive. Specify month as YYYY-MM",
	Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		database := dump.CreateDB()

		csvFile, err := createOutputFile("services-" + args[0])

		if err != nil {
			os.Exit(1)
		}

		defer csvFile.Close()
		dump.DumpServicesStops(database, csvFile, GzipCompression, args[0]+"-01", args[0]+"-31")
	},
}

var dumpYearCmd = &cobra.Command{
	Use:   "year MONTH",
	Short: "Dump a single year",
	Long:  "Dump a single year from the train archive. Specify year as YYYY",
	Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		database := dump.CreateDB()

		csvFile, err := createOutputFile("services-" + args[0])

		if err != nil {
			os.Exit(1)
		}

		defer csvFile.Close()
		dump.DumpServicesStops(database, csvFile, GzipCompression, args[0]+"-01-01", args[0]+"-12-31")
	},
}

func createOutputFile(defaultFileName string) (*os.File, error) {
	if DumpStdOut {
		return os.Stdout, nil
	}

	if FileName == "" {
		FileName = defaultFileName
	}

	if !strings.HasSuffix(FileName, ".csv") && !strings.HasSuffix(FileName, ".csv.gz") {
		FileName += ".csv"
	}

	if !strings.HasSuffix(FileName, ".gz") && GzipCompression {
		FileName += ".gz"
	}

	csvFile, err := os.Create(FileName)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
		return nil, err
	}

	log.Info("Writing to ", FileName)

	return csvFile, nil
}

func init() {
	RootCmd.AddCommand(dumpCmd)
	dumpCmd.AddCommand(dumpDayCmd)
	dumpCmd.AddCommand(dumpMonthCmd)
	dumpCmd.AddCommand(dumpYearCmd)

	dumpCmd.PersistentFlags().BoolVar(&DumpStdOut, "stdout", false, "dump to stdout")
	dumpCmd.PersistentFlags().BoolVar(&GzipCompression, "gzip", true, "gzip")
	dumpCmd.PersistentFlags().StringVarP(&FileName, "filename", "f", "", "filename")
	dumpCmd.MarkFlagsMutuallyExclusive("stdout", "filename")
}
