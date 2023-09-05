package cmd

import (
	"log"
	"os"

	"github.com/rijdendetreinen/treinarchief-dumper/dump"
	"github.com/spf13/cobra"
)

var FileName string
var DumpStdOut bool

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

		csvFile, err := createOutputFile()

		if err != nil {
			os.Exit(1)
		}

		defer csvFile.Close()
		dump.DumpServicesStops(database, csvFile, args[0], args[0])
	},
}

var dumpMonthCmd = &cobra.Command{
	Use:   "month MONTH",
	Short: "Dump a single month",
	Long:  "Dump a single month from the train archive. Specify date as YYYY-MM",
	Args:  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
	Run: func(cmd *cobra.Command, args []string) {
		database := dump.CreateDB()

		csvFile, err := createOutputFile()

		if err != nil {
			os.Exit(1)
		}

		defer csvFile.Close()
		dump.DumpServicesStops(database, csvFile, args[0]+"-01", args[0]+"-31")
	},
}

func createOutputFile() (*os.File, error) {
	if DumpStdOut {
		return os.Stdout, nil
	}

	csvFile, err := os.Create(FileName)

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
		return nil, err
	}

	return csvFile, nil
}

func init() {
	RootCmd.AddCommand(dumpCmd)
	dumpCmd.AddCommand(dumpDayCmd)
	dumpCmd.AddCommand(dumpMonthCmd)

	dumpCmd.PersistentFlags().BoolVar(&DumpStdOut, "stdout", false, "dump to stdout")
	dumpCmd.PersistentFlags().StringVarP(&FileName, "filename", "f", "dump.csv", "filename")
	dumpCmd.MarkFlagsMutuallyExclusive("stdout", "filename")
}
