package cmd

import (
	"github.com/rijdendetreinen/treinarchief-dumper/db"
	"github.com/spf13/cobra"
)

var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump",
}

var dumpDayCmd = &cobra.Command{
	Use:   "day",
	Short: "Dump a single day",
	Run: func(cmd *cobra.Command, args []string) {
		database := db.CreateDB()

		db.DumpServicesStops(database, args[0], args[0])
	},
}

func init() {
	RootCmd.AddCommand(dumpCmd)
	dumpCmd.AddCommand(dumpDayCmd)
}
