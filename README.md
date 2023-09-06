# treinarchief-dumper

This is an internal tool, used to dump data from the[Rijden de Treinen train archive](https://www.rijdendetreinen.nl/treinarchief)
([English vesion](https://www.rijdendetreinen.nl/en/train-archive)) to a CSV file.

This tool directly selects data from the MySQL database behind the train archive. It selects all services within the given period,
and then iterates over each service to add the service stops to a CSV file.

The resulting CSV files are distributed as [open data](https://www.rijdendetreinen.nl/over/open-data).
