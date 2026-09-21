// The load harness lives in its own module so vegeta's dependency tree stays
// out of the service's supply chain. It drives the running API over HTTP and
// PostgreSQL only, and imports no jungle package.
module jungle/tests/load

go 1.26.4

require (
	github.com/jackc/pgx/v5 v5.11.0
	github.com/tsenart/vegeta/v12 v12.13.0
)

require (
	github.com/influxdata/tdigest v0.0.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/rs/dnscache v0.0.0-20230804202142-fc85eb664529 // indirect
	golang.org/x/net v0.27.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)
