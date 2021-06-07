package postgres

// Config holds etcd configuration alongside with configuration related to our higher level interface.
type Config struct {
	Dsn string `long:"dsn" description:"Database connection string."`
}
