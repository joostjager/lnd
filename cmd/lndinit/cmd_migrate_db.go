package main

import (
	"fmt"

	"github.com/jessevdk/go-flags"
	"github.com/lightningnetwork/lnd/kvdb"
	"github.com/lightningnetwork/lnd/kvdb/etcd"
	"github.com/lightningnetwork/lnd/kvdb/postgres"
	"github.com/lightningnetwork/lnd/lncfg"
	"github.com/lightningnetwork/lnd/signal"
)

type migrateDBCommand struct {
	Source     *lncfg.DB `long:"source" short:"s" description:"The source database where the data is read from; will be deleted by default (after successful migration) to avoid keeping old channel state around"`
	Dest       *lncfg.DB `long:"dest" short:"d" description:"The destination database where the data is written to"`
	KeepSource bool      `long:"keep-source" description:"Don't delete the data in the source database after successful migration'"`
}

func newMigrateDBCommand() *migrateDBCommand {
	return &migrateDBCommand{
		Source: &lncfg.DB{
			Backend: lncfg.BoltBackend,
			Etcd:    &etcd.Config{},
			Bolt: &kvdb.BoltConfig{
				DBTimeout: kvdb.DefaultDBTimeout,
			},
			Postgres: &postgres.Config{},
		},
		Dest: &lncfg.DB{
			Backend: lncfg.EtcdBackend,
			Etcd:    &etcd.Config{},
			Bolt: &kvdb.BoltConfig{
				DBTimeout: kvdb.DefaultDBTimeout,
			},
			Postgres: &postgres.Config{},
		},
	}
}

func (x *migrateDBCommand) Register(parser *flags.Parser) error {
	_, err := parser.AddCommand(
		"migrate-db",
		"Migrate an lnd source DB to a destination DB",
		"Migrate an lnd source database (for example the bbolt based "+
			"channel.db) to a destination database (for example "+
			"a PostgreSQL database)",
		x,
	)
	return err
}

func (x *migrateDBCommand) Execute(_ []string) error {
	// Since this will potentially run for a while, make sure we catch any
	// interrupt signals.
	_, err := signal.Intercept()
	if err != nil {
		return fmt.Errorf("error intercepting signals: %v", err)
	}

	return nil
}
