package db

import (
	"github.com/coreos/bbolt"
	"blockchaininfo-test-task/tools"
)

func InitDB(boltDBName string) *bolt.DB {
	// bolt DB name can be customized in conf.json
	CashDB, err := bolt.Open(boltDBName, 0600, nil)
	tools.Check("Error while open bolt DB", err)
	return CashDB
}
