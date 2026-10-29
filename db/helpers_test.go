package db

import (
	"errors"

	"mysqlfake/binlog"
)

var errTest = errors.New("test error")

type binlogTxn = binlog.Txn
