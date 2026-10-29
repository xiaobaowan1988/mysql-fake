package metrics

// 引擎各层使用的指标 (登记在 Default 注册表里)。
var (
	// SQL 层
	Statements       = Default.CounterVec("mysqlfake_statements_total", "Statements executed, by type and status (rate() of this is the QPS).", "type", "status")
	StatementSeconds = Default.HistogramVec("mysqlfake_statement_duration_seconds", "Statement latency in seconds, by type.", DefBuckets, "type")
	RowsSent         = Default.Counter("mysqlfake_rows_sent_total", "Rows returned to clients.")
	RowsExamined     = Default.Counter("mysqlfake_rows_examined_total", "Rows read from storage by scans (before filtering).")
	SlowQueries      = Default.Counter("mysqlfake_slow_queries_total", "Statements slower than the slow query threshold.")

	// 缓冲池
	BufferHits      = Default.Counter("mysqlfake_buffer_pool_hits_total", "Page fetches served from the buffer pool.")
	BufferMisses    = Default.Counter("mysqlfake_buffer_pool_misses_total", "Page fetches that had to read from disk.")
	BufferEvictions = Default.Counter("mysqlfake_buffer_pool_evictions_total", "Pages evicted from the buffer pool.")
	Checkpoints     = Default.Counter("mysqlfake_checkpoints_total", "Fuzzy checkpoints completed.")

	// WAL
	WALBytes  = Default.Counter("mysqlfake_wal_written_bytes_total", "Bytes written to the write-ahead log.")
	WALFsyncs = Default.Counter("mysqlfake_wal_fsyncs_total", "fsync calls on the write-ahead log.")

	// 事务与锁
	TxnCommits   = Default.Counter("mysqlfake_txn_commits_total", "Transactions committed.")
	TxnRollbacks = Default.Counter("mysqlfake_txn_rollbacks_total", "Transactions rolled back.")
	LockWaits    = Default.Counter("mysqlfake_lock_waits_total", "Lock requests that had to wait.")
	LockWaitTime = Default.Histogram("mysqlfake_lock_wait_seconds", "Time spent waiting for row/metadata locks.", DefBuckets)
	Deadlocks    = Default.Counter("mysqlfake_deadlocks_total", "Deadlocks detected (victim transactions).")
	LockTimeouts = Default.Counter("mysqlfake_lock_wait_timeouts_total", "Lock waits that timed out.")

	// 网络
	ConnAccepted = Default.Counter("mysqlfake_connections_accepted_total", "Client connections accepted.")
	ConnRejected = Default.Counter("mysqlfake_connections_rejected_total", "Client connections rejected (max connections).")
)
