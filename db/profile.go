package db

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"mysqlfake/metrics"
	"mysqlfake/sql"
)

// Profile 是一条语句的执行画像 (Session.LastProfile)。耗时都是墙钟时间。
type Profile struct {
	SQL          string
	Type         string // select insert update delete ddl txn explain other
	Parse        time.Duration
	Plan         time.Duration
	Exec         time.Duration // 执行 (不含解析和规划，含等锁)
	LockWait     time.Duration // 其中因等待行锁/元数据锁而阻塞的时间
	Total        time.Duration
	RowsExamined int64 // 从存储扫描读出的行数 (过滤之前)
	RowsSent     int64
	Err          error
}

func stmtType(st sql.Statement) string {
	switch st.(type) {
	case *sql.SelectStmt:
		return "select"
	case *sql.InsertStmt:
		return "insert"
	case *sql.UpdateStmt:
		return "update"
	case *sql.DeleteStmt:
		return "delete"
	case *sql.CreateTableStmt, *sql.CreateIndexStmt, *sql.AlterTableStmt, *sql.AnalyzeStmt, *sql.BackupStmt:
		return "ddl"
	case *sql.BeginStmt, *sql.CommitStmt, *sql.RollbackStmt:
		return "txn"
	case *sql.ExplainStmt:
		return "explain"
	}
	return "other"
}

// slowLog 把超过阈值的语句追加写入文件 (MySQL 慢查询日志风格)。
type slowLog struct {
	threshold time.Duration
	mu        sync.Mutex
	w         io.Writer
	closer    io.Closer
}

func openSlowLog(threshold time.Duration, path string) (*slowLog, error) {
	if threshold <= 0 {
		return nil, nil
	}
	l := &slowLog{threshold: threshold, w: os.Stderr}
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		l.w, l.closer = f, f
	}
	return l, nil
}

func (l *slowLog) record(p *Profile) {
	if l == nil || p.Total < l.threshold {
		return
	}
	metrics.SlowQueries.Inc()
	sec := func(d time.Duration) string { return fmt.Sprintf("%.6f", d.Seconds()) }
	var b strings.Builder
	fmt.Fprintf(&b, "# Time: %s\n", time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"))
	fmt.Fprintf(&b, "# Query_time: %s  Lock_time: %s  Rows_sent: %d  Rows_examined: %d  Parse: %s  Plan: %s  Exec: %s\n",
		sec(p.Total), sec(p.LockWait), p.RowsSent, p.RowsExamined, sec(p.Parse), sec(p.Plan), sec(p.Exec))
	if p.Err != nil {
		fmt.Fprintf(&b, "# Error: %s\n", strings.ReplaceAll(p.Err.Error(), "\n", " "))
	}
	q := strings.TrimSpace(p.SQL)
	if !strings.HasSuffix(q, ";") {
		q += ";"
	}
	b.WriteString(q + "\n")
	l.mu.Lock()
	io.WriteString(l.w, b.String())
	l.mu.Unlock()
}

func (l *slowLog) close() {
	if l != nil && l.closer != nil {
		l.closer.Close()
	}
}

// LastProfile 返回本会话最近一条语句的执行画像。
func (s *Session) LastProfile() *Profile { return s.lastProf }
