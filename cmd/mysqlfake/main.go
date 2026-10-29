// mysqlfake: 交互式 REPL 或 MySQL 协议服务。
//
//	mysqlfake [-db 文件路径]                      本地 REPL
//	mysqlfake serve [-db 文件路径] [-addr 地址]   启动 MySQL 协议服务 (mysql / PyMySQL 等客户端可连接)
//	mysqlfake restore -out 目录 -backup 全量备份 [-backup 增量备份 ...] [-binlog 路径 -until 时间点 | -until-seq 序号]
//	                                              离线还原备份链，并可重放 Binlog 做 PITR
//
// 备份用 SQL 触发: BACKUP TO '目录' [INCREMENTAL FROM '上一份备份目录']
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"mysqlfake/db"
	"mysqlfake/metrics"
	"mysqlfake/server"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			serve(os.Args[2:])
			return
		case "restore":
			restore(os.Args[2:])
			return
		}
	}
	path := flag.String("db", "mysqlfake.db", "database file")
	flag.Parse()
	eng, err := db.Open(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open database:", err)
		os.Exit(1)
	}
	defer eng.Close()
	repl(eng, os.Stdin, os.Stdout, isTerminal(os.Stdin))
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func repl(eng *db.Engine, in *os.File, out *os.File, interactive bool) {
	prompt := func(cont bool) {
		if !interactive {
			return
		}
		if cont {
			fmt.Fprint(out, "    -> ")
		} else {
			fmt.Fprint(out, "mysql> ")
		}
	}
	if interactive {
		fmt.Fprintln(out, "mysqlfake — type SQL ending with ';', 'exit' to quit.")
	}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	var buf strings.Builder
	prompt(false)
	for sc.Scan() {
		line := sc.Text()
		if buf.Len() == 0 {
			switch strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(line), ";"))) {
			case "exit", "quit", `\q`:
				return
			case "":
				prompt(false)
				continue
			}
		}
		buf.WriteString(line + "\n")
		stmts, rest := splitStatements(buf.String())
		for _, s := range stmts {
			res, err := eng.Exec(s)
			if err != nil {
				fmt.Fprintln(out, "ERROR:", err)
				continue
			}
			fmt.Fprint(out, res.Format())
		}
		buf.Reset()
		buf.WriteString(rest)
		prompt(strings.TrimSpace(rest) != "")
	}
}

// splitStatements 按分号切分 (忽略字符串内的分号)，返回完整语句和未结束的剩余文本。
func splitStatements(src string) (stmts []string, rest string) {
	inStr := false
	start := 0
	for i := 0; i < len(src); i++ {
		switch c := src[i]; {
		case c == '\'':
			inStr = !inStr
		case c == ';' && !inStr:
			if s := strings.TrimSpace(src[start:i]); s != "" {
				stmts = append(stmts, s)
			}
			start = i + 1
		}
	}
	return stmts, src[start:]
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	path := fs.String("db", "mysqlfake.db", "database file")
	addr := fs.String("addr", "127.0.0.1:3307", "listen address")
	mode := fs.String("mode", "auto", "I/O model: auto | eventloop | goroutine")
	maxWorkers := fs.Int("max-workers", 64, "max concurrently executing statements")
	maxConns := fs.Int("max-conns", 0, "max connections (0 = unlimited)")
	metricsAddr := fs.String("metrics-addr", "", "serve Prometheus metrics at http://ADDR/metrics (e.g. 127.0.0.1:9100)")
	slowMs := fs.Int("slow-ms", 0, "log statements slower than this many milliseconds (0 = off)")
	slowLog := fs.String("slow-log", "", "slow query log file (default: stderr)")
	fs.Parse(args)
	eng, err := db.OpenWithOptions(*path, db.Options{SlowQueryThreshold: time.Duration(*slowMs) * time.Millisecond, SlowQueryLog: *slowLog})
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot open database:", err)
		os.Exit(1)
	}
	srv := server.New(server.Config{Addr: *addr, Engine: eng, Mode: server.Mode(*mode), MaxWorkers: *maxWorkers, MaxConns: *maxConns})
	if err := srv.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "cannot start server:", err)
		eng.Close()
		os.Exit(1)
	}
	fmt.Printf("mysqlfake listening on %s (%s)\n", srv.Addr(), srv.EffectiveMode())
	if *metricsAddr != "" {
		go func() {
			if err := http.ListenAndServe(*metricsAddr, metrics.Default.Handler()); err != nil {
				fmt.Fprintln(os.Stderr, "metrics endpoint:", err)
			}
		}()
		fmt.Printf("metrics on http://%s/metrics\n", *metricsAddr)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Close()
	eng.Close()
}

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func restore(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	out := fs.String("out", "", "directory to restore into (must not contain a database yet)")
	var chain listFlag
	fs.Var(&chain, "backup", "backup directory; repeat for the chain: full first, then incrementals in order")
	binlogPath := fs.String("binlog", "", "binlog file to replay for point-in-time recovery")
	until := fs.String("until", "", "recover up to this time (RFC3339)")
	untilSeq := fs.Uint64("until-seq", 0, "recover up to this binlog sequence number")
	fs.Parse(args)
	if *out == "" || len(chain) == 0 {
		fs.Usage()
		os.Exit(2)
	}
	var target db.RecoveryTarget
	if *until != "" {
		t, err := time.Parse(time.RFC3339Nano, *until)
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad -until:", err)
			os.Exit(2)
		}
		target.Time = t
	}
	target.Seq = *untilSeq
	n, err := db.RestoreBackups(*out, chain, *binlogPath, target)
	if err != nil {
		fmt.Fprintln(os.Stderr, "restore failed:", err)
		os.Exit(1)
	}
	fmt.Printf("restored %d backup(s) into %s; replayed %d binlog transaction(s)\n", len(chain), *out, n)
}
