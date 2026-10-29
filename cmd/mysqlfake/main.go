// mysqlfake 是一个交互式 REPL: mysqlfake [-db 文件路径]
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"mysqlfake/db"
)

func main() {
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
