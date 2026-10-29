package sql

import (
	"fmt"
	"strings"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tKeyword
	tInt
	tString
	tSymbol
)

type token struct {
	kind tokKind
	text string // 关键字为大写；标识符保持原样
	pos  int
}

var keywords = map[string]bool{
	"CREATE": true, "TABLE": true, "INSERT": true, "INTO": true, "VALUES": true,
	"SELECT": true, "FROM": true, "WHERE": true, "AND": true, "OR": true, "NOT": true,
	"NULL": true, "IS": true, "PRIMARY": true, "KEY": true, "INT": true, "INTEGER": true,
	"BIGINT": true, "VARCHAR": true, "SHOW": true, "TABLES": true,
	"UPDATE": true, "SET": true, "DELETE": true, "BEGIN": true, "START": true,
	"TRANSACTION": true, "COMMIT": true, "ROLLBACK": true,
	"JOIN": true, "INNER": true, "ON": true, "AS": true, "ORDER": true, "BY": true, "LIMIT": true,
	"ASC": true, "DESC": true, "EXPLAIN": true, "ANALYZE": true,
	"INDEX": true, "ALTER": true, "BACKUP": true, "TO": true, "INCREMENTAL": true, "ADD": true, "COLUMN": true, "DEFAULT": true,
}

func lex(src string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(src) {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '-' && i+1 < len(src) && src[i+1] == '-':
			for i < len(src) && src[i] != '\n' {
				i++
			}
		case isAlpha(c):
			j := i
			for j < len(src) && (isAlpha(src[j]) || isDigit(src[j])) {
				j++
			}
			w := src[i:j]
			if up := strings.ToUpper(w); keywords[up] {
				toks = append(toks, token{tKeyword, up, i})
			} else {
				toks = append(toks, token{tIdent, w, i})
			}
			i = j
		case c == '`':
			j := strings.IndexByte(src[i+1:], '`')
			if j < 0 {
				return nil, fmt.Errorf("syntax error at %d: unterminated identifier", i)
			}
			toks = append(toks, token{tIdent, src[i+1 : i+1+j], i})
			i += j + 2
		case isDigit(c):
			j := i
			for j < len(src) && isDigit(src[j]) {
				j++
			}
			toks = append(toks, token{tInt, src[i:j], i})
			i = j
		case c == '\'' || c == '"':
			// 字符串字面量: 支持引号加倍 ('' / "") 和 MySQL 的反斜杠转义
			quote := c
			var sb strings.Builder
			j := i + 1
			closed := false
			for j < len(src) {
				ch := src[j]
				switch {
				case ch == quote:
					if j+1 < len(src) && src[j+1] == quote {
						sb.WriteByte(quote)
						j += 2
						continue
					}
					closed = true
					j++
				case ch == '\\' && j+1 < len(src):
					sb.WriteByte(unescape(src[j+1]))
					j += 2
					continue
				default:
					sb.WriteByte(ch)
					j++
					continue
				}
				break
			}
			if !closed {
				return nil, fmt.Errorf("syntax error at %d: unterminated string", i)
			}
			toks = append(toks, token{tString, sb.String(), i})
			i = j
		default:
			two := ""
			if i+1 < len(src) {
				two = src[i : i+2]
			}
			switch two {
			case "<=", ">=", "!=", "<>":
				if two == "<>" {
					two = "!="
				}
				toks = append(toks, token{tSymbol, two, i})
				i += 2
				continue
			}
			if strings.IndexByte("(),;=<>*-+.", c) >= 0 {
				toks = append(toks, token{tSymbol, string(c), i})
				i++
				continue
			}
			return nil, fmt.Errorf("syntax error at %d: unexpected character %q", i, c)
		}
	}
	toks = append(toks, token{tEOF, "", len(src)})
	return toks, nil
}

func isAlpha(c byte) bool { return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') }
func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// unescape 返回反斜杠转义序列 \x 对应的字节 (未知转义保持字符本身，与 MySQL 一致)。
func unescape(c byte) byte {
	switch c {
	case 'n':
		return '\n'
	case 't':
		return '\t'
	case 'r':
		return '\r'
	case '0':
		return 0
	case 'b':
		return '\b'
	case 'Z':
		return 26
	}
	return c
}
