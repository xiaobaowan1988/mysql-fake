package sql

import (
	"fmt"
	"strconv"

	"mysqlfake/tuple"
)

type parser struct {
	toks []token
	pos  int
}

// Parse 解析单条 SQL 语句 (末尾分号可选)。
func Parse(src string) (Statement, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	st, err := p.statement()
	if err != nil {
		return nil, err
	}
	p.acceptSym(";")
	if p.peek().kind != tEOF {
		return nil, p.errf("unexpected %q", p.peek().text)
	}
	return st, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }
func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tEOF {
		p.pos++
	}
	return t
}

func (p *parser) errf(format string, a ...any) error {
	return fmt.Errorf("syntax error at %d: %s", p.peek().pos, fmt.Sprintf(format, a...))
}

func (p *parser) isKw(kw string) bool {
	t := p.peek()
	return t.kind == tKeyword && t.text == kw
}

func (p *parser) acceptKw(kw string) bool {
	if p.isKw(kw) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectKw(kw string) error {
	if !p.acceptKw(kw) {
		return p.errf("expected %s, got %q", kw, p.peek().text)
	}
	return nil
}

func (p *parser) acceptSym(s string) bool {
	t := p.peek()
	if t.kind == tSymbol && t.text == s {
		p.pos++
		return true
	}
	return false
}

func (p *parser) expectSym(s string) error {
	if !p.acceptSym(s) {
		return p.errf("expected %q, got %q", s, p.peek().text)
	}
	return nil
}

func (p *parser) ident() (string, error) {
	t := p.peek()
	if t.kind != tIdent {
		return "", p.errf("expected identifier, got %q", t.text)
	}
	p.pos++
	return t.text, nil
}

func (p *parser) statement() (Statement, error) {
	switch {
	case p.acceptKw("CREATE"):
		return p.createTable()
	case p.acceptKw("INSERT"):
		return p.insert()
	case p.acceptKw("SELECT"):
		return p.selectStmt()
	case p.acceptKw("SHOW"):
		if err := p.expectKw("TABLES"); err != nil {
			return nil, err
		}
		return &ShowTablesStmt{}, nil
	}
	return nil, p.errf("unsupported statement %q", p.peek().text)
}

func (p *parser) createTable() (Statement, error) {
	if err := p.expectKw("TABLE"); err != nil {
		return nil, err
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym("("); err != nil {
		return nil, err
	}
	st := &CreateTableStmt{Table: name}
	for {
		col, err := p.ident()
		if err != nil {
			return nil, err
		}
		def := ColumnDef{Name: col}
		switch {
		case p.acceptKw("INT"), p.acceptKw("INTEGER"), p.acceptKw("BIGINT"):
			def.Type = tuple.TypeInt
		case p.acceptKw("VARCHAR"):
			def.Type = tuple.TypeVarchar
			if err := p.expectSym("("); err != nil {
				return nil, err
			}
			t := p.next()
			n, perr := strconv.Atoi(t.text)
			if t.kind != tInt || perr != nil || n <= 0 || n > 1000 {
				return nil, p.errf("invalid VARCHAR length %q (1..1000)", t.text)
			}
			def.Len = n
			if err := p.expectSym(")"); err != nil {
				return nil, err
			}
		default:
			return nil, p.errf("expected column type, got %q", p.peek().text)
		}
		if p.acceptKw("PRIMARY") {
			if err := p.expectKw("KEY"); err != nil {
				return nil, err
			}
			def.PrimaryKey = true
		}
		st.Columns = append(st.Columns, def)
		if p.acceptSym(",") {
			continue
		}
		break
	}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	return st, nil
}

func (p *parser) insert() (Statement, error) {
	if err := p.expectKw("INTO"); err != nil {
		return nil, err
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	st := &InsertStmt{Table: name}
	if p.acceptSym("(") {
		for {
			c, err := p.ident()
			if err != nil {
				return nil, err
			}
			st.Columns = append(st.Columns, c)
			if !p.acceptSym(",") {
				break
			}
		}
		if err := p.expectSym(")"); err != nil {
			return nil, err
		}
	}
	if err := p.expectKw("VALUES"); err != nil {
		return nil, err
	}
	for {
		if err := p.expectSym("("); err != nil {
			return nil, err
		}
		var row []Expr
		for {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			row = append(row, e)
			if !p.acceptSym(",") {
				break
			}
		}
		if err := p.expectSym(")"); err != nil {
			return nil, err
		}
		st.Rows = append(st.Rows, row)
		if !p.acceptSym(",") {
			break
		}
	}
	return st, nil
}

func (p *parser) selectStmt() (Statement, error) {
	st := &SelectStmt{}
	if p.acceptSym("*") {
		st.Star = true
	} else {
		for {
			c, err := p.ident()
			if err != nil {
				return nil, err
			}
			st.Items = append(st.Items, c)
			if !p.acceptSym(",") {
				break
			}
		}
	}
	if err := p.expectKw("FROM"); err != nil {
		return nil, err
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	st.Table = name
	if p.acceptKw("WHERE") {
		if st.Where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// 表达式优先级: OR < AND < NOT < 比较/IS NULL < 原子
func (p *parser) expr() (Expr, error) { return p.orExpr() }

func (p *parser) orExpr() (Expr, error) {
	l, err := p.andExpr()
	if err != nil {
		return nil, err
	}
	for p.acceptKw("OR") {
		r, err := p.andExpr()
		if err != nil {
			return nil, err
		}
		l = &BinaryExpr{Op: "OR", Left: l, Right: r}
	}
	return l, nil
}

func (p *parser) andExpr() (Expr, error) {
	l, err := p.notExpr()
	if err != nil {
		return nil, err
	}
	for p.acceptKw("AND") {
		r, err := p.notExpr()
		if err != nil {
			return nil, err
		}
		l = &BinaryExpr{Op: "AND", Left: l, Right: r}
	}
	return l, nil
}

func (p *parser) notExpr() (Expr, error) {
	if p.acceptKw("NOT") {
		e, err := p.notExpr()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: e}, nil
	}
	return p.cmpExpr()
}

func (p *parser) cmpExpr() (Expr, error) {
	l, err := p.atom()
	if err != nil {
		return nil, err
	}
	if p.acceptKw("IS") {
		not := p.acceptKw("NOT")
		if err := p.expectKw("NULL"); err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: l, Not: not}, nil
	}
	t := p.peek()
	if t.kind == tSymbol {
		switch t.text {
		case "=", "!=", "<", "<=", ">", ">=":
			p.pos++
			r, err := p.atom()
			if err != nil {
				return nil, err
			}
			return &BinaryExpr{Op: t.text, Left: l, Right: r}, nil
		}
	}
	return l, nil
}

func (p *parser) atom() (Expr, error) {
	t := p.peek()
	switch {
	case t.kind == tInt:
		p.pos++
		return p.intLiteral(t.text, false)
	case t.kind == tSymbol && t.text == "-":
		p.pos++
		n := p.peek()
		if n.kind != tInt {
			return nil, p.errf("expected number after '-'")
		}
		p.pos++
		return p.intLiteral(n.text, true)
	case t.kind == tString:
		p.pos++
		return &Literal{tuple.StrValue(t.text)}, nil
	case t.kind == tKeyword && t.text == "NULL":
		p.pos++
		return &Literal{tuple.Value{Null: true}}, nil
	case t.kind == tIdent:
		p.pos++
		return &ColumnRef{Name: t.text}, nil
	case t.kind == tSymbol && t.text == "(":
		p.pos++
		e, err := p.expr()
		if err != nil {
			return nil, err
		}
		return e, p.expectSym(")")
	}
	return nil, p.errf("unexpected %q in expression", t.text)
}

func (p *parser) intLiteral(s string, neg bool) (Expr, error) {
	if neg {
		s = "-" + s
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, p.errf("integer out of range: %s", s)
	}
	return &Literal{tuple.IntValue(v)}, nil
}
