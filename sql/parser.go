package sql

import (
	"fmt"
	"strconv"
	"strings"

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
	case p.acceptKw("UPDATE"):
		return p.update()
	case p.acceptKw("DELETE"):
		return p.deleteStmt()
	case p.acceptKw("BEGIN"):
		return &BeginStmt{}, nil
	case p.acceptKw("START"):
		if err := p.expectKw("TRANSACTION"); err != nil {
			return nil, err
		}
		return &BeginStmt{}, nil
	case p.acceptKw("COMMIT"):
		return &CommitStmt{}, nil
	case p.acceptKw("ROLLBACK"):
		return &RollbackStmt{}, nil
	case p.acceptKw("ALTER"):
		return p.alterTable()
	case p.acceptKw("BACKUP"):
		if err := p.expectKw("TO"); err != nil {
			return nil, err
		}
		t := p.next()
		if t.kind != tString {
			return nil, p.errf("expected a quoted directory after TO")
		}
		st := &BackupStmt{Dir: t.text}
		if p.acceptKw("INCREMENTAL") {
			if err := p.expectKw("FROM"); err != nil {
				return nil, err
			}
			f := p.next()
			if f.kind != tString {
				return nil, p.errf("expected a quoted directory after FROM")
			}
			st.From = f.text
		}
		return st, nil
	case p.acceptKw("EXPLAIN"):
		analyze := p.acceptKw("ANALYZE")
		if err := p.expectKw("SELECT"); err != nil {
			return nil, err
		}
		sel, err := p.selectStmt()
		if err != nil {
			return nil, err
		}
		return &ExplainStmt{Select: sel.(*SelectStmt), Analyze: analyze}, nil
	case p.acceptKw("ANALYZE"):
		if err := p.expectKw("TABLE"); err != nil {
			return nil, err
		}
		name, err := p.ident()
		if err != nil {
			return nil, err
		}
		return &AnalyzeStmt{Table: name}, nil
	case p.acceptKw("SHOW"):
		if err := p.expectKw("TABLES"); err != nil {
			return nil, err
		}
		return &ShowTablesStmt{}, nil
	}
	return nil, p.errf("unsupported statement %q", p.peek().text)
}

func (p *parser) createIndex() (Statement, error) {
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectKw("ON"); err != nil {
		return nil, err
	}
	table, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym("("); err != nil {
		return nil, err
	}
	col, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectSym(")"); err != nil {
		return nil, err
	}
	return &CreateIndexStmt{Name: name, Table: table, Column: col}, nil
}

func (p *parser) alterTable() (Statement, error) {
	if err := p.expectKw("TABLE"); err != nil {
		return nil, err
	}
	table, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectKw("ADD"); err != nil {
		return nil, err
	}
	p.acceptKw("COLUMN")
	def, err := p.columnDef()
	if err != nil {
		return nil, err
	}
	if def.PrimaryKey {
		return nil, p.errf("cannot add a PRIMARY KEY column")
	}
	st := &AlterTableStmt{Table: table, Add: def}
	if p.acceptKw("DEFAULT") {
		e, err := p.atom()
		if err != nil {
			return nil, err
		}
		lit, ok := e.(*Literal)
		if !ok {
			return nil, p.errf("DEFAULT must be a literal")
		}
		st.Default = lit
	}
	return st, nil
}

func (p *parser) createTable() (Statement, error) {
	if p.acceptKw("INDEX") {
		return p.createIndex()
	}
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
		def, err := p.columnDef()
		if err != nil {
			return nil, err
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

// columnDef 解析 "name type [PRIMARY KEY]"。
func (p *parser) columnDef() (ColumnDef, error) {
	col, err := p.ident()
	if err != nil {
		return ColumnDef{}, err
	}
	def := ColumnDef{Name: col}
	switch {
	case p.acceptKw("INT"), p.acceptKw("INTEGER"), p.acceptKw("BIGINT"):
		def.Type = tuple.TypeInt
	case p.acceptKw("VARCHAR"):
		def.Type = tuple.TypeVarchar
		if err := p.expectSym("("); err != nil {
			return def, err
		}
		t := p.next()
		n, perr := strconv.Atoi(t.text)
		if t.kind != tInt || perr != nil || n <= 0 || n > 1000 {
			return def, p.errf("invalid VARCHAR length %q (1..1000)", t.text)
		}
		def.Len = n
		if err := p.expectSym(")"); err != nil {
			return def, err
		}
	default:
		return def, p.errf("expected column type, got %q", p.peek().text)
	}
	if p.acceptKw("PRIMARY") {
		if err := p.expectKw("KEY"); err != nil {
			return def, err
		}
		def.PrimaryKey = true
	}
	return def, nil
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

func (p *parser) colRef() (ColumnRef, error) {
	name, err := p.ident()
	if err != nil {
		return ColumnRef{}, err
	}
	if p.acceptSym(".") {
		col, err := p.ident()
		if err != nil {
			return ColumnRef{}, err
		}
		return ColumnRef{Table: name, Name: col}, nil
	}
	return ColumnRef{Name: name}, nil
}

func (p *parser) tableRef() (TableRef, error) {
	name, err := p.ident()
	if err != nil {
		return TableRef{}, err
	}
	ref := TableRef{Name: name}
	if p.acceptKw("AS") {
		if ref.Alias, err = p.ident(); err != nil {
			return TableRef{}, err
		}
	} else if p.peek().kind == tIdent && !p.isWord("FOR") {
		ref.Alias = p.next().text
	}
	return ref, nil
}

func (p *parser) isWord(w string) bool {
	t := p.peek()
	return (t.kind == tIdent || t.kind == tKeyword) && strings.EqualFold(t.text, w)
}

func (p *parser) selectStmt() (Statement, error) {
	st := &SelectStmt{Limit: -1}
	if p.acceptSym("*") {
		st.Star = true
	} else {
		for {
			c, err := p.colRef()
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
	first, err := p.tableRef()
	if err != nil {
		return nil, err
	}
	st.From = append(st.From, first)
	var onConds []Expr
	for {
		switch {
		case p.acceptSym(","):
		case p.acceptKw("INNER"):
			if err := p.expectKw("JOIN"); err != nil {
				return nil, err
			}
		case p.acceptKw("JOIN"):
		default:
			goto joined
		}
		{
			t, err := p.tableRef()
			if err != nil {
				return nil, err
			}
			st.From = append(st.From, t)
			if p.acceptKw("ON") {
				c, err := p.expr()
				if err != nil {
					return nil, err
				}
				onConds = append(onConds, c)
			}
		}
	}
joined:
	if st.Where, err = p.optWhere(); err != nil {
		return nil, err
	}
	for _, c := range onConds { // ON 条件与 WHERE 合并 (INNER JOIN 语义等价)
		if st.Where == nil {
			st.Where = c
		} else {
			st.Where = &BinaryExpr{Op: "AND", Left: c, Right: st.Where}
		}
	}
	if p.acceptKw("ORDER") {
		if err := p.expectKw("BY"); err != nil {
			return nil, err
		}
		for {
			c, err := p.colRef()
			if err != nil {
				return nil, err
			}
			item := OrderItem{Col: c}
			if p.acceptKw("DESC") {
				item.Desc = true
			} else {
				p.acceptKw("ASC")
			}
			st.OrderBy = append(st.OrderBy, item)
			if !p.acceptSym(",") {
				break
			}
		}
	}
	if p.acceptKw("LIMIT") {
		t := p.next()
		n, perr := strconv.Atoi(t.text)
		if t.kind != tInt || perr != nil {
			return nil, p.errf("expected a non-negative integer after LIMIT")
		}
		st.Limit = n
	}
	if p.acceptWord("FOR") {
		switch {
		case p.acceptKw("UPDATE"):
			st.Lock = LockUpdate
		case p.acceptWord("SHARE"):
			st.Lock = LockShare
		default:
			return nil, p.errf("expected UPDATE or SHARE after FOR")
		}
	}
	return st, nil
}

func (p *parser) optWhere() (Expr, error) {
	if p.acceptKw("WHERE") {
		return p.expr()
	}
	return nil, nil
}

// acceptWord 匹配上下文关键字 (FOR/SHARE)，它们仍可用作普通标识符。
func (p *parser) acceptWord(w string) bool {
	t := p.peek()
	if (t.kind == tIdent || t.kind == tKeyword) && strings.EqualFold(t.text, w) {
		p.pos++
		return true
	}
	return false
}

func (p *parser) update() (Statement, error) {
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	if err := p.expectKw("SET"); err != nil {
		return nil, err
	}
	st := &UpdateStmt{Table: name}
	for {
		col, err := p.ident()
		if err != nil {
			return nil, err
		}
		if err := p.expectSym("="); err != nil {
			return nil, err
		}
		e, err := p.addExpr()
		if err != nil {
			return nil, err
		}
		st.Sets = append(st.Sets, Assignment{Column: col, Value: e})
		if !p.acceptSym(",") {
			break
		}
	}
	if st.Where, err = p.optWhere(); err != nil {
		return nil, err
	}
	return st, nil
}

func (p *parser) deleteStmt() (Statement, error) {
	if err := p.expectKw("FROM"); err != nil {
		return nil, err
	}
	name, err := p.ident()
	if err != nil {
		return nil, err
	}
	st := &DeleteStmt{Table: name}
	if st.Where, err = p.optWhere(); err != nil {
		return nil, err
	}
	return st, nil
}

// 表达式优先级: OR < AND < NOT < 比较/IS NULL < 加减 < 乘 < 原子
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

func (p *parser) addExpr() (Expr, error) {
	l, err := p.mulExpr()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t.kind == tSymbol && (t.text == "+" || t.text == "-") {
			p.pos++
			r, err := p.mulExpr()
			if err != nil {
				return nil, err
			}
			l = &BinaryExpr{Op: t.text, Left: l, Right: r}
			continue
		}
		return l, nil
	}
}

func (p *parser) mulExpr() (Expr, error) {
	l, err := p.atom()
	if err != nil {
		return nil, err
	}
	for p.acceptSym("*") {
		r, err := p.atom()
		if err != nil {
			return nil, err
		}
		l = &BinaryExpr{Op: "*", Left: l, Right: r}
	}
	return l, nil
}

func (p *parser) cmpExpr() (Expr, error) {
	l, err := p.addExpr()
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
			r, err := p.addExpr()
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
		c, err := p.colRef()
		if err != nil {
			return nil, err
		}
		return &c, nil
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
