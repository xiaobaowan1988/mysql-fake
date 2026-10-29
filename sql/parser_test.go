package sql

import (
	"fmt"
	"testing"

	"mysqlfake/tuple"
)

func TestParseCreate(t *testing.T) {
	st, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(32), age bigint);")
	if err != nil {
		t.Fatal(err)
	}
	c := st.(*CreateTableStmt)
	if c.Table != "users" || len(c.Columns) != 3 || !c.Columns[0].PrimaryKey ||
		c.Columns[1].Type != tuple.TypeVarchar || c.Columns[1].Len != 32 || c.Columns[2].Type != tuple.TypeInt {
		t.Fatalf("bad ast: %+v", c)
	}
}

func TestParseInsert(t *testing.T) {
	st, err := Parse("insert into t (id, name) values (1, 'it''s'), (-2, NULL)")
	if err != nil {
		t.Fatal(err)
	}
	i := st.(*InsertStmt)
	if len(i.Rows) != 2 || len(i.Columns) != 2 {
		t.Fatalf("bad ast: %+v", i)
	}
	if i.Rows[0][1].(*Literal).Value.Str != "it's" || i.Rows[1][0].(*Literal).Value.Int != -2 ||
		!i.Rows[1][1].(*Literal).Value.Null {
		t.Fatalf("bad literals: %+v", i.Rows)
	}
}

func TestParseSelect(t *testing.T) {
	st, err := Parse("SELECT id, name FROM t WHERE id >= 3 AND (name = 'a' OR NOT age IS NULL) ")
	if err != nil {
		t.Fatal(err)
	}
	s := st.(*SelectStmt)
	if s.Star || len(s.Items) != 2 || len(s.From) != 1 || s.From[0].Name != "t" || s.Limit != -1 {
		t.Fatalf("bad ast: %+v", s)
	}
	and := s.Where.(*BinaryExpr)
	if and.Op != "AND" || and.Left.(*BinaryExpr).Op != ">=" {
		t.Fatalf("bad where: %+v", and)
	}
	if or := and.Right.(*BinaryExpr); or.Op != "OR" {
		t.Fatalf("bad precedence: %+v", or)
	}
	if st, err = Parse("select * from t"); err != nil || !st.(*SelectStmt).Star {
		t.Fatal("select star failed")
	}
	if _, err = Parse("show tables"); err != nil {
		t.Fatal(err)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"", "SELEC * FROM t", "SELECT FROM t", "SELECT * FROM", "CREATE TABLE t (a FLOAT)",
		"INSERT INTO t VALUES 1", "SELECT * FROM t WHERE", "SELECT * FROM t WHERE a = 1 extra",
		"INSERT INTO t VALUES ('abc)", "CREATE TABLE t (a VARCHAR(0))", "SELECT * FROM t WHERE a = 99999999999999999999",
	}
	for _, s := range bad {
		if _, err := Parse(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}

func TestParseDMLAndTxn(t *testing.T) {
	st, err := Parse("UPDATE t SET a = a + 1, b = 'x' WHERE id = 3 AND a * 2 > 4 - 1")
	if err != nil {
		t.Fatal(err)
	}
	u := st.(*UpdateStmt)
	if u.Table != "t" || len(u.Sets) != 2 || u.Sets[0].Value.(*BinaryExpr).Op != "+" || u.Where == nil {
		t.Fatalf("bad update: %+v", u)
	}
	cmp := u.Where.(*BinaryExpr).Right.(*BinaryExpr)
	if cmp.Op != ">" || cmp.Left.(*BinaryExpr).Op != "*" || cmp.Right.(*BinaryExpr).Op != "-" {
		t.Fatalf("bad arithmetic precedence: %+v", cmp)
	}
	st, err = Parse("DELETE FROM t WHERE id < 5")
	if err != nil || st.(*DeleteStmt).Where == nil {
		t.Fatal("delete failed", err)
	}
	if _, err = Parse("delete from t"); err != nil {
		t.Fatal(err)
	}
	for q, want := range map[string]any{"begin": &BeginStmt{}, "START TRANSACTION": &BeginStmt{},
		"commit;": &CommitStmt{}, "ROLLBACK": &RollbackStmt{}} {
		st, err := Parse(q)
		if err != nil || fmt.Sprintf("%T", st) != fmt.Sprintf("%T", want) {
			t.Fatalf("%s: %T %v", q, st, err)
		}
	}
	st, _ = Parse("select * from t where id = 1 for update")
	if st.(*SelectStmt).Lock != LockUpdate {
		t.Fatal("for update")
	}
	st, _ = Parse("select * from t for share")
	if st.(*SelectStmt).Lock != LockShare {
		t.Fatal("for share")
	}
	if _, err := Parse("select * from t for"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := Parse("UPDATE t SET WHERE id=1"); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseJoinOrderLimitExplain(t *testing.T) {
	st, err := Parse("SELECT a.id, b.name FROM users AS a JOIN orders b ON a.id = b.uid INNER JOIN items ON items.oid = b.id WHERE a.age > 3 ORDER BY a.id DESC, b.name LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	s := st.(*SelectStmt)
	if len(s.From) != 3 || s.From[0].Binding() != "a" || s.From[1].Binding() != "b" || s.From[2].Binding() != "items" || s.From[0].Name != "users" {
		t.Fatalf("bad from: %+v", s.From)
	}
	if s.Items[0].Table != "a" || s.Items[1].Name != "name" || len(s.OrderBy) != 2 || !s.OrderBy[0].Desc || s.OrderBy[1].Desc || s.Limit != 10 {
		t.Fatalf("bad select: %+v", s)
	}
	// ON 条件被并入 WHERE: a.age > 3 AND (ON1) AND (ON2) 共 3 个合取项
	if n := len(SplitAnd(s.Where)); n != 3 {
		t.Fatalf("expected 3 conjuncts, got %d: %s", n, ExprString(s.Where))
	}
	st, err = Parse("select * from a, b, c where a.x = b.x and b.y = c.y")
	if err != nil || len(st.(*SelectStmt).From) != 3 {
		t.Fatalf("comma join: %v", err)
	}
	st, err = Parse("EXPLAIN SELECT * FROM t WHERE id = 1")
	if err != nil || st.(*ExplainStmt).Select.From[0].Name != "t" {
		t.Fatal("explain", err)
	}
	st, err = Parse("ANALYZE TABLE t;")
	if err != nil || st.(*AnalyzeStmt).Table != "t" {
		t.Fatal("analyze", err)
	}
	// FOR UPDATE 不会被当成别名
	st, err = Parse("select * from t for update")
	if err != nil || st.(*SelectStmt).Lock != LockUpdate || st.(*SelectStmt).From[0].Alias != "" {
		t.Fatal("for update", err)
	}
	for _, bad := range []string{"select * from a join", "select * from a join b on", "select * from t order id", "select * from t limit x", "select a. from t", "explain update t set a=1"} {
		if _, err := Parse(bad); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}
