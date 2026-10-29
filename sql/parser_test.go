package sql

import (
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
	if s.Star || len(s.Items) != 2 || s.Table != "t" {
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
		"INSERT INTO t VALUES 1", "SELECT * FROM t WHERE", "SELECT * FROM t extra",
		"INSERT INTO t VALUES ('abc)", "CREATE TABLE t (a VARCHAR(0))", "SELECT * FROM t WHERE a = 99999999999999999999",
	}
	for _, s := range bad {
		if _, err := Parse(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
}
