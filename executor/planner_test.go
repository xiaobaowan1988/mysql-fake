package executor

import (
	"testing"

	"mysqlfake/catalog"
	"mysqlfake/sql"
	"mysqlfake/tuple"
)

func plan(t *testing.T, q string) Executor {
	t.Helper()
	ti := &catalog.TableInfo{Name: "t", PK: 0, Schema: &tuple.Schema{Columns: []tuple.Column{
		{Name: "id", Type: tuple.TypeInt, PrimaryKey: true}, {Name: "n", Type: tuple.TypeInt}}}}
	st, err := sql.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	ex, _, err := PlanSelect(st.(*sql.SelectStmt), ti)
	if err != nil {
		t.Fatal(err)
	}
	return ex
}

func scanOf(ex Executor) Executor {
	e := ex.(*Projection).child
	if f, ok := e.(*Filter); ok {
		e = f.child
	}
	return e
}

func TestPlannerChoosesScan(t *testing.T) {
	if _, ok := scanOf(plan(t, "SELECT * FROM t")).(*SeqScan); !ok {
		t.Error("no where -> SeqScan")
	}
	if _, ok := scanOf(plan(t, "SELECT * FROM t WHERE n = 1")).(*SeqScan); !ok {
		t.Error("non-pk predicate -> SeqScan")
	}
	if _, ok := scanOf(plan(t, "SELECT * FROM t WHERE id = 1 OR n = 2")).(*SeqScan); !ok {
		t.Error("OR must not use index")
	}
	is, ok := scanOf(plan(t, "SELECT * FROM t WHERE n = 1 AND id >= 5 AND 10 > id")).(*IndexScan)
	if !ok || is.Start == nil || is.End == nil {
		t.Fatalf("expected bounded IndexScan, got %T", scanOf(plan(t, "SELECT * FROM t WHERE id >= 5")))
	}
	if _, ok := scanOf(plan(t, "SELECT * FROM t WHERE id = 3")).(*IndexScan); !ok {
		t.Error("pk equality -> IndexScan")
	}
}
