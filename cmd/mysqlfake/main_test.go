package main

import (
	"reflect"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	stmts, rest := splitStatements("select 'a;b'; insert into t\nvalues (1); sel")
	want := []string{"select 'a;b'", "insert into t\nvalues (1)"}
	if !reflect.DeepEqual(stmts, want) || rest != " sel" {
		t.Fatalf("got %q rest %q", stmts, rest)
	}
}
