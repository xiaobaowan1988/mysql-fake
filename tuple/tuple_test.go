package tuple

import (
	"bytes"
	"math"
	"sort"
	"testing"
)

var testSchema = &Schema{Columns: []Column{
	{Name: "id", Type: TypeInt, PrimaryKey: true},
	{Name: "name", Type: TypeVarchar, Len: 20},
	{Name: "age", Type: TypeInt},
}}

func TestRoundTrip(t *testing.T) {
	rows := [][]Value{
		{IntValue(1), StrValue("alice"), IntValue(30)},
		{IntValue(-5), NullValue(TypeVarchar), NullValue(TypeInt)},
		{IntValue(0), StrValue(""), IntValue(math.MinInt64)},
	}
	for _, r := range rows {
		b, err := Marshal(testSchema, r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Unmarshal(testSchema, b)
		if err != nil {
			t.Fatal(err)
		}
		for i := range r {
			if Compare(r[i], got[i]) != 0 || r[i].Null != got[i].Null {
				t.Fatalf("mismatch col %d: %v vs %v", i, r[i], got[i])
			}
		}
	}
}

func TestMarshalErrors(t *testing.T) {
	if _, err := Marshal(testSchema, []Value{IntValue(1)}); err == nil {
		t.Fatal("expected arity error")
	}
	if _, err := Marshal(testSchema, []Value{StrValue("x"), StrValue("a"), IntValue(1)}); err == nil {
		t.Fatal("expected type error")
	}
	if _, err := Marshal(testSchema, []Value{IntValue(1), StrValue("this string is way too long"), IntValue(1)}); err == nil {
		t.Fatal("expected length error")
	}
	if _, err := Unmarshal(testSchema, []byte{0, 1}); err == nil {
		t.Fatal("expected corrupt error")
	}
}

func TestKeyOrder(t *testing.T) {
	vals := []int64{math.MinInt64, -100, -1, 0, 1, 99, math.MaxInt64}
	var keys [][]byte
	for _, v := range vals {
		k, _ := EncodeKey(IntValue(v))
		keys = append(keys, k)
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatal("int keys not order preserving")
	}
	for i, v := range vals {
		d, err := DecodeKey(TypeInt, keys[i])
		if err != nil || d.Int != v {
			t.Fatalf("decode %d: %v %v", v, d, err)
		}
	}
	if _, err := EncodeKey(NullValue(TypeInt)); err == nil {
		t.Fatal("null key must fail")
	}
}

func TestSchemaRoundTrip(t *testing.T) {
	b := MarshalSchema(testSchema)
	s, n, err := UnmarshalSchema(b)
	if err != nil || n != len(b) || len(s.Columns) != 3 || s.Columns[1] != testSchema.Columns[1] || !s.Columns[0].PrimaryKey {
		t.Fatalf("schema round trip failed: %+v %v", s, err)
	}
}
