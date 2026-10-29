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

func TestIndexKeyEncoding(t *testing.T) {
	// INT: 保序
	ints := []int64{math.MinInt64, -5, 0, 1, 1000, math.MaxInt64}
	var keys [][]byte
	for _, v := range ints {
		k, err := IndexKey(IntValue(v), []byte("pk"))
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, k)
		p, pk, err := SplitIndexKey(TypeInt, k)
		if err != nil || string(pk) != "pk" {
			t.Fatal(err, pk)
		}
		if d, _ := DecodeIndexPrefix(TypeInt, p); d.Int != v {
			t.Fatalf("decode %d got %d", v, d.Int)
		}
	}
	if !sort.SliceIsSorted(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 }) {
		t.Fatal("int index keys not ordered")
	}
	// VARCHAR: 含 0x00 与前缀关系的字符串也要保序，且不同值的前缀互不为前缀
	strs := []string{"", "a", "a\x00", "a\x00b", "a\x01", "ab", "b", "\x00", "\x00\x00"}
	sort.Strings(strs)
	var skeys [][]byte
	for _, s := range strs {
		k, err := IndexKey(StrValue(s), []byte{9, 9})
		if err != nil {
			t.Fatal(err)
		}
		skeys = append(skeys, k)
		p, pk, err := SplitIndexKey(TypeVarchar, k)
		if err != nil || !bytes.Equal(pk, []byte{9, 9}) {
			t.Fatalf("%q: split %v %v", s, pk, err)
		}
		if d, _ := DecodeIndexPrefix(TypeVarchar, p); d.Str != s {
			t.Fatalf("decode %q got %q", s, d.Str)
		}
	}
	if !sort.SliceIsSorted(skeys, func(i, j int) bool { return bytes.Compare(skeys[i], skeys[j]) < 0 }) {
		t.Fatal("varchar index keys not ordered")
	}
	// 主键部分不会影响值之间的先后: ("a", 其他pk) 一定在 ("ab", 任意pk) 之前
	k1, _ := IndexKey(StrValue("a"), bytes.Repeat([]byte{0xFF}, 10))
	k2, _ := IndexKey(StrValue("ab"), []byte{0})
	if bytes.Compare(k1, k2) >= 0 {
		t.Fatal("pk suffix leaked into value ordering")
	}
	if _, err := IndexKey(NullValue(TypeInt), nil); err == nil {
		t.Fatal("NULL must not be indexable")
	}
	// PrefixEnd
	p, _ := EncodeIndexPrefix(IntValue(5))
	end := PrefixEnd(p)
	inside, _ := IndexKey(IntValue(5), []byte{0xFF, 0xFF})
	next, _ := IndexKey(IntValue(6), nil)
	if bytes.Compare(inside, end) >= 0 || bytes.Compare(end, next) > 0 {
		t.Fatal("PrefixEnd bounds wrong")
	}
	if PrefixEnd([]byte{0xFF, 0xFF}) != nil {
		t.Fatal("all-FF prefix has no upper bound")
	}
}
