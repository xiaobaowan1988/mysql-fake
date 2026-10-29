package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"mysqlfake/storage"
)

func newTree(t *testing.T) (*Tree, *storage.DiskManager, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bt.db")
	d, err := storage.OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := Create(d)
	if err != nil {
		t.Fatal(err)
	}
	return tr, d, path
}

func key(i int) []byte { return []byte(fmt.Sprintf("k%08d", i)) }

func TestInsertGetSplit(t *testing.T) {
	tr, d, _ := newTree(t)
	defer d.Close()
	val := bytes.Repeat([]byte("v"), 900) // 大 value，快速产生多层分裂
	perm := rand.New(rand.NewSource(1)).Perm(3000)
	for _, i := range perm {
		if err := tr.Insert(key(i), val); err != nil {
			t.Fatal(err)
		}
	}
	// 确认树至少有 3 层
	n, _ := tr.load(tr.root)
	depth := 1
	for !n.leaf {
		n, _ = tr.load(n.children[0])
		depth++
	}
	if depth < 3 {
		t.Fatalf("expected depth >= 3, got %d", depth)
	}
	for i := 0; i < 3000; i++ {
		v, ok, err := tr.Get(key(i))
		if err != nil || !ok || !bytes.Equal(v, val) {
			t.Fatalf("get %d: ok=%v err=%v", i, ok, err)
		}
	}
	if _, ok, _ := tr.Get(key(5000)); ok {
		t.Fatal("unexpected key")
	}
	if err := tr.Insert(key(7), val); err != ErrDuplicateKey {
		t.Fatalf("expected duplicate error, got %v", err)
	}
}

func TestScanRange(t *testing.T) {
	tr, d, _ := newTree(t)
	defer d.Close()
	for _, i := range rand.New(rand.NewSource(2)).Perm(2000) {
		tr.Insert(key(i), []byte(fmt.Sprint(i)))
	}
	it := tr.Scan(nil, nil)
	cnt := 0
	for it.Next() {
		if !bytes.Equal(it.Key(), key(cnt)) {
			t.Fatalf("pos %d got %s", cnt, it.Key())
		}
		cnt++
	}
	if it.Err() != nil || cnt != 2000 {
		t.Fatalf("full scan: cnt=%d err=%v", cnt, it.Err())
	}
	it = tr.Scan(key(500), key(1500))
	cnt = 0
	for it.Next() {
		if !bytes.Equal(it.Key(), key(500+cnt)) {
			t.Fatalf("range pos %d got %s", cnt, it.Key())
		}
		cnt++
	}
	if cnt != 1000 {
		t.Fatalf("range scan got %d", cnt)
	}
	// 起点不存在的范围
	it = tr.Scan([]byte("k00000100x"), []byte("k00000103"))
	var got []string
	for it.Next() {
		got = append(got, string(it.Key()))
	}
	if fmt.Sprint(got) != "[k00000101 k00000102]" {
		t.Fatalf("unexpected %v", got)
	}
}

func TestDeleteUpdateAndEmptyLeaves(t *testing.T) {
	tr, d, _ := newTree(t)
	defer d.Close()
	val := bytes.Repeat([]byte("x"), 500)
	for i := 0; i < 1000; i++ {
		tr.Insert(key(i), val)
	}
	// 删光中间一大段，叶子会变空
	for i := 200; i < 800; i++ {
		ok, err := tr.Delete(key(i))
		if !ok || err != nil {
			t.Fatalf("delete %d: %v %v", i, ok, err)
		}
	}
	if ok, _ := tr.Delete(key(300)); ok {
		t.Fatal("double delete should report false")
	}
	it := tr.Scan(nil, nil)
	cnt := 0
	for it.Next() {
		cnt++
	}
	if cnt != 400 {
		t.Fatalf("expected 400 left, got %d", cnt)
	}
	if err := tr.Update(key(300), val); err != ErrKeyNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
	// 更新为更大的 value 触发分裂
	big := bytes.Repeat([]byte("y"), 1000)
	for i := 0; i < 200; i++ {
		if err := tr.Update(key(i), big); err != nil {
			t.Fatal(err)
		}
	}
	v, _, _ := tr.Get(key(100))
	if !bytes.Equal(v, big) {
		t.Fatal("update lost")
	}
	// 在被删空的区间重新插入
	for i := 200; i < 800; i++ {
		if err := tr.Put(key(i), val); err != nil {
			t.Fatal(err)
		}
	}
	it = tr.Scan(nil, nil)
	cnt = 0
	for it.Next() {
		cnt++
	}
	if cnt != 1000 {
		t.Fatalf("expected 1000, got %d", cnt)
	}
}

func TestRandomAgainstMap(t *testing.T) {
	tr, d, _ := newTree(t)
	defer d.Close()
	rng := rand.New(rand.NewSource(3))
	ref := map[string][]byte{}
	for step := 0; step < 20000; step++ {
		k := key(rng.Intn(1500))
		switch rng.Intn(3) {
		case 0, 1:
			v := bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, rng.Intn(300)+1)
			if err := tr.Put(k, v); err != nil {
				t.Fatal(err)
			}
			ref[string(k)] = v
		case 2:
			ok, err := tr.Delete(k)
			if err != nil {
				t.Fatal(err)
			}
			_, exist := ref[string(k)]
			if ok != exist {
				t.Fatalf("delete mismatch for %s", k)
			}
			delete(ref, string(k))
		}
	}
	var keys []string
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	it := tr.Scan(nil, nil)
	i := 0
	for it.Next() {
		if i >= len(keys) || string(it.Key()) != keys[i] || !bytes.Equal(it.Value(), ref[keys[i]]) {
			t.Fatalf("scan mismatch at %d", i)
		}
		i++
	}
	if i != len(keys) {
		t.Fatalf("scan returned %d, want %d", i, len(keys))
	}
}

func TestPersistence(t *testing.T) {
	tr, d, path := newTree(t)
	for i := 0; i < 500; i++ {
		tr.Insert(key(i), []byte(fmt.Sprint(i)))
	}
	root := tr.Root()
	d.Close()
	d, err := storage.OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tr = Open(d, root)
	v, ok, _ := tr.Get(key(321))
	if !ok || string(v) != "321" {
		t.Fatal("data lost after reopen")
	}
}

func TestLimits(t *testing.T) {
	tr, d, _ := newTree(t)
	defer d.Close()
	if err := tr.Insert(nil, []byte("v")); err != ErrEmptyKey {
		t.Fatal("empty key")
	}
	if err := tr.Insert(make([]byte, MaxKeySize+1), nil); err != ErrKeyTooLarge {
		t.Fatal("big key")
	}
	if err := tr.Insert([]byte("a"), make([]byte, MaxValueSize+1)); err != ErrValueTooLarge {
		t.Fatal("big value")
	}
	// 最大 key + 最大 value 的条目也能正常分裂
	for i := 0; i < 50; i++ {
		k := append(bytes.Repeat([]byte{'k'}, MaxKeySize-4), []byte(fmt.Sprintf("%04d", i))...)
		if err := tr.Insert(k, make([]byte, MaxValueSize)); err != nil {
			t.Fatal(err)
		}
	}
}
