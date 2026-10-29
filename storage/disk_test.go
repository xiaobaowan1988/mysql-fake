package storage

import (
	"path/filepath"
	"testing"
)

func TestDiskManagerBasic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := d.AllocatePage()
	b, _ := d.AllocatePage()
	if a != 1 || b != 2 {
		t.Fatalf("unexpected ids %d %d", a, b)
	}
	p := &Page{}
	p.Reset(a)
	p.SetType(PageTypeLeaf)
	copy(p.Payload(), "hello")
	if err := d.WritePage(p); err != nil {
		t.Fatal(err)
	}
	// 释放后复用，且内容被清零
	if err := d.FreePage(a); err != nil {
		t.Fatal(err)
	}
	if err := d.FreePage(b); err != nil {
		t.Fatal(err)
	}
	c, _ := d.AllocatePage()
	if c != b {
		t.Fatalf("expected reuse of %d, got %d", b, c)
	}
	if err := d.SetCatalogRoot(7); err != nil {
		t.Fatal(err)
	}
	d.Close()

	d, err = OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if d.CatalogRoot() != 7 || d.FreeHead() != a || d.NumPages() != 3 {
		t.Fatalf("header not persisted: root=%d free=%d n=%d", d.CatalogRoot(), d.FreeHead(), d.NumPages())
	}
	e, _ := d.AllocatePage()
	if e != a {
		t.Fatalf("expected reuse of %d, got %d", a, e)
	}
	var q Page
	if err := d.ReadPage(e, &q); err != nil {
		t.Fatal(err)
	}
	if q.Type() != PageTypeInvalid || q.Payload()[0] != 0 {
		t.Fatal("reallocated page should be zeroed")
	}
	if err := d.ReadPage(0, &q); err == nil {
		t.Fatal("reading page 0 should fail")
	}
	if err := d.ReadPage(99, &q); err == nil {
		t.Fatal("reading out of range should fail")
	}
}

func TestBadMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.db")
	d, _ := OpenDiskManager(path)
	d.f.WriteAt([]byte("XXXXXXXX"), 0)
	d.Close()
	if _, err := OpenDiskManager(path); err != ErrBadMagic {
		t.Fatalf("expected ErrBadMagic, got %v", err)
	}
}
