package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

func writeTestPage(t *testing.T, d *DiskManager, id PageID, s string) *Page {
	t.Helper()
	p := &Page{}
	p.Reset(id)
	p.SetType(PageTypeLeaf)
	p.SetLSN(uint64(len(s)))
	copy(p.Payload(), s)
	if err := d.WritePage(p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestChecksumDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.db")
	d, _ := OpenDiskManager(path)
	id, _ := d.AllocatePage()
	writeTestPage(t, d, id, "important data")
	var p Page
	if err := d.ReadPage(id, &p); err != nil {
		t.Fatal(err)
	}
	// 翻转一个字节 (模拟静默损坏)
	if _, err := d.f.WriteAt([]byte{0xFF}, int64(id)*PageSize+100); err != nil {
		t.Fatal(err)
	}
	if err := d.ReadPage(id, &p); !errors.Is(err, ErrChecksum) {
		t.Fatalf("expected ErrChecksum, got %v", err)
	}
	// 没写过的 (全零) 页是合法的
	id2, _ := d.AllocatePage()
	if err := d.ReadPage(id2, &p); err != nil {
		t.Fatalf("zero page must be valid: %v", err)
	}
	d.Close()
}

func TestDoublewriteRepairsTornPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dw.db")
	d, _ := OpenDiskManager(path)
	id, _ := d.AllocatePage()
	writeTestPage(t, d, id, "version one")
	want := writeTestPage(t, d, id, "version two, the latest")
	// 模拟撕裂写: 数据页只有前 2KB 是新内容，后半是垃圾
	garbage := bytes.Repeat([]byte{0xAB}, PageSize/2)
	if _, err := d.f.WriteAt(garbage, int64(id)*PageSize+PageSize/2); err != nil {
		t.Fatal(err)
	}
	var p Page
	if err := d.ReadPage(id, &p); !errors.Is(err, ErrChecksum) {
		t.Fatalf("torn page must be detected, got %v", err)
	}
	d.f.Close() // 不走 Close，模拟崩溃
	d.dw.Close()

	d, err := OpenDiskManager(path) // 打开时从 .dw 修复
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.ReadPage(id, &p); err != nil {
		t.Fatalf("page should have been repaired: %v", err)
	}
	if !bytes.HasPrefix(p.Payload(), []byte("version two, the latest")) || p.LSN() != want.LSN() {
		t.Fatalf("repaired content wrong: %q", p.Payload()[:30])
	}
}

func TestDoublewriteIgnoresGarbageSlotAndKeepsGoodPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dw2.db")
	d, _ := OpenDiskManager(path)
	id, _ := d.AllocatePage()
	writeTestPage(t, d, id, "stable")
	// 破坏 .dw 槽位 (模拟写 .dw 时崩溃)；数据页完好，不应被改动
	d.dw.WriteAt(bytes.Repeat([]byte{7}, 500), 100)
	d.f.Close()
	d.dw.Close()
	d, err := OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var p Page
	if err := d.ReadPage(id, &p); err != nil || !bytes.HasPrefix(p.Payload(), []byte("stable")) {
		t.Fatalf("good page damaged: %v", err)
	}
}

func TestDoublewriteRingWrapAndBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ring.db")
	d, _ := OpenDiskManager(path)
	defer d.Close()
	var pages []*Page
	for i := 0; i < 200; i++ { // 超过槽位数，触发分批和回绕
		id, _ := d.AllocatePage()
		p := &Page{}
		p.Reset(id)
		p.SetType(PageTypeLeaf)
		copy(p.Payload(), fmt.Sprintf("page-%d", i))
		pages = append(pages, p)
	}
	if err := d.WritePages(pages); err != nil {
		t.Fatal(err)
	}
	for i, p := range pages {
		var q Page
		if err := d.ReadPage(p.ID, &q); err != nil || !bytes.HasPrefix(q.Payload(), []byte(fmt.Sprintf("page-%d", i))) {
			t.Fatalf("page %d: %v", i, err)
		}
	}
	// 调用方的页不被修改 (不会被写入校验和)
	if binary.LittleEndian.Uint32(pages[0].Data[16:20]) != 0 {
		t.Fatal("WritePages must not modify caller pages")
	}
}
