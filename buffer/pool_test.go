package buffer

import (
	"bytes"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"mysqlfake/btree"
	"mysqlfake/storage"
	"mysqlfake/wal"
)

type env struct {
	d    *storage.DiskManager
	lm   *wal.LogManager
	pool *Pool
	path string
}

func newEnv(t *testing.T, capacity int) *env {
	t.Helper()
	path := filepath.Join(t.TempDir(), "p.db")
	return openEnv(t, path, capacity)
}

func openEnv(t *testing.T, path string, capacity int) *env {
	t.Helper()
	d, err := storage.OpenDiskManager(path)
	if err != nil {
		t.Fatal(err)
	}
	lm, err := wal.Open(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Recover(d, lm); err != nil {
		t.Fatal(err)
	}
	return &env{d, lm, New(d, lm, capacity), path}
}

func (e *env) crash() { e.lm.CloseNoFlush(); e.d.Close() }

func page(id storage.PageID, s string) *storage.Page {
	p := &storage.Page{}
	p.Reset(id)
	p.SetType(storage.PageTypeLeaf)
	copy(p.Payload(), s)
	return p
}

func TestDiff(t *testing.T) {
	a := make([]byte, 100)
	b := make([]byte, 100)
	if len(diff(a, b)) != 0 {
		t.Fatal("equal pages -> no spans")
	}
	b[3], b[10], b[90] = 1, 1, 1
	got := diff(a, b)
	if fmt.Sprint(got) != "[[3 11] [90 91]]" {
		t.Fatalf("got %v", got)
	}
}

func TestReadWriteAndLRUEviction(t *testing.T) {
	e := newEnv(t, 3)
	defer e.d.Close()
	var ids []storage.PageID
	for i := 0; i < 10; i++ {
		id, err := e.pool.AllocatePage()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		if err := e.pool.WritePage(page(id, fmt.Sprintf("page-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// 只有 3 个帧，其余脏页必须已被淘汰并写回磁盘
	for i, id := range ids {
		var p storage.Page
		if err := e.pool.ReadPage(id, &p); err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(p.Payload(), []byte(fmt.Sprintf("page-%d", i))) {
			t.Fatalf("page %d corrupted: %q", id, p.Payload()[:10])
		}
	}
	// WAL 规则: 被淘汰落盘的页，其 LSN 以内的日志必然已刷盘
	var raw storage.Page
	e.d.ReadPage(ids[0], &raw)
	if raw.LSN() == 0 || e.lm.FlushedLSN() < raw.LSN() {
		t.Fatalf("WAL violated: page lsn %d flushed %d", raw.LSN(), e.lm.FlushedLSN())
	}
}

func TestLRUOrderAndPinning(t *testing.T) {
	e := newEnv(t, 2)
	defer e.d.Close()
	a, _ := e.pool.AllocatePage()
	b, _ := e.pool.AllocatePage()
	e.pool.WritePage(page(a, "a"))
	e.pool.WritePage(page(b, "b"))
	fa, err := e.pool.FetchPage(a) // pin a
	if err != nil {
		t.Fatal(err)
	}
	fb, _ := e.pool.FetchPage(b)
	c, _ := e.d.AllocatePage()
	if _, err := e.pool.FetchPage(c); err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
	e.pool.Unpin(fb) // b 可被淘汰，a 仍被 pin
	fc, err := e.pool.FetchPage(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, cached := e.pool.table[b]; cached {
		t.Fatal("b should have been evicted")
	}
	if _, cached := e.pool.table[a]; !cached {
		t.Fatal("pinned a must stay")
	}
	e.pool.Unpin(fa)
	e.pool.Unpin(fc)
}

func TestFreeAndReuse(t *testing.T) {
	e := newEnv(t, 4)
	defer e.d.Close()
	a, _ := e.pool.AllocatePage()
	e.pool.WritePage(page(a, "old life"))
	if err := e.pool.FreePage(a); err != nil {
		t.Fatal(err)
	}
	b, _ := e.pool.AllocatePage()
	if a != b {
		t.Fatalf("expected reuse of %d, got %d", a, b)
	}
	var p storage.Page
	e.pool.ReadPage(b, &p)
	if p.Type() != storage.PageTypeInvalid || p.Payload()[0] != 0 {
		t.Fatal("reallocated page must be zero")
	}
	e.pool.WritePage(page(b, "new life"))
	e.pool.ReadPage(b, &p)
	if !bytes.HasPrefix(p.Payload(), []byte("new life")) {
		t.Fatal("write after reuse lost")
	}
}

func TestLatchConcurrency(t *testing.T) {
	e := newEnv(t, 4)
	defer e.d.Close()
	id, _ := e.pool.AllocatePage()
	e.pool.WritePage(page(id, ""))
	var wg sync.WaitGroup
	counter := 0
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				f, err := e.pool.FetchPage(id)
				if err != nil {
					t.Error(err)
					return
				}
				f.WLatch() // 排他: 读-改-写计数器
				counter++
				f.Page.Payload()[0]++
				f.WUnlatch()
				e.pool.Unpin(f)
			}
		}()
	}
	wg.Wait()
	f, _ := e.pool.FetchPage(id)
	defer e.pool.Unpin(f)
	if counter != 1600 || f.Page.Payload()[0] != byte(1600%256) {
		t.Fatalf("counter=%d byte=%d", counter, f.Page.Payload()[0])
	}
}

func TestBTreeOverTinyPoolConcurrent(t *testing.T) {
	e := newEnv(t, 4)
	defer e.d.Close()
	tr, err := btree.Create(e.pool)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 400; i++ {
				k := []byte(fmt.Sprintf("g%d-%05d", g, i))
				if err := tr.Insert(k, bytes.Repeat([]byte("v"), 100)); err != nil {
					t.Error(err)
					return
				}
				if _, ok, err := tr.Get(k); !ok || err != nil {
					t.Errorf("missing %s: %v", k, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	n := 0
	it := tr.Scan(nil, nil)
	for it.Next() {
		n++
	}
	if n != 1600 || it.Err() != nil {
		t.Fatalf("n=%d err=%v", n, it.Err())
	}
}

func TestCrashRecoveryRedo(t *testing.T) {
	e := newEnv(t, 64)
	tr, _ := btree.Create(e.pool)
	root := tr.Root()
	for i := 0; i < 300; i++ {
		tr.Insert([]byte(fmt.Sprintf("key%04d", i)), bytes.Repeat([]byte("x"), 200))
	}
	if err := e.pool.Commit(1); err != nil { // 日志落盘，但数据页全在内存里
		t.Fatal(err)
	}
	if e.pool.DirtyCount() == 0 {
		t.Fatal("expected dirty pages")
	}
	e.crash() // 宕机: 脏页丢失

	e = openEnv(t, e.path, 64) // 打开时 Recover 回放日志
	defer e.d.Close()
	tr = btree.Open(e.pool, root)
	for i := 0; i < 300; i++ {
		if _, ok, err := tr.Get([]byte(fmt.Sprintf("key%04d", i))); !ok || err != nil {
			t.Fatalf("key%04d lost after recovery: %v", i, err)
		}
	}
}

func TestUncommittedTailLostButFlushedPagesConsistent(t *testing.T) {
	// 小缓冲池会导致淘汰: 页刷盘前日志必须先刷 (WAL)，恢复后已落盘数据不重复/不丢失。
	e := newEnv(t, 4)
	tr, _ := btree.Create(e.pool)
	root := tr.Root()
	for i := 0; i < 200; i++ {
		tr.Insert([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("y"), 300))
	}
	e.pool.Commit(1)
	for i := 200; i < 260; i++ { // 这些没有提交，尾部日志可能丢失
		tr.Insert([]byte(fmt.Sprintf("k%04d", i)), []byte("z"))
	}
	e.crash()
	e = openEnv(t, e.path, 4)
	defer e.d.Close()
	tr = btree.Open(e.pool, root)
	for i := 0; i < 200; i++ {
		if _, ok, _ := tr.Get([]byte(fmt.Sprintf("k%04d", i))); !ok {
			t.Fatalf("committed key %d lost", i)
		}
	}
}

func TestCheckpointTruncatesLog(t *testing.T) {
	e := newEnv(t, 16)
	defer e.d.Close()
	tr, _ := btree.Create(e.pool)
	tr.Insert([]byte("a"), []byte("1"))
	if err := e.pool.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if e.pool.DirtyCount() != 0 {
		t.Fatal("checkpoint must flush all dirty pages")
	}
	e.lm.Close()
	lm, _ := wal.Open(e.path + ".wal")
	defer lm.Close()
	if len(lm.Records()) != 0 {
		t.Fatalf("log should be empty, has %d", len(lm.Records()))
	}
}
