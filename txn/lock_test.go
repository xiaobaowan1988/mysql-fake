package txn

import (
	"sync"
	"testing"
	"time"
)

func lockAsync(lm *LockManager, txn uint64, res string, mode LockMode) chan error {
	ch := make(chan error, 1)
	go func() { ch <- lm.Lock(txn, res, mode) }()
	return ch
}

func expectBlocked(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("expected lock to block, returned %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func expectGranted(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("expected grant, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lock not granted")
	}
}

func TestSharedCompatibleExclusiveBlocks(t *testing.T) {
	lm := NewLockManager()
	if lm.Lock(1, "r", LockS) != nil || lm.Lock(2, "r", LockS) != nil {
		t.Fatal("S locks must be compatible")
	}
	x := lockAsync(lm, 3, "r", LockX)
	expectBlocked(t, x)
	lm.ReleaseAll(1)
	expectBlocked(t, x)
	lm.ReleaseAll(2)
	expectGranted(t, x)
	if m, ok := lm.Holds(3, "r"); !ok || m != LockX {
		t.Fatal("txn 3 should hold X")
	}
	s := lockAsync(lm, 4, "r", LockS)
	expectBlocked(t, s)
	lm.ReleaseAll(3)
	expectGranted(t, s)
	lm.ReleaseAll(4)
	if len(lm.queues) != 0 || len(lm.held) != 0 {
		t.Fatal("lock tables should be empty")
	}
}

func TestReentrantAndUpgrade(t *testing.T) {
	lm := NewLockManager()
	lm.Lock(1, "r", LockX)
	if lm.Lock(1, "r", LockS) != nil || lm.Lock(1, "r", LockX) != nil {
		t.Fatal("re-entrant lock failed")
	}
	if m, _ := lm.Holds(1, "r"); m != LockX {
		t.Fatal("X must not be downgraded")
	}
	lm.ReleaseAll(1)

	// 唯一持有者的 S->X 升级立即成功
	lm.Lock(1, "q", LockS)
	if err := lm.Lock(1, "q", LockX); err != nil {
		t.Fatal(err)
	}
	if m, _ := lm.Holds(1, "q"); m != LockX {
		t.Fatal("not upgraded")
	}
	lm.ReleaseAll(1)

	// 有其他 S 持有者时，升级要等对方释放
	lm.Lock(1, "u", LockS)
	lm.Lock(2, "u", LockS)
	up := lockAsync(lm, 1, "u", LockX)
	expectBlocked(t, up)
	lm.ReleaseAll(2)
	expectGranted(t, up)
	if m, _ := lm.Holds(1, "u"); m != LockX {
		t.Fatal("upgrade not applied")
	}
}

func TestFIFONoStarvation(t *testing.T) {
	lm := NewLockManager()
	lm.Lock(1, "r", LockS)
	x := lockAsync(lm, 2, "r", LockX)
	expectBlocked(t, x)
	s := lockAsync(lm, 3, "r", LockS) // 虽与持有者相容，但不能越过排队的 X
	expectBlocked(t, s)
	lm.ReleaseAll(1)
	expectGranted(t, x)
	expectBlocked(t, s)
	lm.ReleaseAll(2)
	expectGranted(t, s)
}

func TestDeadlockDetection(t *testing.T) {
	lm := NewLockManager()
	lm.Lock(1, "A", LockX)
	lm.Lock(2, "B", LockX)
	w1 := lockAsync(lm, 1, "B", LockX) // 1 等 2
	expectBlocked(t, w1)
	// 2 再等 1 -> 成环，2 是牺牲者
	if err := lm.Lock(2, "A", LockX); err != ErrDeadlock {
		t.Fatalf("expected deadlock, got %v", err)
	}
	lm.ReleaseAll(2) // 牺牲者回滚释放锁，1 得以继续
	expectGranted(t, w1)
}

func TestThreeWayDeadlock(t *testing.T) {
	lm := NewLockManager()
	for i, r := range []string{"A", "B", "C"} {
		lm.Lock(uint64(i+1), r, LockX)
	}
	w1 := lockAsync(lm, 1, "B", LockX)
	expectBlocked(t, w1)
	w2 := lockAsync(lm, 2, "C", LockX)
	expectBlocked(t, w2)
	if err := lm.Lock(3, "A", LockX); err != ErrDeadlock {
		t.Fatalf("expected deadlock, got %v", err)
	}
	lm.ReleaseAll(3)
	expectGranted(t, w2)
	lm.ReleaseAll(2)
	expectGranted(t, w1)
}

func TestUpgradeDeadlock(t *testing.T) {
	lm := NewLockManager()
	lm.Lock(1, "r", LockS)
	lm.Lock(2, "r", LockS)
	up1 := lockAsync(lm, 1, "r", LockX)
	expectBlocked(t, up1)
	if err := lm.Lock(2, "r", LockX); err != ErrDeadlock {
		t.Fatalf("expected deadlock, got %v", err)
	}
	lm.ReleaseAll(2)
	expectGranted(t, up1)
}

func TestNoFalseDeadlock(t *testing.T) {
	lm := NewLockManager()
	lm.Lock(1, "A", LockX)
	w := lockAsync(lm, 2, "A", LockS) // 2 等 1，不成环
	expectBlocked(t, w)
	w3 := lockAsync(lm, 3, "A", LockS)
	expectBlocked(t, w3)
	lm.ReleaseAll(1)
	expectGranted(t, w)
	expectGranted(t, w3)
}

func TestLockTimeout(t *testing.T) {
	lm := NewLockManager()
	lm.Timeout = 50 * time.Millisecond
	lm.Lock(1, "r", LockX)
	if err := lm.Lock(2, "r", LockX); err != ErrLockTimeout {
		t.Fatalf("expected timeout, got %v", err)
	}
	// 超时的请求已被清理，持有者释放后新请求可立即获得
	lm.ReleaseAll(1)
	if err := lm.Lock(3, "r", LockX); err != nil {
		t.Fatal(err)
	}
}

func TestManyContenders(t *testing.T) {
	lm := NewLockManager()
	var wg sync.WaitGroup
	counter := 0
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if err := lm.Lock(id, "hot", LockX); err != nil {
					t.Error(err)
					return
				}
				counter++ // 受 X 锁保护
				lm.ReleaseAll(id)
			}
		}(uint64(i))
	}
	wg.Wait()
	if counter != 1000 {
		t.Fatalf("counter=%d", counter)
	}
}

func TestReadViewVisibility(t *testing.T) {
	rv := &ReadView{Creator: 10, Active: map[uint64]struct{}{7: {}, 12: {}}, MinActive: 7, UpLimit: 15}
	cases := map[uint64]bool{
		3: true, 6: true, // < MinActive
		7: false, 12: false, // 活跃
		8: true, 11: true, // 区间内且已提交
		10: true,             // 自己
		15: false, 99: false, // >= UpLimit
	}
	for id, want := range cases {
		if got := rv.Visible(id); got != want {
			t.Errorf("Visible(%d)=%v want %v", id, got, want)
		}
	}
}

func TestVersionCodec(t *testing.T) {
	v := RowVersion{TxnID: 5, RollPtr: 9, Deleted: true, Data: []byte("abc")}
	got, err := DecodeVersion(v.Encode())
	if err != nil || got.TxnID != 5 || got.RollPtr != 9 || !got.Deleted || string(got.Data) != "abc" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := DecodeVersion([]byte{1, 2}); err == nil {
		t.Fatal("expected error")
	}
}
