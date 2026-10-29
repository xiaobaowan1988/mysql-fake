package btree

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"mysqlfake/storage"
)

var (
	ErrDuplicateKey  = errors.New("btree: duplicate key")
	ErrKeyNotFound   = errors.New("btree: key not found")
	ErrKeyTooLarge   = errors.New("btree: key too large")
	ErrValueTooLarge = errors.New("btree: value too large")
	ErrEmptyKey      = errors.New("btree: empty key")
)

// Tree 是一棵 B+ 树。根页号在整个生命周期内保持不变，因此可以放心记录在 Catalog 中。
//
// 并发控制: 页级读写锁存器 (Latch) + 螃蟹步 (Crab Walking)。
//   - 读 (Get/Scan): 自顶向下，先锁住子节点再释放父节点 (手递手)，范围扫描沿叶子链同样手递手。
//   - 写 (Insert/Update/Put): 先走乐观路径: 沿途只加读锁，只对叶子加写锁；叶子放得下就直接改。
//     放不下 (需要分裂) 才退回悲观路径: 自顶向下对路径加写锁，一旦遇到"安全"节点 (吸收一次子节点分裂也不会满)
//     就释放它的所有祖先，因此根节点只在整棵树真的要变高时才被独占。
//   - Delete 不做节点合并，永远走乐观路径。
type Tree struct {
	io   storage.PageIO
	root storage.PageID

	latchMu sync.Mutex
	latches map[storage.PageID]*sync.RWMutex

	optimistic, pessimistic atomic.Int64
}

// Stats 返回写操作走乐观路径和退回悲观路径的次数。
type Stats struct{ Optimistic, Pessimistic int64 }

func (t *Tree) Stats() Stats {
	return Stats{t.optimistic.Load(), t.pessimistic.Load()}
}

func (t *Tree) latch(id storage.PageID) *sync.RWMutex {
	t.latchMu.Lock()
	defer t.latchMu.Unlock()
	l := t.latches[id]
	if l == nil {
		l = new(sync.RWMutex)
		t.latches[id] = l
	}
	return l
}

func makeTree(io storage.PageIO, root storage.PageID) *Tree {
	return &Tree{io: io, root: root, latches: map[storage.PageID]*sync.RWMutex{}}
}

// Create 新建一棵空树 (分配一个空叶子作为根)。
func Create(io storage.PageIO) (*Tree, error) {
	id, err := io.AllocatePage()
	if err != nil {
		return nil, err
	}
	t := makeTree(io, id)
	var p storage.Page
	p.ID = id
	if err := (&node{leaf: true}).encode(&p); err != nil {
		return nil, err
	}
	if err := io.WritePage(&p); err != nil {
		return nil, err
	}
	return t, nil
}

func Open(io storage.PageIO, root storage.PageID) *Tree { return makeTree(io, root) }

func (t *Tree) Root() storage.PageID { return t.root }

func (t *Tree) load(id storage.PageID) (*node, error) {
	var p storage.Page
	if err := t.io.ReadPage(id, &p); err != nil {
		return nil, err
	}
	n, err := decode(&p)
	if err != nil {
		return nil, fmt.Errorf("page %d: %w", id, err)
	}
	return n, nil
}

func (t *Tree) store(id storage.PageID, n *node) error {
	var p storage.Page
	if err := t.io.ReadPage(id, &p); err != nil {
		return err
	}
	if err := n.encode(&p); err != nil {
		return err
	}
	return t.io.WritePage(&p)
}

// storeNew 写入一个新分配的页。
func (t *Tree) storeNew(n *node) (storage.PageID, error) {
	id, err := t.io.AllocatePage()
	if err != nil {
		return 0, err
	}
	var p storage.Page
	p.ID = id
	if err := n.encode(&p); err != nil {
		return 0, err
	}
	return id, t.io.WritePage(&p)
}

func checkKV(key, val []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(val) > MaxValueSize {
		return ErrValueTooLarge
	}
	return nil
}

// Get 点查。返回的 value 是副本。
func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	pid := t.root
	l := t.latch(pid)
	l.RLock()
	for {
		n, err := t.load(pid)
		if err != nil {
			l.RUnlock()
			return nil, false, err
		}
		if n.leaf {
			i, found := n.search(key)
			l.RUnlock()
			if !found {
				return nil, false, nil
			}
			return n.vals[i], true, nil
		}
		child := n.children[n.childIndex(key)]
		cl := t.latch(child)
		cl.RLock()
		l.RUnlock()
		pid, l = child, cl
	}
}

type putMode int

const (
	modeInsert putMode = iota // key 已存在则报错
	modeUpdate                // key 不存在则报错
	modePut                   // upsert
)

// Insert 插入新键，已存在返回 ErrDuplicateKey。
func (t *Tree) Insert(key, val []byte) error { return t.put(key, val, modeInsert) }

// Update 更新已存在的键，不存在返回 ErrKeyNotFound。
func (t *Tree) Update(key, val []byte) error { return t.put(key, val, modeUpdate) }

// Put 插入或覆盖。
func (t *Tree) Put(key, val []byte) error { return t.put(key, val, modePut) }

// applyToLeaf 在叶子节点内存副本上执行写入；返回写入是否会让节点大小超过页容量。
func applyToLeaf(n *node, key, val []byte, mode putMode) (overflow bool, err error) {
	i, found := n.search(key)
	switch {
	case found && mode == modeInsert:
		return false, ErrDuplicateKey
	case !found && mode == modeUpdate:
		return false, ErrKeyNotFound
	case found:
		n.vals[i] = bytes.Clone(val)
	default:
		n.keys = insertBytes(n.keys, i, bytes.Clone(key))
		n.vals = insertBytes(n.vals, i, bytes.Clone(val))
	}
	return n.size() > storage.PayloadSize, nil
}

func (t *Tree) put(key, val []byte, mode putMode) error {
	if err := checkKV(key, val); err != nil {
		return err
	}
	done, err := t.putOptimistic(key, val, mode)
	if done || err != nil {
		if done {
			t.optimistic.Add(1)
		}
		return err
	}
	t.pessimistic.Add(1)
	return t.putPessimistic(key, val, mode)
}

// descendLeafX 沿途只加读锁，返回被写锁住的叶子 (及释放函数)。
func (t *Tree) descendLeafX(key []byte) (pid storage.PageID, n *node, release func(), err error) {
	for {
		rl := t.latch(t.root)
		rl.RLock()
		rn, err := t.load(t.root)
		if err != nil {
			rl.RUnlock()
			return 0, nil, nil, err
		}
		if rn.leaf { // 树只有一个叶子 (根): 升级为写锁并复查，期间根可能已经分裂
			rl.RUnlock()
			rl.Lock()
			if rn, err = t.load(t.root); err != nil {
				rl.Unlock()
				return 0, nil, nil, err
			}
			if rn.leaf {
				return t.root, rn, rl.Unlock, nil
			}
			rl.Unlock()
			continue
		}
		parentL, parent := rl, rn
		for {
			child := parent.children[parent.childIndex(key)]
			cl := t.latch(child)
			cl.RLock()
			cn, err := t.load(child)
			if err != nil {
				cl.RUnlock()
				parentL.RUnlock()
				return 0, nil, nil, err
			}
			if cn.leaf {
				// 父节点的读锁一直持有，叶子不可能在此期间分裂或被替换。
				cl.RUnlock()
				cl.Lock()
				cn, err = t.load(child)
				parentL.RUnlock()
				if err != nil {
					cl.Unlock()
					return 0, nil, nil, err
				}
				return child, cn, cl.Unlock, nil
			}
			parentL.RUnlock()
			parentL, parent = cl, cn
		}
	}
}

// putOptimistic 只锁叶子；叶子放得下返回 done=true，需要分裂则返回 done=false 让调用方走悲观路径。
func (t *Tree) putOptimistic(key, val []byte, mode putMode) (bool, error) {
	pid, n, release, err := t.descendLeafX(key)
	if err != nil {
		return false, err
	}
	defer release()
	overflow, err := applyToLeaf(n, key, val, mode)
	if err != nil {
		return true, err
	}
	if overflow {
		return false, nil
	}
	return true, t.store(pid, n)
}

type step struct {
	pid storage.PageID
	n   *node
	ci  int // 该节点在父节点 children 中的下标
	l   *sync.RWMutex
}

// safeFor 判断节点吸收一次下层变化后是否一定不会超过页容量。
func safeFor(n *node, key, val []byte) bool {
	if n.leaf {
		return n.size()+4+len(key)+len(val) <= storage.PayloadSize
	}
	return n.size()+2+MaxKeySize+4 <= storage.PayloadSize
}

func (t *Tree) putPessimistic(key, val []byte, mode putMode) error {
	rl := t.latch(t.root)
	rl.Lock()
	rn, err := t.load(t.root)
	if err != nil {
		rl.Unlock()
		return err
	}
	path := []step{{pid: t.root, n: rn, ci: -1, l: rl}}
	unlockAll := func() {
		for _, s := range path {
			s.l.Unlock()
		}
	}
	for !path[len(path)-1].n.leaf {
		cur := path[len(path)-1]
		ci := cur.n.childIndex(key)
		child := cur.n.children[ci]
		cl := t.latch(child)
		cl.Lock()
		cn, err := t.load(child)
		if err != nil {
			cl.Unlock()
			unlockAll()
			return err
		}
		if safeFor(cn, key, val) { // 子节点不会分裂: 祖先不再需要锁
			unlockAll()
			path = path[:0]
		}
		path = append(path, step{pid: child, n: cn, ci: ci, l: cl})
	}
	defer func() { unlockAll() }()

	level := len(path) - 1
	overflow, err := applyToLeaf(path[level].n, key, val, mode)
	if err != nil {
		return err
	}
	if !overflow {
		return t.store(path[level].pid, path[level].n)
	}
	sep, right, err := t.split(path[level].pid, path[level].n)
	if err != nil {
		return err
	}
	for level > 0 {
		level--
		par := &path[level]
		ci := path[level+1].ci
		par.n.keys = insertBytes(par.n.keys, ci, sep)
		par.n.children = append(par.n.children, 0)
		copy(par.n.children[ci+2:], par.n.children[ci+1:])
		par.n.children[ci+1] = right
		if par.n.size() <= storage.PayloadSize {
			return t.store(par.pid, par.n)
		}
		if sep, right, err = t.split(par.pid, par.n); err != nil {
			return err
		}
	}
	top := path[0]
	if top.pid != t.root {
		return errors.New("btree: internal error: split propagated above a safe node")
	}
	// 根分裂: 根页号不能变，把左半部分搬到新页，根改为内部节点。
	lid, err := t.storeNew(top.n)
	if err != nil {
		return err
	}
	return t.store(t.root, &node{keys: [][]byte{sep}, children: []storage.PageID{lid, right}})
}

func (t *Tree) split(pid storage.PageID, n *node) ([]byte, storage.PageID, error) {
	var sep []byte
	var rn *node
	if n.leaf {
		// 按字节数找到使左半约占一半的位置。
		total, acc, mid := n.size(), leafMeta, 0
		for mid = 0; mid < len(n.keys)-1; mid++ {
			acc += 4 + len(n.keys[mid]) + len(n.vals[mid])
			if acc >= total/2 {
				mid++
				break
			}
		}
		rn = &node{leaf: true, keys: n.keys[mid:], vals: n.vals[mid:], next: n.next}
		n.keys, n.vals = n.keys[:mid:mid], n.vals[:mid:mid]
		sep = bytes.Clone(rn.keys[0])
	} else {
		mid := len(n.keys) / 2
		sep = n.keys[mid]
		rn = &node{keys: n.keys[mid+1:], children: n.children[mid+1:]}
		n.keys, n.children = n.keys[:mid:mid], n.children[:mid+1:mid+1]
	}
	rid, err := t.storeNew(rn)
	if err != nil {
		return nil, 0, err
	}
	if n.leaf {
		n.next = rid
	}
	return sep, rid, t.store(pid, n)
}

// Delete 物理删除一个键 (不做节点合并)。返回键是否存在。
func (t *Tree) Delete(key []byte) (bool, error) {
	pid, n, release, err := t.descendLeafX(key)
	if err != nil {
		return false, err
	}
	defer release()
	i, found := n.search(key)
	if !found {
		return false, nil
	}
	n.keys = append(n.keys[:i], n.keys[i+1:]...)
	n.vals = append(n.vals[:i], n.vals[i+1:]...)
	return true, t.store(pid, n)
}

func insertBytes(s [][]byte, i int, v []byte) [][]byte {
	s = append(s, nil)
	copy(s[i+1:], s[i:])
	s[i] = v
	return s
}

// Iterator 按 key 升序遍历 [start, end)。start 为 nil 表示从头开始，end 为 nil 表示到末尾。
// 迭代器不在两次 Next 之间持有锁，每次取一批叶子数据，因此可与写入并发。
type Iterator struct {
	t       *Tree
	start   []byte
	end     []byte
	last    []byte
	started bool
	buf     []entry
	pos     int
	done    bool
	err     error
	cur     entry
}

type entry struct{ key, val []byte }

func (t *Tree) Scan(start, end []byte) *Iterator {
	return &Iterator{t: t, start: start, end: end}
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	for it.pos >= len(it.buf) {
		if it.done {
			return false
		}
		if it.err = it.fill(); it.err != nil {
			return false
		}
	}
	it.cur = it.buf[it.pos]
	it.pos++
	it.last, it.started = it.cur.key, true
	return true
}

func (it *Iterator) Key() []byte   { return it.cur.key }
func (it *Iterator) Value() []byte { return it.cur.val }
func (it *Iterator) Err() error    { return it.err }

// fill 重新定位到 "最后返回的键之后" 并读取一批条目。沿树手递手加读锁，沿叶子链同样手递手。
func (it *Iterator) fill() error {
	t := it.t
	target := it.start
	if it.started {
		target = it.last
	}
	pid := t.root
	l := t.latch(pid)
	l.RLock()
	var n *node
	for {
		var err error
		if n, err = t.load(pid); err != nil {
			l.RUnlock()
			return err
		}
		if n.leaf {
			break
		}
		var child storage.PageID
		if target == nil {
			child = n.children[0]
		} else {
			child = n.children[n.childIndex(target)]
		}
		cl := t.latch(child)
		cl.RLock()
		l.RUnlock()
		pid, l = child, cl
	}
	defer func() { l.RUnlock() }()
	it.buf, it.pos = it.buf[:0], 0
	for {
		i := 0
		if target != nil {
			var found bool
			i, found = n.search(target)
			if found && it.started {
				i++ // 已返回过的键不再重复
			}
		}
		for ; i < len(n.keys); i++ {
			if it.end != nil && bytes.Compare(n.keys[i], it.end) >= 0 {
				it.done = true
				return nil
			}
			it.buf = append(it.buf, entry{n.keys[i], n.vals[i]})
		}
		if n.next == storage.InvalidPageID {
			it.done = true
			return nil
		}
		if len(it.buf) > 0 {
			return nil
		}
		next := n.next
		nl := t.latch(next)
		nl.RLock()
		l.RUnlock()
		pid, l = next, nl
		var err error
		if n, err = t.load(pid); err != nil {
			return err
		}
		target = nil // 后续叶子的所有键都在 target 之后
	}
}
