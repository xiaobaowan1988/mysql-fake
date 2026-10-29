package btree

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

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
// 并发控制: 树级读写锁 (写互斥，读共享)。
type Tree struct {
	io   storage.PageIO
	root storage.PageID
	mu   sync.RWMutex
}

// Create 新建一棵空树 (分配一个空叶子作为根)。
func Create(io storage.PageIO) (*Tree, error) {
	id, err := io.AllocatePage()
	if err != nil {
		return nil, err
	}
	t := &Tree{io: io, root: id}
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

func Open(io storage.PageIO, root storage.PageID) *Tree {
	return &Tree{io: io, root: root}
}

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
	t.mu.RLock()
	defer t.mu.RUnlock()
	pid := t.root
	for {
		n, err := t.load(pid)
		if err != nil {
			return nil, false, err
		}
		if n.leaf {
			i, found := n.search(key)
			if !found {
				return nil, false, nil
			}
			return n.vals[i], true, nil
		}
		pid = n.children[n.childIndex(key)]
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

func (t *Tree) put(key, val []byte, mode putMode) error {
	if err := checkKV(key, val); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	sep, right, err := t.insertAt(t.root, key, val, mode)
	if err != nil || right == storage.InvalidPageID {
		return err
	}
	// 根分裂: 根页号不能变，把左半部分搬到新页，根改为内部节点。
	left, err := t.load(t.root)
	if err != nil {
		return err
	}
	lid, err := t.storeNew(left)
	if err != nil {
		return err
	}
	return t.store(t.root, &node{keys: [][]byte{sep}, children: []storage.PageID{lid, right}})
}

// insertAt 在 pid 子树中插入；若 pid 发生分裂，左半留在 pid，返回分隔键和右半页号。
func (t *Tree) insertAt(pid storage.PageID, key, val []byte, mode putMode) ([]byte, storage.PageID, error) {
	n, err := t.load(pid)
	if err != nil {
		return nil, 0, err
	}
	if n.leaf {
		i, found := n.search(key)
		switch {
		case found && mode == modeInsert:
			return nil, 0, ErrDuplicateKey
		case !found && mode == modeUpdate:
			return nil, 0, ErrKeyNotFound
		case found:
			n.vals[i] = bytes.Clone(val)
		default:
			n.keys = insertBytes(n.keys, i, bytes.Clone(key))
			n.vals = insertBytes(n.vals, i, bytes.Clone(val))
		}
	} else {
		ci := n.childIndex(key)
		sep, right, err := t.insertAt(n.children[ci], key, val, mode)
		if err != nil || right == storage.InvalidPageID {
			return nil, 0, err
		}
		n.keys = insertBytes(n.keys, ci, sep)
		n.children = append(n.children, 0)
		copy(n.children[ci+2:], n.children[ci+1:])
		n.children[ci+1] = right
	}
	if n.size() <= storage.PayloadSize {
		return nil, 0, t.store(pid, n)
	}
	return t.split(pid, n)
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
	t.mu.Lock()
	defer t.mu.Unlock()
	pid := t.root
	for {
		n, err := t.load(pid)
		if err != nil {
			return false, err
		}
		if !n.leaf {
			pid = n.children[n.childIndex(key)]
			continue
		}
		i, found := n.search(key)
		if !found {
			return false, nil
		}
		n.keys = append(n.keys[:i], n.keys[i+1:]...)
		n.vals = append(n.vals[:i], n.vals[i+1:]...)
		return true, t.store(pid, n)
	}
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

// fill 重新定位到 "最后返回的键之后" 并读取一批条目。
func (it *Iterator) fill() error {
	t := it.t
	t.mu.RLock()
	defer t.mu.RUnlock()
	target := it.start
	if it.started {
		target = it.last
	}
	pid := t.root
	var n *node
	for {
		var err error
		if n, err = t.load(pid); err != nil {
			return err
		}
		if n.leaf {
			break
		}
		if target == nil {
			pid = n.children[0]
		} else {
			pid = n.children[n.childIndex(target)]
		}
	}
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
		var err error
		if n, err = t.load(n.next); err != nil {
			return err
		}
		target = nil // 后续叶子的所有键都在 target 之后
	}
}
