// Package catalog 管理表的元数据。元数据持久化在一棵系统 B+ 树里:
// key = 小写表名, value = 表数据 B+ 树根页号(4) | Schema。
// 系统树的根页号记录在数据库文件头的 catalogRoot 字段。
package catalog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"mysqlfake/btree"
	"mysqlfake/storage"
	"mysqlfake/tuple"
)

var (
	ErrTableExists   = errors.New("table already exists")
	ErrTableNotFound = errors.New("table not found")
)

// Meta 是 Catalog 需要的文件头元数据访问接口 (由 DiskManager 实现)。
type Meta interface {
	CatalogRoot() storage.PageID
	SetCatalogRoot(storage.PageID) error
}

// IndexInfo 是一个二级索引 (非唯一，单列)。
type IndexInfo struct {
	Name      string
	Column    int // 被索引的列下标
	Tree      *btree.Tree
	ValidFrom uint64 // 创建 (切换) 时的事务号: 只有在其之后建立的 Read View 才能用它做快照读
}

// TableInfo 在发布之后不可变；DDL 通过 Catalog 换成新的 TableInfo 对象。
type TableInfo struct {
	Name    string
	Schema  *tuple.Schema
	PK      int // 主键列下标
	Tree    *btree.Tree
	Indexes []*IndexInfo
}

// IndexOn 返回建立在 col 列上的索引 (没有返回 nil)。
func (t *TableInfo) IndexOn(col int) *IndexInfo {
	for _, ix := range t.Indexes {
		if ix.Column == col {
			return ix
		}
	}
	return nil
}

type Catalog struct {
	mu     sync.Mutex
	io     storage.PageIO
	sys    *btree.Tree
	tables map[string]*TableInfo
}

// Open 打开 Catalog；首次使用时创建系统树。
func Open(io storage.PageIO, meta Meta) (*Catalog, error) {
	c := &Catalog{io: io, tables: map[string]*TableInfo{}}
	if root := meta.CatalogRoot(); root != storage.InvalidPageID {
		c.sys = btree.Open(io, root)
		return c, c.load()
	}
	t, err := btree.Create(io)
	if err != nil {
		return nil, err
	}
	if err := meta.SetCatalogRoot(t.Root()); err != nil {
		return nil, err
	}
	c.sys = t
	return c, nil
}

func (c *Catalog) load() error {
	it := c.sys.Scan(nil, nil)
	for it.Next() {
		v := it.Value()
		if len(v) < 4 {
			return fmt.Errorf("catalog: corrupt entry %q", it.Key())
		}
		root := storage.PageID(binary.LittleEndian.Uint32(v))
		s, n, err := tuple.UnmarshalSchema(v[4:])
		if err != nil {
			return fmt.Errorf("catalog: corrupt schema for %q: %w", it.Key(), err)
		}
		name := string(it.Key())
		ti := &TableInfo{Name: name, Schema: s, PK: s.PKIndex(), Tree: btree.Open(c.io, root)}
		if ti.Indexes, err = c.decodeIndexes(v[4+n:]); err != nil {
			return fmt.Errorf("catalog: corrupt index list for %q: %w", it.Key(), err)
		}
		c.tables[name] = ti
	}
	return it.Err()
}

// CreateTable 校验并创建表。要求恰有一个单列主键，类型为 INT 或 VARCHAR。
func (c *Catalog) CreateTable(name string, cols []tuple.Column) (*TableInfo, error) {
	key := strings.ToLower(name)
	if key == "" {
		return nil, errors.New("empty table name")
	}
	if len(cols) == 0 {
		return nil, errors.New("table must have at least one column")
	}
	pks := 0
	seen := map[string]bool{}
	for _, col := range cols {
		l := strings.ToLower(col.Name)
		if seen[l] {
			return nil, fmt.Errorf("duplicate column name %q", col.Name)
		}
		seen[l] = true
		if col.PrimaryKey {
			pks++
		}
	}
	if pks != 1 {
		return nil, errors.New("table must have exactly one PRIMARY KEY column")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[key]; ok {
		return nil, fmt.Errorf("%w: %s", ErrTableExists, name)
	}
	tree, err := btree.Create(c.io)
	if err != nil {
		return nil, err
	}
	s := &tuple.Schema{Columns: cols}
	ti := &TableInfo{Name: key, Schema: s, PK: s.PKIndex(), Tree: tree}
	val, err := encodeTable(ti)
	if err != nil {
		return nil, err
	}
	if err := c.sys.Insert([]byte(key), val); err != nil {
		return nil, err
	}
	c.tables[key] = ti
	return ti, nil
}

func (c *Catalog) GetTable(name string) (*TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ti, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return ti, nil
}

func (c *Catalog) ListTables() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.tables))
	for n := range c.tables {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// DropTable 从 Catalog 中移除表 (仅用于 CREATE TABLE 复制失败后的回退；表页不回收)。
func (c *Catalog) DropTable(name string) error {
	key := strings.ToLower(name)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tables[key]; !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	if _, err := c.sys.Delete([]byte(key)); err != nil {
		return err
	}
	delete(c.tables, key)
	return nil
}

// IsTableExists 判断错误是否为"表已存在"。
func IsTableExists(err error) bool { return errors.Is(err, ErrTableExists) }

// 目录项: 表树根页(4) | Schema | 索引个数(2) { 名字长度(2) 名字 列下标(2) 根页(4) ValidFrom(8) }*
func encodeTable(ti *TableInfo) ([]byte, error) {
	val := binary.LittleEndian.AppendUint32(nil, uint32(ti.Tree.Root()))
	val = append(val, tuple.MarshalSchema(ti.Schema)...)
	val = binary.LittleEndian.AppendUint16(val, uint16(len(ti.Indexes)))
	for _, ix := range ti.Indexes {
		val = binary.LittleEndian.AppendUint16(val, uint16(len(ix.Name)))
		val = append(val, ix.Name...)
		val = binary.LittleEndian.AppendUint16(val, uint16(ix.Column))
		val = binary.LittleEndian.AppendUint32(val, uint32(ix.Tree.Root()))
		val = binary.LittleEndian.AppendUint64(val, ix.ValidFrom)
	}
	if len(val) > btree.MaxValueSize {
		return nil, errors.New("table definition too large")
	}
	return val, nil
}

func (c *Catalog) decodeIndexes(b []byte) ([]*IndexInfo, error) {
	if len(b) == 0 {
		return nil, nil // 没有索引段 (旧格式)
	}
	if len(b) < 2 {
		return nil, tuple.ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint16(b))
	pos := 2
	var out []*IndexInfo
	for i := 0; i < n; i++ {
		if pos+2 > len(b) {
			return nil, tuple.ErrCorrupt
		}
		l := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if pos+l+14 > len(b) {
			return nil, tuple.ErrCorrupt
		}
		ix := &IndexInfo{Name: string(b[pos : pos+l])}
		pos += l
		ix.Column = int(binary.LittleEndian.Uint16(b[pos:]))
		root := storage.PageID(binary.LittleEndian.Uint32(b[pos+2:]))
		ix.ValidFrom = binary.LittleEndian.Uint64(b[pos+6:])
		ix.Tree = btree.Open(c.io, root)
		pos += 14
		out = append(out, ix)
	}
	return out, nil
}

// publish 持久化并替换内存中的 TableInfo (调用方持有 c.mu)。
func (c *Catalog) publish(ti *TableInfo) error {
	val, err := encodeTable(ti)
	if err != nil {
		return err
	}
	if err := c.sys.Put([]byte(ti.Name), val); err != nil {
		return err
	}
	c.tables[ti.Name] = ti
	return nil
}

var (
	ErrIndexExists   = errors.New("index already exists")
	ErrIndexNotFound = errors.New("index not found")
)

// AddIndex 注册一个已经建好的索引树 (Online DDL 切换时调用)。
func (c *Catalog) AddIndex(table string, ix *IndexInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.tables[strings.ToLower(table)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, table)
	}
	for _, e := range old.Indexes {
		if strings.EqualFold(e.Name, ix.Name) {
			return fmt.Errorf("%w: %s", ErrIndexExists, ix.Name)
		}
	}
	cp := *old
	cp.Indexes = append(append([]*IndexInfo(nil), old.Indexes...), ix)
	return c.publish(&cp)
}

// RemoveIndex 注销索引 (仅用于 DDL 复制失败后的撤销)。
func (c *Catalog) RemoveIndex(table, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.tables[strings.ToLower(table)]
	if !ok {
		return fmt.Errorf("%w: %s", ErrTableNotFound, table)
	}
	cp := *old
	cp.Indexes = nil
	found := false
	for _, e := range old.Indexes {
		if strings.EqualFold(e.Name, name) {
			found = true
			continue
		}
		cp.Indexes = append(cp.Indexes, e)
	}
	if !found {
		return fmt.Errorf("%w: %s", ErrIndexNotFound, name)
	}
	return c.publish(&cp)
}

// ReplaceTable 原子地换上新的表结构和数据树 (ADD COLUMN 切换)，保留索引。返回被替换的旧 TableInfo。
func (c *Catalog) ReplaceTable(table string, schema *tuple.Schema, tree *btree.Tree) (*TableInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.tables[strings.ToLower(table)]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTableNotFound, table)
	}
	cp := *old
	cp.Schema, cp.PK, cp.Tree = schema, schema.PKIndex(), tree
	if err := c.publish(&cp); err != nil {
		return nil, err
	}
	return old, nil
}

// RestoreTable 把表恢复成给定的 TableInfo (撤销 ReplaceTable)。
func (c *Catalog) RestoreTable(ti *TableInfo) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publish(ti)
}
