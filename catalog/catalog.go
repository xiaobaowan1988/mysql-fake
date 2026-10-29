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

type TableInfo struct {
	Name   string
	Schema *tuple.Schema
	PK     int // 主键列下标
	Tree   *btree.Tree
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
		s, _, err := tuple.UnmarshalSchema(v[4:])
		if err != nil {
			return fmt.Errorf("catalog: corrupt schema for %q: %w", it.Key(), err)
		}
		name := string(it.Key())
		c.tables[name] = &TableInfo{Name: name, Schema: s, PK: s.PKIndex(), Tree: btree.Open(c.io, root)}
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
	val := binary.LittleEndian.AppendUint32(nil, uint32(tree.Root()))
	val = append(val, tuple.MarshalSchema(s)...)
	if len(val) > btree.MaxValueSize {
		return nil, errors.New("table definition too large")
	}
	if err := c.sys.Insert([]byte(key), val); err != nil {
		return nil, err
	}
	ti := &TableInfo{Name: key, Schema: s, PK: s.PKIndex(), Tree: tree}
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
