package executor

import (
	"errors"
	"fmt"

	"mysqlfake/btree"
	"mysqlfake/catalog"
	"mysqlfake/tuple"
)

// Insert 把预先绑定好的行写入表。第一次 Next 执行全部插入并返回 (nil, nil)。
type Insert struct {
	table    *catalog.TableInfo
	rows     [][]tuple.Value
	Affected int
	done     bool
}

func NewInsert(t *catalog.TableInfo, rows [][]tuple.Value) *Insert {
	return &Insert{table: t, rows: rows}
}

func (in *Insert) Init() error { in.done, in.Affected = false, 0; return nil }

func (in *Insert) Next() (*Row, error) {
	if in.done {
		return nil, nil
	}
	in.done = true
	for _, vals := range in.rows {
		key, err := tuple.EncodeKey(vals[in.table.PK])
		if err != nil {
			return nil, err
		}
		data, err := tuple.Marshal(in.table.Schema, vals)
		if err != nil {
			return nil, err
		}
		if err := in.table.Tree.Insert(key, data); err != nil {
			if errors.Is(err, btree.ErrDuplicateKey) {
				return nil, fmt.Errorf("duplicate entry '%s' for key 'PRIMARY'", vals[in.table.PK])
			}
			return nil, err
		}
		in.Affected++
	}
	return nil, nil
}
