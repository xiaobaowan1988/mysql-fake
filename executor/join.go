package executor

import (
	"encoding/binary"
	"sort"

	"mysqlfake/tuple"
)

// RowsExec 从内存中的行产生输出 (测试与物化结果使用)。
type RowsExec struct {
	Rows [][]tuple.Value
	pos  int
}

func (e *RowsExec) Init() error { e.pos = 0; return nil }
func (e *RowsExec) Next() (*Row, error) {
	if e.pos >= len(e.Rows) {
		return nil, nil
	}
	e.pos++
	return &Row{Values: e.Rows[e.pos-1]}, nil
}

func concat(a, b []tuple.Value) []tuple.Value {
	out := make([]tuple.Value, 0, len(a)+len(b))
	out = append(out, a...)
	return append(out, b...)
}

// ---- Nested Loop Join ----

// NestedLoopJoin: 右输入在 Init 时整体物化，对每个左行扫描一遍。pred 作用于拼接后的行，可为 nil (笛卡尔积)。
type NestedLoopJoin struct {
	left, right Executor
	pred        Expr
	inner       []*Row
	lrow        *Row
	pos         int
}

func NewNestedLoopJoin(left, right Executor, pred Expr) *NestedLoopJoin {
	return &NestedLoopJoin{left: left, right: right, pred: pred}
}

func (j *NestedLoopJoin) Init() error {
	if err := j.left.Init(); err != nil {
		return err
	}
	if err := j.right.Init(); err != nil {
		return err
	}
	j.inner, j.lrow, j.pos = nil, nil, 0
	for {
		r, err := j.right.Next()
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}
		j.inner = append(j.inner, r)
	}
}

func (j *NestedLoopJoin) Next() (*Row, error) {
	for {
		if j.lrow == nil {
			l, err := j.left.Next()
			if l == nil || err != nil {
				return nil, err
			}
			j.lrow, j.pos = l, 0
		}
		for j.pos < len(j.inner) {
			vals := concat(j.lrow.Values, j.inner[j.pos].Values)
			j.pos++
			if j.pred == nil || EvalBool(j.pred, vals) {
				return &Row{Values: vals}, nil
			}
		}
		j.lrow = nil
	}
}

// ---- Hash Join ----

// hashKey 把若干列编码为哈希键；任何一列为 NULL 返回 ok=false (NULL 不参与等值连接)。
func hashKey(vals []tuple.Value, idx []int) (string, bool) {
	var b []byte
	for _, i := range idx {
		v := vals[i]
		if v.Null {
			return "", false
		}
		if v.Type == tuple.TypeInt {
			b = append(b, 'i')
			b = binary.BigEndian.AppendUint64(b, uint64(v.Int))
		} else {
			b = append(b, 's')
			b = binary.BigEndian.AppendUint32(b, uint32(len(v.Str)))
			b = append(b, v.Str...)
		}
	}
	return string(b), true
}

// HashJoin: 以右输入为构建端、左输入为探测端 (内存哈希表，不落盘)。输出列为左列 ++ 右列。
// leftKeys/rightKeys 是等值连接列在各自输入中的位置，residual 作用于拼接后的行 (可为 nil)。
type HashJoin struct {
	left, right         Executor
	leftKeys, rightKeys []int
	residual            Expr
	table               map[string][]*Row
	lrow                *Row
	matches             []*Row
	pos                 int
}

func NewHashJoin(left, right Executor, leftKeys, rightKeys []int, residual Expr) *HashJoin {
	return &HashJoin{left: left, right: right, leftKeys: leftKeys, rightKeys: rightKeys, residual: residual}
}

func (j *HashJoin) Init() error {
	if err := j.left.Init(); err != nil {
		return err
	}
	if err := j.right.Init(); err != nil {
		return err
	}
	j.table, j.lrow, j.matches, j.pos = map[string][]*Row{}, nil, nil, 0
	for {
		r, err := j.right.Next()
		if err != nil {
			return err
		}
		if r == nil {
			return nil
		}
		if k, ok := hashKey(r.Values, j.rightKeys); ok {
			j.table[k] = append(j.table[k], r)
		}
	}
}

func (j *HashJoin) Next() (*Row, error) {
	for {
		for j.pos < len(j.matches) {
			vals := concat(j.lrow.Values, j.matches[j.pos].Values)
			j.pos++
			if j.residual == nil || EvalBool(j.residual, vals) {
				return &Row{Values: vals}, nil
			}
		}
		l, err := j.left.Next()
		if l == nil || err != nil {
			return nil, err
		}
		j.lrow, j.matches, j.pos = l, nil, 0
		if k, ok := hashKey(l.Values, j.leftKeys); ok {
			j.matches = j.table[k]
		}
	}
}

// ---- Sort-Merge Join ----

// MergeJoin 要求两个输入都已按各自的连接键升序排列 (NULL 最小)，对键相同的行做笛卡尔积。
// 只对第一个等值条件做归并，其余条件放在 residual 里校验。
type MergeJoin struct {
	left, right Executor
	lk, rk      int
	residual    Expr

	lrow  *Row
	rpeek *Row
	group []*Row
	gkey  tuple.Value
	gi    int
}

func NewMergeJoin(left, right Executor, lk, rk int, residual Expr) *MergeJoin {
	return &MergeJoin{left: left, right: right, lk: lk, rk: rk, residual: residual}
}

func (j *MergeJoin) Init() error {
	if err := j.left.Init(); err != nil {
		return err
	}
	if err := j.right.Init(); err != nil {
		return err
	}
	var err error
	j.lrow, j.group, j.gi = nil, nil, 0
	j.rpeek, err = j.right.Next()
	return err
}

func (j *MergeJoin) Next() (*Row, error) {
	for {
		if j.lrow != nil && j.gi < len(j.group) {
			vals := concat(j.lrow.Values, j.group[j.gi].Values)
			j.gi++
			if j.residual == nil || EvalBool(j.residual, vals) {
				return &Row{Values: vals}, nil
			}
			continue
		}
		l, err := j.left.Next()
		if l == nil || err != nil {
			return nil, err
		}
		j.lrow, j.gi = l, 0
		lkey := l.Values[j.lk]
		if lkey.Null {
			j.group = nil
			continue
		}
		if len(j.group) > 0 && tuple.Compare(lkey, j.gkey) == 0 {
			continue // 沿用同一个右侧分组
		}
		j.group = nil
		for j.rpeek != nil && (j.rpeek.Values[j.rk].Null || tuple.Compare(j.rpeek.Values[j.rk], lkey) < 0) {
			if j.rpeek, err = j.right.Next(); err != nil {
				return nil, err
			}
		}
		if j.rpeek != nil && tuple.Compare(j.rpeek.Values[j.rk], lkey) == 0 {
			j.gkey = j.rpeek.Values[j.rk]
			for j.rpeek != nil && tuple.Compare(j.rpeek.Values[j.rk], j.gkey) == 0 {
				j.group = append(j.group, j.rpeek)
				if j.rpeek, err = j.right.Next(); err != nil {
					return nil, err
				}
			}
		}
	}
}

// ---- Sort / Limit ----

type SortKey struct {
	Idx  int
	Desc bool
}

// Sort 物化全部输入后排序 (稳定排序，NULL 视为最小)。
type Sort struct {
	child Executor
	keys  []SortKey
	rows  []*Row
	pos   int
}

func NewSort(child Executor, keys []SortKey) *Sort { return &Sort{child: child, keys: keys} }

func (s *Sort) Init() error {
	if err := s.child.Init(); err != nil {
		return err
	}
	s.rows, s.pos = nil, 0
	for {
		r, err := s.child.Next()
		if err != nil {
			return err
		}
		if r == nil {
			break
		}
		s.rows = append(s.rows, r)
	}
	sort.SliceStable(s.rows, func(i, j int) bool {
		for _, k := range s.keys {
			c := tuple.Compare(s.rows[i].Values[k.Idx], s.rows[j].Values[k.Idx])
			if c == 0 {
				continue
			}
			if k.Desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
	return nil
}

func (s *Sort) Next() (*Row, error) {
	if s.pos >= len(s.rows) {
		return nil, nil
	}
	s.pos++
	return s.rows[s.pos-1], nil
}

// Limit 最多输出 n 行。
type Limit struct {
	child Executor
	n, c  int
}

func NewLimit(child Executor, n int) *Limit { return &Limit{child: child, n: n} }

func (l *Limit) Init() error { l.c = 0; return l.child.Init() }

func (l *Limit) Next() (*Row, error) {
	if l.c >= l.n {
		return nil, nil
	}
	r, err := l.child.Next()
	if r == nil || err != nil {
		return nil, err
	}
	l.c++
	return r, nil
}

// ColumnOffsets 返回 Projection 使用的构造器 (按列下标裁剪)。
func NewProject(child Executor, cols []int) Executor { return NewProjection(child, cols) }
