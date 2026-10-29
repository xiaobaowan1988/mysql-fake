package vector

import (
	"encoding/binary"

	"mysqlfake/tuple"
)

var identity [BatchSize]int32

func init() {
	for i := range identity {
		identity[i] = int32(i)
	}
}

// active 返回有效行下标，Sel 为 nil 时返回只读的恒等序列 (不分配)。
func active(b *Batch) []int32 {
	if b.Sel != nil {
		return b.Sel
	}
	return identity[:b.N]
}

// Filter 用核函数过滤: 只更新选择向量，不拷贝数据。
type Filter struct {
	child BatchExecutor
	k     Kernel
}

func NewFilter(child BatchExecutor, k Kernel) *Filter { return &Filter{child, k} }

func (f *Filter) Init() error { return f.child.Init() }

func (f *Filter) NextBatch() (*Batch, error) {
	for {
		b, err := f.child.NextBatch()
		if b == nil || err != nil {
			return nil, err
		}
		t, _ := f.k.Eval(b, active(b))
		if len(t) == 0 {
			continue
		}
		return &Batch{Cols: b.Cols, N: b.N, Sel: t}, nil
	}
}

// Project 按列下标裁剪/重排 (只拷贝列指针)。
type Project struct {
	child BatchExecutor
	cols  []int
}

func NewProject(child BatchExecutor, cols []int) *Project { return &Project{child, cols} }

func (p *Project) Init() error { return p.child.Init() }

func (p *Project) NextBatch() (*Batch, error) {
	b, err := p.child.NextBatch()
	if b == nil || err != nil {
		return nil, err
	}
	cols := make([]*tuple.ColBuf, len(p.cols))
	for i, c := range p.cols {
		cols[i] = b.Cols[c]
	}
	return &Batch{Cols: cols, N: b.N, Sel: b.Sel}, nil
}

// Limit 最多输出 n 行。
type Limit struct {
	child BatchExecutor
	n, c  int
}

func NewLimit(child BatchExecutor, n int) *Limit { return &Limit{child: child, n: n} }

func (l *Limit) Init() error { l.c = 0; return l.child.Init() }

func (l *Limit) NextBatch() (*Batch, error) {
	if l.c >= l.n {
		return nil, nil
	}
	b, err := l.child.NextBatch()
	if b == nil || err != nil {
		return nil, err
	}
	sel := active(b)
	if remain := l.n - l.c; len(sel) > remain {
		sel = sel[:remain]
	}
	l.c += len(sel)
	return &Batch{Cols: b.Cols, N: b.N, Sel: sel}, nil
}

// HashJoin: 右输入整体读入列式哈希表 (内存内)，左输入按批探测，输出左列 ++ 右列。
// 单个 INT 键走 map[int64] 快速路径，其余用编码后的字符串键。residual 由上层的 Filter 处理。
type HashJoin struct {
	left, right    BatchExecutor
	lkeys, rkeys   []int
	ltypes, rtypes []tuple.Type
	build          []*tuple.ColBuf
	intIdx         map[int64][]int32
	strIdx         map[string][]int32
	fast           bool
	cur            *Batch
	sel            []int32
	pos            int
	matches        []int32
	mpos           int
	probeRow       int32
	done           bool
}

func NewHashJoin(left, right BatchExecutor, lkeys, rkeys []int, ltypes, rtypes []tuple.Type) *HashJoin {
	return &HashJoin{left: left, right: right, lkeys: lkeys, rkeys: rkeys, ltypes: ltypes, rtypes: rtypes}
}

// rowKey 把一行的连接键编码成字符串；任何键为 NULL 返回 ok=false。
func rowKey(cols []*tuple.ColBuf, keys []int, i int32) (string, bool) {
	var b []byte
	for _, k := range keys {
		c := cols[k]
		if c.Nulls[i] {
			return "", false
		}
		if c.Type == tuple.TypeInt {
			b = append(b, 'i')
			b = binary.BigEndian.AppendUint64(b, uint64(c.Ints[i]))
		} else {
			b = append(b, 's')
			b = binary.BigEndian.AppendUint32(b, uint32(len(c.Strs[i])))
			b = append(b, c.Strs[i]...)
		}
	}
	return string(b), true
}

func (j *HashJoin) Init() error {
	if err := j.left.Init(); err != nil {
		return err
	}
	if err := j.right.Init(); err != nil {
		return err
	}
	j.build = newCols(j.rtypes, BatchSize)
	j.fast = len(j.rkeys) == 1 && j.rtypes[j.rkeys[0]] == tuple.TypeInt
	j.intIdx, j.strIdx = nil, nil
	if j.fast {
		j.intIdx = map[int64][]int32{}
	} else {
		j.strIdx = map[string][]int32{}
	}
	j.cur, j.sel, j.pos, j.matches, j.mpos, j.done = nil, nil, 0, nil, 0, false
	n := int32(0)
	for {
		b, err := j.right.NextBatch()
		if err != nil {
			return err
		}
		if b == nil {
			return nil
		}
		for _, i := range active(b) {
			for c, col := range j.build {
				col.AppendFrom(b.Cols[c], int(i))
			}
			if j.fast {
				if kc := b.Cols[j.rkeys[0]]; !kc.Nulls[i] {
					j.intIdx[kc.Ints[i]] = append(j.intIdx[kc.Ints[i]], n)
				}
			} else if k, ok := rowKey(b.Cols, j.rkeys, i); ok {
				j.strIdx[k] = append(j.strIdx[k], n)
			}
			n++
		}
	}
}

func (j *HashJoin) lookup(b *Batch, i int32) []int32 {
	if j.fast {
		if kc := b.Cols[j.lkeys[0]]; !kc.Nulls[i] {
			return j.intIdx[kc.Ints[i]]
		}
		return nil
	}
	if k, ok := rowKey(b.Cols, j.lkeys, i); ok {
		return j.strIdx[k]
	}
	return nil
}

func (j *HashJoin) NextBatch() (*Batch, error) {
	if j.done {
		return nil, nil
	}
	var lidx, ridx []int32
	flush := func() *Batch {
		out := newCols(append(append([]tuple.Type(nil), j.ltypes...), j.rtypes...), len(lidx))
		for c := range j.ltypes {
			for _, li := range lidx {
				out[c].AppendFrom(j.cur.Cols[c], int(li))
			}
		}
		for c := range j.rtypes {
			dst := out[len(j.ltypes)+c]
			for _, ri := range ridx {
				dst.AppendFrom(j.build[c], int(ri))
			}
		}
		return &Batch{Cols: out, N: len(lidx)}
	}
	for {
		for j.mpos < len(j.matches) {
			lidx = append(lidx, j.probeRow)
			ridx = append(ridx, j.matches[j.mpos])
			j.mpos++
			if len(lidx) >= BatchSize {
				return flush(), nil
			}
		}
		if j.cur != nil && j.pos < len(j.sel) {
			j.probeRow = j.sel[j.pos]
			j.pos++
			j.matches, j.mpos = j.lookup(j.cur, j.probeRow), 0
			continue
		}
		if len(lidx) > 0 { // 换批之前先输出已攒的结果 (它们引用当前探测批)
			return flush(), nil
		}
		b, err := j.left.NextBatch()
		if err != nil {
			return nil, err
		}
		if b == nil {
			j.done = true
			return nil, nil
		}
		j.cur, j.sel, j.pos, j.matches, j.mpos = b, active(b), 0, nil, 0
	}
}
