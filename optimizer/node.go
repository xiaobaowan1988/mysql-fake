// Package optimizer 是基于代价的查询优化器: 统计信息 -> 选择率/基数估算 -> 访问路径选择 ->
// 动态规划枚举 Join 顺序与 Join 算法 (带有序性剪枝) -> 生成执行器树和 EXPLAIN 文本。
package optimizer

import (
	"fmt"
	"strings"

	"mysqlfake/catalog"
	"mysqlfake/executor"
	"mysqlfake/sql"
)

type colID struct{ rel, col int }

type rel struct {
	idx  int
	ref  sql.TableRef
	ti   *catalog.TableInfo
	rows float64 // 统计信息中的行数 (无统计时取默认值)
}

// Node 是物理计划树的节点，同时保存估算值 (EXPLAIN 展示) 和构建执行器所需的信息。
type Node struct {
	Op       string // SeqScan IndexScan NestedLoop HashJoin MergeJoin Sort Limit Project
	Detail   string
	Rows     float64
	Cost     float64
	Children []*Node

	cols  []colID
	order *colID // 输出按该列升序

	// 扫描
	rel     *rel
	start   []byte
	end     []byte
	filters []sql.Expr
	// 连接
	lkeys, rkeys []colID
	residual     []sql.Expr
	sortL, sortR bool // MergeJoin: 对应输入需要先排序
	// Sort / Limit / Project
	sortKeys []executor.SortKey
	limit    int
	projCols []colID
}

// Plan 是优化器的输出。
type Plan struct {
	Root    executor.Executor
	Columns []string
	Tree    *Node
	Rows    float64
	Cost    float64
}

// Explain 返回缩进的计划文本，每行一个算子。
func (p *Plan) Explain() []string {
	var lines []string
	var walk func(n *Node, depth int)
	walk = func(n *Node, depth int) {
		s := strings.Repeat("  ", depth) + n.Op
		if n.Detail != "" {
			s += " " + n.Detail
		}
		if n.Op != "Project" {
			s += fmt.Sprintf("  (rows=%.0f cost=%.2f)", n.Rows, n.Cost)
		}
		lines = append(lines, s)
		for _, c := range n.Children {
			walk(c, depth+1)
		}
	}
	walk(p.Tree, 0)
	return lines
}

// Ops 返回计划树中所有算子名 (先序)，测试用。
func (p *Plan) Ops() []string {
	var ops []string
	var walk func(n *Node)
	walk = func(n *Node) {
		ops = append(ops, n.Op)
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(p.Tree)
	return ops
}
