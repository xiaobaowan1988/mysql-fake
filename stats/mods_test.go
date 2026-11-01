package stats

import "testing"

// 采样期间到达的修改不能丢: 否则守护线程永远不会为这些修改重新采样。
func TestSetSampledKeepsModsArrivedDuringSampling(t *testing.T) {
	s := NewStore()
	s.AddMods("T", 100)
	start := s.Mods("t") // 采样开始前读到的修改数 (表名不区分大小写)
	s.AddMods("t", 20)   // 采样期间又来了 20 个
	s.SetSampled("t", &TableStats{Rows: 500}, start)
	if got := s.Mods("t"); got != 20 {
		t.Fatalf("mods after sampling = %d, want the 20 that arrived during sampling", got)
	}
	if s.Get("t").Rows != 500 {
		t.Fatal("stats not stored")
	}
	// Set 仍然是"全部清零"的语义
	s.Set("t", &TableStats{Rows: 1})
	if got := s.Mods("t"); got != 0 {
		t.Fatalf("Set must reset mods, got %d", got)
	}
	// 计数不会变成负数
	s.SetSampled("t", &TableStats{Rows: 2}, 999)
	if got := s.Mods("t"); got != 0 {
		t.Fatalf("mods must not go negative, got %d", got)
	}
}
