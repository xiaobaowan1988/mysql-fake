package server

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"mysqlfake/db"
	"mysqlfake/metrics"
)

// Mode 选择网络 I/O 模型。
type Mode string

const (
	ModeAuto      Mode = "auto"      // 支持时用事件循环，否则 goroutine
	ModeEventLoop Mode = "eventloop" // epoll (Linux) / kqueue (BSD, macOS) 事件驱动
	ModeGoroutine Mode = "goroutine" // 一连接一个 goroutine (所有平台，兜底/对比)
)

type Config struct {
	Addr   string // 例如 "127.0.0.1:3307"，端口 0 表示随机端口
	Engine *db.Engine
	Mode   Mode

	MinWorkers int           // 工作池最小协程数 (默认 4)
	MaxWorkers int           // 最大并发执行的语句数 (默认 64)
	WorkerIdle time.Duration // 空闲缩容时间 (默认 5s)
	MaxConns   int           // 最大连接数，0 表示不限
}

type ServerStats struct {
	Conns    int
	Accepted int64
	Rejected int64
	Pool     PoolStats
}

type Server struct {
	cfg  Config
	pool *WorkerPool
	mode Mode
	addr string

	loop *eventLoop
	ln   net.Listener
	wg   sync.WaitGroup

	mu    sync.Mutex
	conns map[*protoConn]struct{} // 存活连接 (关闭时统一清理会话)

	nconns, accepted, rejected atomic.Int64
	closed                     atomic.Bool
}

func New(cfg Config) *Server {
	if cfg.MinWorkers == 0 {
		cfg.MinWorkers = 4
	}
	if cfg.MaxWorkers == 0 {
		cfg.MaxWorkers = 64
	}
	if cfg.WorkerIdle == 0 {
		cfg.WorkerIdle = 5 * time.Second
	}
	if cfg.Mode == "" {
		cfg.Mode = ModeAuto
	}
	return &Server{cfg: cfg, conns: map[*protoConn]struct{}{}}
}

var ErrEventLoopUnsupported = errors.New("server: event loop (epoll/kqueue) is not supported on this platform")

// Start 绑定地址并在后台开始服务。
func (s *Server) Start() error {
	s.pool = NewWorkerPool(s.cfg.MinWorkers, s.cfg.MaxWorkers, s.cfg.WorkerIdle)
	metrics.Default.GaugeFunc("mysqlfake_connections", "Currently open client connections.", func() float64 { return float64(s.nconns.Load()) })
	metrics.Default.GaugeFunc("mysqlfake_worker_pool_workers", "Worker goroutines currently alive in the statement worker pool.", func() float64 { return float64(s.pool.Stats().Workers) })
	metrics.Default.GaugeFunc("mysqlfake_worker_pool_queued", "Requests waiting for a worker.", func() float64 { return float64(s.pool.Stats().Queued) })
	mode := s.cfg.Mode
	if mode == ModeAuto {
		mode = ModeGoroutine
		if eventLoopSupported {
			mode = ModeEventLoop
		}
	}
	s.mode = mode
	switch mode {
	case ModeEventLoop:
		if !eventLoopSupported {
			s.pool.Close()
			return ErrEventLoopUnsupported
		}
		l, err := newEventLoop(s)
		if err != nil {
			s.pool.Close()
			return err
		}
		s.loop, s.addr = l, l.addr
		s.wg.Add(1)
		go func() { defer s.wg.Done(); l.run() }()
	case ModeGoroutine:
		ln, err := net.Listen("tcp", s.cfg.Addr)
		if err != nil {
			s.pool.Close()
			return err
		}
		s.ln, s.addr = ln, ln.Addr().String()
		s.wg.Add(1)
		go func() { defer s.wg.Done(); s.serveGoroutines() }()
	default:
		s.pool.Close()
		return errors.New("server: unknown mode " + string(mode))
	}
	return nil
}

func (s *Server) Addr() string        { return s.addr }
func (s *Server) EffectiveMode() Mode { return s.mode }

func (s *Server) Stats() ServerStats {
	return ServerStats{Conns: int(s.nconns.Load()), Accepted: s.accepted.Load(), Rejected: s.rejected.Load(), Pool: s.pool.Stats()}
}

func (s *Server) track(pc *protoConn) {
	s.mu.Lock()
	s.conns[pc] = struct{}{}
	s.mu.Unlock()
	s.nconns.Add(1)
	s.accepted.Add(1)
	metrics.ConnAccepted.Inc()
}

// releaseConn 在连接结束后回收: 回滚会话中未提交的事务并更新计数。
func (s *Server) releaseConn(pc *protoConn) { s.untrack(pc) }

func (s *Server) untrack(pc *protoConn) {
	s.mu.Lock()
	_, ok := s.conns[pc]
	delete(s.conns, pc)
	s.mu.Unlock()
	if ok {
		s.nconns.Add(-1)
		pc.close()
	}
}

// Close 停止接收连接，关闭所有连接，等待执行中的语句结束，并回滚未提交的事务。
func (s *Server) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.loop != nil {
		s.loop.stop()
	}
	if s.ln != nil {
		s.ln.Close()
	}
	s.mu.Lock()
	for pc := range s.conns {
		if pc.netConn != nil {
			pc.netConn.Close()
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
	s.pool.Close()
	s.mu.Lock()
	remaining := make([]*protoConn, 0, len(s.conns))
	for pc := range s.conns {
		remaining = append(remaining, pc)
	}
	s.mu.Unlock()
	for _, pc := range remaining {
		s.untrack(pc)
	}
	return nil
}
