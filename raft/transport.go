package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type VoteArgs struct {
	Term      uint64
	Candidate int
	LastIndex uint64
	LastTerm  uint64
}

type VoteReply struct {
	Term    uint64
	Granted bool
}

type AppendArgs struct {
	Term      uint64
	Leader    int
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry
	Commit    uint64
}

type AppendReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
}

// Handler 由 Node 实现，传输层把收到的 RPC 交给它。
type Handler interface {
	HandleVote(*VoteArgs) *VoteReply
	HandleAppend(*AppendArgs) *AppendReply
}

// Transport 是节点间的 RPC 传输。每个节点持有自己的 Transport 实例。
type Transport interface {
	Register(h Handler)
	Vote(to int, a *VoteArgs) (*VoteReply, error)
	Append(to int, a *AppendArgs) (*AppendReply, error)
	Close()
}

var ErrUnreachable = errors.New("raft: peer unreachable")

// ---- 进程内网络 (测试与单进程集群用，可模拟宕机与网络分区) ----

type Network struct {
	mu       sync.Mutex
	handlers map[int]Handler
	down     map[int]bool
	cut      map[[2]int]bool
}

func NewNetwork() *Network {
	return &Network{handlers: map[int]Handler{}, down: map[int]bool{}, cut: map[[2]int]bool{}}
}

type memTransport struct {
	net *Network
	id  int
}

func (n *Network) Transport(id int) Transport { return &memTransport{net: n, id: id} }

// SetDown 模拟节点宕机 (收发都失败)。
func (n *Network) SetDown(id int, down bool) {
	n.mu.Lock()
	n.down[id] = down
	n.mu.Unlock()
}

// Isolate 切断 id 与其余所有节点的双向连接。
func (n *Network) Isolate(id int, all []int) {
	n.mu.Lock()
	for _, o := range all {
		if o != id {
			n.cut[[2]int{id, o}] = true
			n.cut[[2]int{o, id}] = true
		}
	}
	n.mu.Unlock()
}

// Heal 恢复全部网络连接 (不改变宕机状态)。
func (n *Network) Heal() {
	n.mu.Lock()
	n.cut = map[[2]int]bool{}
	n.mu.Unlock()
}

func (n *Network) route(from, to int) (Handler, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h := n.handlers[to]
	if h == nil || n.down[from] || n.down[to] || n.cut[[2]int{from, to}] {
		return nil, ErrUnreachable
	}
	return h, nil
}

func (t *memTransport) Register(h Handler) {
	t.net.mu.Lock()
	t.net.handlers[t.id] = h
	t.net.mu.Unlock()
}

func (t *memTransport) Vote(to int, a *VoteArgs) (*VoteReply, error) {
	h, err := t.net.route(t.id, to)
	if err != nil {
		return nil, err
	}
	cp := *a
	r := h.HandleVote(&cp)
	if _, err := t.net.route(to, t.id); err != nil { // 回包路径也要通
		return nil, err
	}
	return r, nil
}

func (t *memTransport) Append(to int, a *AppendArgs) (*AppendReply, error) {
	h, err := t.net.route(t.id, to)
	if err != nil {
		return nil, err
	}
	cp := *a
	r := h.HandleAppend(&cp)
	if _, err := t.net.route(to, t.id); err != nil {
		return nil, err
	}
	return r, nil
}

func (t *memTransport) Close() {
	t.net.mu.Lock()
	delete(t.net.handlers, t.id)
	t.net.mu.Unlock()
}

// ---- TCP 传输 (net/rpc + gob) ----

type rpcService struct{ h Handler }

func (s *rpcService) Vote(a *VoteArgs, r *VoteReply) error {
	*r = *s.h.HandleVote(a)
	return nil
}

func (s *rpcService) Append(a *AppendArgs, r *AppendReply) error {
	*r = *s.h.HandleAppend(a)
	return nil
}

type TCPTransport struct {
	self    int
	ln      net.Listener
	mu      sync.Mutex
	addrs   map[int]string
	clients map[int]*rpc.Client
	Timeout time.Duration
	closed  bool
}

// NewTCPTransport 在 listenAddr 上监听 (可用 "127.0.0.1:0")，peers 之后用 SetPeers 设置。
func NewTCPTransport(self int, listenAddr string) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	return &TCPTransport{self: self, ln: ln, addrs: map[int]string{}, clients: map[int]*rpc.Client{}, Timeout: time.Second}, nil
}

func (t *TCPTransport) Addr() string { return t.ln.Addr().String() }

func (t *TCPTransport) SetPeers(addrs map[int]string) {
	t.mu.Lock()
	t.addrs = addrs
	t.mu.Unlock()
}

func (t *TCPTransport) Register(h Handler) {
	srv := rpc.NewServer()
	srv.RegisterName("Raft", &rpcService{h})
	go func() {
		for {
			c, err := t.ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(c)
		}
	}()
}

func (t *TCPTransport) client(to int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrUnreachable
	}
	if c := t.clients[to]; c != nil {
		return c, nil
	}
	addr, ok := t.addrs[to]
	if !ok {
		return nil, ErrUnreachable
	}
	conn, err := net.DialTimeout("tcp", addr, t.Timeout)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.clients[to] = c
	return c, nil
}

func (t *TCPTransport) call(to int, method string, args, reply any) error {
	c, err := t.client(to)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			t.drop(to, c)
		}
		return call.Error
	case <-time.After(t.Timeout):
		t.drop(to, c)
		return errors.New("raft: rpc timeout")
	}
}

func (t *TCPTransport) drop(to int, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *TCPTransport) Vote(to int, a *VoteArgs) (*VoteReply, error) {
	var r VoteReply
	if err := t.call(to, "Raft.Vote", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) Append(to int, a *AppendArgs) (*AppendReply, error) {
	var r AppendReply
	if err := t.call(to, "Raft.Append", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (t *TCPTransport) Close() {
	t.mu.Lock()
	t.closed = true
	for _, c := range t.clients {
		c.Close()
	}
	t.mu.Unlock()
	t.ln.Close()
}
