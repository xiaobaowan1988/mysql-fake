package server

import (
	"crypto/rand"
	"encoding/binary"
	"net"
	"strings"
	"sync/atomic"

	"mysqlfake/db"
	"mysqlfake/tuple"
)

type stage int

const (
	stageHandshake stage = iota // 已发送握手包，等待客户端应答
	stageReady
)

// protoConn 是与 I/O 无关的协议状态机: 事件循环和 goroutine 模式共用。
// 同一时刻只会有一个 goroutine 调用 handle (每连接请求串行)。
type protoConn struct {
	id    uint32
	eng   *db.Engine
	sess  *db.Session
	stage stage
	salt  [20]byte

	netConn net.Conn // goroutine 模式下的底层连接 (事件循环模式为 nil)
}

var nextConnID atomic.Uint32

func newProtoConn(eng *db.Engine) *protoConn {
	c := &protoConn{id: nextConnID.Add(1), eng: eng, sess: eng.NewSession()}
	rand.Read(c.salt[:])
	for i := range c.salt { // 盐值里不能有 0 字节
		if c.salt[i] == 0 {
			c.salt[i] = 'x'
		}
	}
	return c
}

func (c *protoConn) greeting() []byte {
	var seq byte
	return framePacket(nil, &seq, greetingPayload(c.id, c.salt))
}

func (c *protoConn) status() uint16 {
	var st uint16
	if c.sess.Autocommit() {
		st |= statusAutocommit
	}
	if c.sess.InTxn() {
		st |= statusInTrans
	}
	return st
}

// handle 处理一个请求包，返回要发送的字节；closeAfter 表示发送后应关闭连接。
func (c *protoConn) handle(p packet) (resp []byte, closeAfter bool) {
	var seq byte
	if c.stage == stageHandshake {
		seq = p.seq + 1
		if _, err := parseHandshakeResponse(p.payload); err != nil {
			return framePacket(nil, &seq, errPayload(errGeneric, "08S01", err.Error())), true
		}
		c.stage = stageReady
		return framePacket(nil, &seq, okPayload(0, c.status())), false
	}
	seq = p.seq + 1
	if len(p.payload) == 0 {
		return framePacket(nil, &seq, errPayload(errUnknownCmd, "08S01", "empty command")), true
	}
	cmd, body := p.payload[0], p.payload[1:]
	switch cmd {
	case comQuit:
		return nil, true
	case comPing, comInitDB:
		return framePacket(nil, &seq, okPayload(0, c.status())), false
	case comFieldLs:
		return framePacket(nil, &seq, eofPayload(c.status())), false
	case comQuery:
		return c.query(string(body), &seq), false
	}
	return framePacket(nil, &seq, errPayload(errUnknownCmd, "08S01", "unknown command")), false
}

func (c *protoConn) query(q string, seq *byte) []byte {
	res, err := c.exec(q)
	if err != nil {
		code, state := mysqlErrFor(err)
		return framePacket(nil, seq, errPayload(code, state, err.Error()))
	}
	if !res.IsQuery {
		return framePacket(nil, seq, okPayload(uint64(res.Affected), c.status()))
	}
	return encodeResultSet(res, seq, c.status())
}

// exec 执行 SQL；客户端连接时常发的 SET/USE/版本查询由兼容层直接应答。
func (c *protoConn) exec(q string) (*db.Result, error) {
	if on, ok := parseAutocommit(q); ok {
		return &db.Result{}, c.sess.SetAutocommit(on)
	}
	if res, ok := compat(q); ok {
		return res, nil
	}
	return c.sess.Exec(q)
}

// parseAutocommit 识别 SET [SESSION] [@@]autocommit = 0|1|ON|OFF。
func parseAutocommit(q string) (on, ok bool) {
	s := strings.ToLower(strings.TrimSpace(strings.TrimRight(strings.TrimSpace(q), ";")))
	s = strings.TrimPrefix(s, "set ")
	if s == strings.ToLower(strings.TrimSpace(strings.TrimRight(strings.TrimSpace(q), ";"))) {
		return false, false
	}
	for _, p := range []string{"session ", "@@session.", "@@"} {
		s = strings.TrimPrefix(s, p)
	}
	if !strings.HasPrefix(s, "autocommit") {
		return false, false
	}
	v := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(strings.TrimPrefix(s, "autocommit")), "="))
	switch v {
	case "1", "on", "true":
		return true, true
	case "0", "off", "false":
		return false, true
	}
	return false, false
}

func (c *protoConn) close() { c.sess.Close() }

// encodeResultSet 按文本协议编码结果集 (不使用 DEPRECATE_EOF)。
func encodeResultSet(res *db.Result, seq *byte, status uint16) []byte {
	out := framePacket(nil, seq, appendLenenc(nil, uint64(len(res.Columns))))
	for i, name := range res.Columns {
		isInt := columnIsInt(res, i)
		def := appendLenencStr(nil, "def")
		def = appendLenencStr(def, "")   // schema
		def = appendLenencStr(def, "")   // table
		def = appendLenencStr(def, "")   // org_table
		def = appendLenencStr(def, name) // name
		def = appendLenencStr(def, name) // org_name
		def = append(def, 0x0c)
		def = binary.LittleEndian.AppendUint16(def, charsetUTF8)
		if isInt {
			def = binary.LittleEndian.AppendUint32(def, 20)
			def = append(def, typeLongLong)
		} else {
			def = binary.LittleEndian.AppendUint32(def, 1024)
			def = append(def, typeVarString)
		}
		def = binary.LittleEndian.AppendUint16(def, 0) // flags
		def = append(def, 0)                           // decimals
		def = append(def, 0, 0)
		out = framePacket(out, seq, def)
	}
	out = framePacket(out, seq, eofPayload(status))
	for _, row := range res.Rows {
		var rp []byte
		for _, v := range row {
			if v.Null {
				rp = append(rp, 0xfb)
			} else {
				rp = appendLenencStr(rp, v.String())
			}
		}
		out = framePacket(out, seq, rp)
	}
	return framePacket(out, seq, eofPayload(status))
}

func columnIsInt(res *db.Result, col int) bool {
	for _, row := range res.Rows {
		if !row[col].Null {
			return row[col].Type == tuple.TypeInt
		}
	}
	return false
}

// compat 应答常见客户端在连接时发送的探测语句。
func compat(q string) (*db.Result, bool) {
	s := strings.ToLower(strings.TrimSpace(strings.TrimRight(strings.TrimSpace(q), ";")))
	one := func(col, val string) (*db.Result, bool) {
		return &db.Result{Columns: []string{col}, IsQuery: true, Rows: [][]tuple.Value{{tuple.StrValue(val)}}}, true
	}
	switch {
	case strings.HasPrefix(s, "set "), strings.HasPrefix(s, "use "):
		return &db.Result{}, true
	case s == "select database()":
		return one("DATABASE()", "mysqlfake")
	case s == "select version()":
		return one("version()", "8.0.36-mysqlfake")
	case strings.HasPrefix(s, "select @@version_comment"):
		return one("@@version_comment", "mysqlfake")
	case s == "show databases":
		return one("Database", "mysqlfake")
	case strings.HasPrefix(s, "select @@"):
		return one(q[len("select "):], "0")
	}
	return nil, false
}
