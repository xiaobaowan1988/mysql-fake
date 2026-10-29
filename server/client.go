package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

// Client 是一个极简的 MySQL 文本协议客户端 (握手、COM_QUERY、COM_PING)，用于测试和演示。
type Client struct {
	conn net.Conn
	seq  byte
	buf  []byte
}

// ServerError 是服务端返回的 ERR 包。
type ServerError struct {
	Code  uint16
	State string
	Msg   string
}

func (e *ServerError) Error() string { return fmt.Sprintf("ERROR %d (%s): %s", e.Code, e.State, e.Msg) }

// QueryResult 是客户端解析出的结果。NULL 用 nil 表示。
type QueryResult struct {
	Columns  []string
	Rows     [][]*string
	Affected uint64
	Status   uint16
}

func Dial(addr string) (*Client, error) {
	nc, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: nc}
	nc.SetDeadline(time.Now().Add(10 * time.Second))
	greet, err := c.readPacket()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if len(greet) > 0 && greet[0] == 0xff {
		nc.Close()
		return nil, parseErr(greet)
	}
	if len(greet) == 0 || greet[0] != 10 {
		nc.Close()
		return nil, fmt.Errorf("unexpected greeting")
	}
	resp := binary.LittleEndian.AppendUint32(nil, clientProtocol41|clientSecureConnection|clientPluginAuth|clientLongPassword|clientTransactions)
	resp = binary.LittleEndian.AppendUint32(resp, 1<<24)
	resp = append(resp, charsetUTF8)
	resp = append(resp, make([]byte, 23)...)
	resp = append(resp, "tester\x00"...)
	resp = append(resp, 0) // 空的认证数据
	resp = append(resp, "mysql_native_password\x00"...)
	if err := c.writePacket(resp); err != nil {
		nc.Close()
		return nil, err
	}
	ok, err := c.readPacket()
	if err != nil {
		nc.Close()
		return nil, err
	}
	if ok[0] == 0xff {
		nc.Close()
		return nil, parseErr(ok)
	}
	nc.SetDeadline(time.Time{}) // 握手超时只用于握手阶段
	return c, nil
}

func parseErr(p []byte) error {
	e := &ServerError{Code: binary.LittleEndian.Uint16(p[1:])}
	if len(p) > 9 && p[3] == '#' {
		e.State, e.Msg = string(p[4:9]), string(p[9:])
	} else {
		e.Msg = string(p[3:])
	}
	return e
}

func (c *Client) writePacket(payload []byte) error {
	_, err := c.conn.Write(framePacket(nil, &c.seq, payload))
	return err
}

func (c *Client) readPacket() ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.conn, hdr[:]); err != nil {
		return nil, err
	}
	n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	c.seq = hdr[3] + 1
	p := make([]byte, n)
	_, err := io.ReadFull(c.conn, p)
	return p, err
}

// SetDeadline 设置读写超时。
func (c *Client) SetDeadline(t time.Time) { c.conn.SetDeadline(t) }

func (c *Client) Ping() error {
	c.seq = 0
	if err := c.writePacket([]byte{comPing}); err != nil {
		return err
	}
	p, err := c.readPacket()
	if err != nil {
		return err
	}
	if p[0] == 0xff {
		return parseErr(p)
	}
	return nil
}

// Query 发送一条 SQL 并解析响应 (OK / ERR / 结果集)。
func (c *Client) Query(q string) (*QueryResult, error) {
	c.seq = 0
	if err := c.writePacket(append([]byte{comQuery}, q...)); err != nil {
		return nil, err
	}
	p, err := c.readPacket()
	if err != nil {
		return nil, err
	}
	switch p[0] {
	case 0xff:
		return nil, parseErr(p)
	case 0x00:
		aff, k, _, _ := readLenenc(p[1:])
		_, k2, _, _ := readLenenc(p[1+k:])
		st := binary.LittleEndian.Uint16(p[1+k+k2:])
		return &QueryResult{Affected: aff, Status: st}, nil
	}
	ncols, _, _, err := readLenenc(p)
	if err != nil {
		return nil, err
	}
	res := &QueryResult{}
	for i := uint64(0); i < ncols; i++ {
		def, err := c.readPacket()
		if err != nil {
			return nil, err
		}
		// 跳过 catalog/schema/table/org_table，取 name
		pos := 0
		var name string
		for f := 0; f < 5; f++ {
			n, k, _, e := readLenenc(def[pos:])
			if e != nil {
				return nil, e
			}
			if f == 4 {
				name = string(def[pos+k : pos+k+int(n)])
			}
			pos += k + int(n)
		}
		res.Columns = append(res.Columns, name)
	}
	if _, err := c.readPacket(); err != nil { // EOF
		return nil, err
	}
	for {
		row, err := c.readPacket()
		if err != nil {
			return nil, err
		}
		if row[0] == 0xfe && len(row) < 9 {
			res.Status = binary.LittleEndian.Uint16(row[3:])
			return res, nil
		}
		if row[0] == 0xff {
			return nil, parseErr(row)
		}
		var vals []*string
		pos := 0
		for pos < len(row) {
			n, k, null, e := readLenenc(row[pos:])
			if e != nil {
				return nil, e
			}
			pos += k
			if null {
				vals = append(vals, nil)
				continue
			}
			s := string(row[pos : pos+int(n)])
			pos += int(n)
			vals = append(vals, &s)
		}
		res.Rows = append(res.Rows, vals)
	}
}

// Quit 发送 COM_QUIT 并关闭连接。
func (c *Client) Quit() {
	c.seq = 0
	c.writePacket([]byte{comQuit})
	c.conn.Close()
}

func (c *Client) Close() { c.conn.Close() }
