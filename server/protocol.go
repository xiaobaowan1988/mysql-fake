// Package server 实现 MySQL 协议子集的网络服务: 事件驱动 I/O (epoll/kqueue) + 动态工作池。
package server

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	comQuit    = 0x01
	comInitDB  = 0x02
	comQuery   = 0x03
	comFieldLs = 0x04
	comPing    = 0x0e

	// 能力标志
	clientLongPassword     = 0x00000001
	clientLongFlag         = 0x00000004
	clientConnectWithDB    = 0x00000008
	clientProtocol41       = 0x00000200
	clientTransactions     = 0x00002000
	clientSecureConnection = 0x00008000
	clientPluginAuth       = 0x00080000
	clientPluginAuthLenenc = 0x00200000

	serverCaps = clientLongPassword | clientLongFlag | clientConnectWithDB | clientProtocol41 |
		clientTransactions | clientSecureConnection | clientPluginAuth

	statusInTrans    = 0x0001
	statusAutocommit = 0x0002

	charsetUTF8 = 45 // utf8mb4_general_ci

	maxPacket = 1<<24 - 1

	typeLongLong  = 0x08
	typeVarString = 0xfd

	errUnknownCmd   = 1047
	errParse        = 1064
	errGeneric      = 1105
	errTooManyConns = 1040
)

var errPacketTooLarge = errors.New("server: packet too large")

func appendLenenc(b []byte, n uint64) []byte {
	switch {
	case n < 251:
		return append(b, byte(n))
	case n < 1<<16:
		return append(b, 0xfc, byte(n), byte(n>>8))
	case n < 1<<24:
		return append(b, 0xfd, byte(n), byte(n>>8), byte(n>>16))
	}
	b = append(b, 0xfe)
	return binary.LittleEndian.AppendUint64(b, n)
}

func appendLenencStr(b []byte, s string) []byte {
	b = appendLenenc(b, uint64(len(s)))
	return append(b, s...)
}

// readLenenc 解析长度编码整数，返回值、消耗字节数、是否为 NULL(0xfb)。
func readLenenc(b []byte) (v uint64, n int, null bool, err error) {
	if len(b) == 0 {
		return 0, 0, false, errors.New("short lenenc")
	}
	switch c := b[0]; {
	case c < 0xfb:
		return uint64(c), 1, false, nil
	case c == 0xfb:
		return 0, 1, true, nil
	case c == 0xfc && len(b) >= 3:
		return uint64(binary.LittleEndian.Uint16(b[1:])), 3, false, nil
	case c == 0xfd && len(b) >= 4:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16, 4, false, nil
	case c == 0xfe && len(b) >= 9:
		return binary.LittleEndian.Uint64(b[1:]), 9, false, nil
	}
	return 0, 0, false, errors.New("bad lenenc")
}

// framePacket 给负载加上 4 字节包头 (3 字节长度 + 序号)；超过 16MB 的负载按协议拆包。
func framePacket(dst []byte, seq *byte, payload []byte) []byte {
	for {
		n := len(payload)
		if n > maxPacket {
			n = maxPacket
		}
		dst = append(dst, byte(n), byte(n>>8), byte(n>>16), *seq)
		*seq++
		dst = append(dst, payload[:n]...)
		payload = payload[n:]
		if n < maxPacket {
			return dst
		}
	}
}

// parsePackets 从缓冲区中取出所有完整的包，返回 (序号, 负载) 列表和剩余字节。
func parsePackets(buf []byte) (pkts []packet, rest []byte, err error) {
	for len(buf) >= 4 {
		n := int(buf[0]) | int(buf[1])<<8 | int(buf[2])<<16
		if n == maxPacket {
			return pkts, buf, errPacketTooLarge // 不支持多包请求
		}
		if len(buf) < 4+n {
			break
		}
		pkts = append(pkts, packet{seq: buf[3], payload: append([]byte(nil), buf[4:4+n]...)})
		buf = buf[4+n:]
	}
	return pkts, buf, nil
}

type packet struct {
	seq     byte
	payload []byte
}

func okPayload(affected uint64, status uint16) []byte {
	b := []byte{0x00}
	b = appendLenenc(b, affected)
	b = appendLenenc(b, 0) // last insert id
	b = binary.LittleEndian.AppendUint16(b, status)
	return binary.LittleEndian.AppendUint16(b, 0) // warnings
}

func errPayload(code uint16, state, msg string) []byte {
	b := []byte{0xff}
	b = binary.LittleEndian.AppendUint16(b, code)
	b = append(b, '#')
	b = append(b, state...)
	return append(b, msg...)
}

func eofPayload(status uint16) []byte {
	b := []byte{0xfe}
	b = binary.LittleEndian.AppendUint16(b, 0)
	return binary.LittleEndian.AppendUint16(b, status)
}

// greetingPayload 是握手包 (Protocol::HandshakeV10)。
func greetingPayload(connID uint32, salt [20]byte) []byte {
	b := []byte{10}
	b = append(b, "8.0.36-mysqlfake\x00"...)
	b = binary.LittleEndian.AppendUint32(b, connID)
	b = append(b, salt[:8]...)
	b = append(b, 0)
	b = binary.LittleEndian.AppendUint16(b, uint16(serverCaps&0xffff))
	b = append(b, charsetUTF8)
	b = binary.LittleEndian.AppendUint16(b, statusAutocommit)
	b = binary.LittleEndian.AppendUint16(b, uint16(serverCaps>>16))
	b = append(b, 21) // auth-plugin-data 总长
	b = append(b, make([]byte, 10)...)
	b = append(b, salt[8:20]...)
	b = append(b, 0)
	return append(b, "mysql_native_password\x00"...)
}

// handshakeResponse 是客户端握手应答里我们关心的部分。
type handshakeResponse struct {
	caps     uint32
	user, db string
}

func parseHandshakeResponse(p []byte) (hr handshakeResponse, err error) {
	if len(p) < 32 {
		return hr, errors.New("handshake response too short")
	}
	hr.caps = binary.LittleEndian.Uint32(p)
	if hr.caps&clientProtocol41 == 0 {
		return hr, errors.New("client does not support protocol 4.1")
	}
	pos := 32
	nul := func() (string, error) {
		for i := pos; i < len(p); i++ {
			if p[i] == 0 {
				s := string(p[pos:i])
				pos = i + 1
				return s, nil
			}
		}
		return "", errors.New("unterminated string in handshake response")
	}
	if hr.user, err = nul(); err != nil {
		return
	}
	switch { // 跳过认证数据 (不校验口令)
	case hr.caps&clientPluginAuthLenenc != 0:
		n, k, _, e := readLenenc(p[pos:])
		if e != nil || pos+k+int(n) > len(p) {
			return hr, errors.New("bad auth data")
		}
		pos += k + int(n)
	case hr.caps&clientSecureConnection != 0:
		if pos >= len(p) || pos+1+int(p[pos]) > len(p) {
			return hr, errors.New("bad auth data")
		}
		pos += 1 + int(p[pos])
	default:
		if _, err = nul(); err != nil {
			return
		}
	}
	if hr.caps&clientConnectWithDB != 0 && pos < len(p) {
		hr.db, _ = nul()
	}
	return hr, nil
}

func mysqlErrFor(err error) (uint16, string) {
	if err == nil {
		return 0, ""
	}
	msg := err.Error()
	if len(msg) >= 12 && msg[:12] == "syntax error" {
		return errParse, "42000"
	}
	return errGeneric, "HY000"
}

func colTypeName(isInt bool) string {
	if isInt {
		return "BIGINT"
	}
	return "VARCHAR"
}

var _ = fmt.Sprintf
