// Package tuple 定义列类型、Schema、行值，以及行的二进制序列化。
package tuple

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

type Type uint8

const (
	TypeInt     Type = 1
	TypeVarchar Type = 2
)

const (
	MaxRowSize = 1007 // B+ 树 value 上限 1024 减去 17 字节版本头
	MaxKeySize = 256
)

func (t Type) String() string {
	switch t {
	case TypeInt:
		return "INT"
	case TypeVarchar:
		return "VARCHAR"
	}
	return "UNKNOWN"
}

type Column struct {
	Name       string
	Type       Type
	Len        int // VARCHAR 最大字节数
	PrimaryKey bool
}

type Schema struct {
	Columns []Column
}

// ColumnIndex 返回列下标，不存在返回 -1 (列名大小写不敏感)。
func (s *Schema) ColumnIndex(name string) int {
	for i, c := range s.Columns {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}
	return -1
}

// PKIndex 返回主键列下标，没有返回 -1。
func (s *Schema) PKIndex() int {
	for i, c := range s.Columns {
		if c.PrimaryKey {
			return i
		}
	}
	return -1
}

type Value struct {
	Type Type
	Null bool
	Int  int64
	Str  string
}

func IntValue(v int64) Value  { return Value{Type: TypeInt, Int: v} }
func StrValue(s string) Value { return Value{Type: TypeVarchar, Str: s} }
func NullValue(t Type) Value  { return Value{Type: t, Null: true} }
func (v Value) String() string {
	if v.Null {
		return "NULL"
	}
	if v.Type == TypeInt {
		return fmt.Sprintf("%d", v.Int)
	}
	return v.Str
}

// Compare 比较两个同类型值，NULL 小于任何非 NULL 值。
func Compare(a, b Value) int {
	switch {
	case a.Null && b.Null:
		return 0
	case a.Null:
		return -1
	case b.Null:
		return 1
	}
	if a.Type == TypeInt {
		switch {
		case a.Int < b.Int:
			return -1
		case a.Int > b.Int:
			return 1
		}
		return 0
	}
	return strings.Compare(a.Str, b.Str)
}

var ErrCorrupt = errors.New("tuple: corrupt record")

// Marshal 序列化一行: null 位图 | 各非空字段。
// INT 为 8 字节小端；VARCHAR 为 2 字节长度 + 内容。
func Marshal(s *Schema, row []Value) ([]byte, error) {
	if len(row) != len(s.Columns) {
		return nil, fmt.Errorf("tuple: expected %d values, got %d", len(s.Columns), len(row))
	}
	n := len(s.Columns)
	buf := make([]byte, (n+7)/8, 64)
	for i, c := range s.Columns {
		v := row[i]
		if v.Null {
			buf[i/8] |= 1 << (i % 8)
			continue
		}
		if v.Type != c.Type {
			return nil, fmt.Errorf("tuple: column %q expects %s, got %s", c.Name, c.Type, v.Type)
		}
		switch c.Type {
		case TypeInt:
			buf = binary.LittleEndian.AppendUint64(buf, uint64(v.Int))
		case TypeVarchar:
			if c.Len > 0 && len(v.Str) > c.Len {
				return nil, fmt.Errorf("tuple: value too long for column %q (max %d)", c.Name, c.Len)
			}
			buf = binary.LittleEndian.AppendUint16(buf, uint16(len(v.Str)))
			buf = append(buf, v.Str...)
		}
	}
	if len(buf) > MaxRowSize {
		return nil, fmt.Errorf("tuple: row too large (%d > %d bytes)", len(buf), MaxRowSize)
	}
	return buf, nil
}

func Unmarshal(s *Schema, b []byte) ([]Value, error) {
	n := len(s.Columns)
	bm := (n + 7) / 8
	if len(b) < bm {
		return nil, ErrCorrupt
	}
	pos := bm
	row := make([]Value, n)
	for i, c := range s.Columns {
		if b[i/8]&(1<<(i%8)) != 0 {
			row[i] = NullValue(c.Type)
			continue
		}
		switch c.Type {
		case TypeInt:
			if pos+8 > len(b) {
				return nil, ErrCorrupt
			}
			row[i] = IntValue(int64(binary.LittleEndian.Uint64(b[pos:])))
			pos += 8
		case TypeVarchar:
			if pos+2 > len(b) {
				return nil, ErrCorrupt
			}
			l := int(binary.LittleEndian.Uint16(b[pos:]))
			pos += 2
			if pos+l > len(b) {
				return nil, ErrCorrupt
			}
			row[i] = StrValue(string(b[pos : pos+l]))
			pos += l
		default:
			return nil, ErrCorrupt
		}
	}
	return row, nil
}

// EncodeKey 把主键值编码为保序的字节串 (bytes.Compare 与 Compare 一致)。
func EncodeKey(v Value) ([]byte, error) {
	if v.Null {
		return nil, errors.New("tuple: primary key cannot be NULL")
	}
	switch v.Type {
	case TypeInt:
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(v.Int)^(1<<63))
		return b[:], nil
	case TypeVarchar:
		if len(v.Str) == 0 {
			return nil, errors.New("tuple: primary key cannot be empty")
		}
		if len(v.Str) > MaxKeySize {
			return nil, fmt.Errorf("tuple: primary key too long (max %d bytes)", MaxKeySize)
		}
		return []byte(v.Str), nil
	}
	return nil, errors.New("tuple: bad key type")
}

func DecodeKey(t Type, b []byte) (Value, error) {
	switch t {
	case TypeInt:
		if len(b) != 8 {
			return Value{}, ErrCorrupt
		}
		return IntValue(int64(binary.BigEndian.Uint64(b) ^ (1 << 63))), nil
	case TypeVarchar:
		return StrValue(string(b)), nil
	}
	return Value{}, ErrCorrupt
}

// KeySuccessor 返回紧随 key 之后的最小字节串，用于把闭区间边界转换为开区间。
func KeySuccessor(key []byte) []byte {
	return append(bytes.Clone(key), 0)
}

// MarshalSchema / UnmarshalSchema 供 Catalog 持久化表结构。
func MarshalSchema(s *Schema) []byte {
	var buf []byte
	buf = binary.LittleEndian.AppendUint16(buf, uint16(len(s.Columns)))
	for _, c := range s.Columns {
		buf = binary.LittleEndian.AppendUint16(buf, uint16(len(c.Name)))
		buf = append(buf, c.Name...)
		buf = append(buf, byte(c.Type))
		buf = binary.LittleEndian.AppendUint16(buf, uint16(c.Len))
		if c.PrimaryKey {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	return buf
}

func UnmarshalSchema(b []byte) (*Schema, int, error) {
	if len(b) < 2 {
		return nil, 0, ErrCorrupt
	}
	n := int(binary.LittleEndian.Uint16(b))
	pos := 2
	s := &Schema{}
	for i := 0; i < n; i++ {
		if pos+2 > len(b) {
			return nil, 0, ErrCorrupt
		}
		l := int(binary.LittleEndian.Uint16(b[pos:]))
		pos += 2
		if pos+l+4 > len(b) {
			return nil, 0, ErrCorrupt
		}
		c := Column{Name: string(b[pos : pos+l])}
		pos += l
		c.Type = Type(b[pos])
		c.Len = int(binary.LittleEndian.Uint16(b[pos+1:]))
		c.PrimaryKey = b[pos+3] == 1
		pos += 4
		s.Columns = append(s.Columns, c)
	}
	return s, pos, nil
}
