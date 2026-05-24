// Package proto 提供隧道认证协议
//
// 协议格式：
//
//	隧道连接: [4字节魔数 "TUNL"] [2字节token长度] [token]
//	公共连接: 直接发送数据（不匹配魔数）
//
// 服务端通过 peek 前 4 字节判断连接类型，避免消耗公共连接数据。
package proto

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

// TunnelMagic 隧道认证魔数
var TunnelMagic = [4]byte{'T', 'U', 'N', 'L'}

// MaxTokenLen 最大令牌长度
const MaxTokenLen = 256

// WriteTunnelAuth 写入隧道认证数据
//
// 格式：[4字节魔数] [2字节token长度] [token]
func WriteTunnelAuth(w io.Writer, token string) error {
	tokenBytes := []byte(token)
	if len(tokenBytes) > MaxTokenLen {
		return fmt.Errorf("token too long: %d > %d", len(tokenBytes), MaxTokenLen)
	}

	buf := make([]byte, 4+2+len(tokenBytes))
	copy(buf[0:4], TunnelMagic[:])
	binary.BigEndian.PutUint16(buf[4:6], uint16(len(tokenBytes)))
	copy(buf[6:], tokenBytes)

	_, err := w.Write(buf)
	return err
}

// ReadTunnelAuth 读取并验证隧道认证数据
//
// 返回解析出的 token，失败返回错误。
func ReadTunnelAuth(r io.Reader) (string, error) {
	header := make([]byte, 4+2)
	if _, err := io.ReadFull(r, header); err != nil {
		return "", fmt.Errorf("read header: %w", err)
	}

	// 验证魔数
	if header[0] != TunnelMagic[0] || header[1] != TunnelMagic[1] ||
		header[2] != TunnelMagic[2] || header[3] != TunnelMagic[3] {
		return "", fmt.Errorf("invalid magic: %q", header[0:4])
	}

	// 读取 token
	tokenLen := binary.BigEndian.Uint16(header[4:6])
	if tokenLen > MaxTokenLen {
		return "", fmt.Errorf("token length too large: %d", tokenLen)
	}

	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(r, tokenBuf); err != nil {
		return "", fmt.Errorf("read token: %w", err)
	}

	return string(tokenBuf), nil
}

// IsTunnelConn 检查连接是否为隧道连接
//
// 通过 peek 前 4 字节判断魔数，不消耗数据。
func IsTunnelConn(br *bufio.Reader) bool {
	peeked, err := br.Peek(4)
	if err != nil {
		return false
	}
	return peeked[0] == TunnelMagic[0] && peeked[1] == TunnelMagic[1] &&
		peeked[2] == TunnelMagic[2] && peeked[3] == TunnelMagic[3]
}
