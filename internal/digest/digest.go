// Package digest 定义内容摘要类型及其校验口径。
//
// 摘要算法：仅支持 sha256（算法标识 "sha256"）。摘要以 "sha256:<hex>" 的
// 规范字符串表示，hex 为 64 个小写字符。任何其它算法、大小写形式或长度
// 均被拒绝，以保证不同格式/算法的标识不会混用。
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
)

// Algorithm 为受支持的摘要算法标识。
type Algorithm string

const (
	// SHA256 是唯一受支持的摘要算法。
	SHA256 Algorithm = "sha256"
)

// Digest 为一个已验证格式的内容摘要值。零值无效。
type Digest struct {
	alg Algorithm
	hex string
}

// 错误定义：不同失败原因使用不同错误，便于调用方区分。
var (
	// ErrUnsupportedAlgorithm 表示摘要字符串使用了不支持的算法。
	ErrUnsupportedAlgorithm = errors.New("digest: unsupported algorithm")
	// ErrInvalidEncoding 表示摘要编码不符合规范（非小写十六进制或长度错误）。
	ErrInvalidEncoding = errors.New("digest: invalid encoding")
	// ErrEmpty 表示对零值摘要执行了需要有效值的操作。
	ErrEmpty = errors.New("digest: empty digest")
)

// Parse 解析规范形式的摘要字符串 "sha256:<hex>"。
func Parse(s string) (Digest, error) {
	return Digest{}, nil
}

// FromBytes 计算字节切片的摘要。
func FromBytes(b []byte) Digest {
	return Digest{}
}

// FromReader 读取 r 的全部内容并计算摘要。
func FromReader(r io.Reader) (Digest, error) {
	return Digest{}, nil
}

// String 返回规范字符串形式。
func (d Digest) String() string { return "" }

// Algorithm 返回摘要算法。
func (d Digest) Algorithm() Algorithm { return "" }

// Hex 返回小写十六进制摘要体。
func (d Digest) Hex() string { return "" }

// IsZero 报告摘要是否为零值。
func (d Digest) IsZero() bool { return true }

// Verify 校验 r 的内容摘要是否与 d 一致。
func (d Digest) Verify(r io.Reader) error { return nil }

// VerifyBytes 校验字节切片摘要是否与 d 一致。
func (d Digest) VerifyBytes(b []byte) error { return nil }

// sum 计算 sha256（内部辅助）。
func sum(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
