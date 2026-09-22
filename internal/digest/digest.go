// Package digest 定义内容摘要类型及其校验口径。
//
// 摘要算法：仅支持 sha256（算法标识 "sha256"）。摘要以 "sha256:<hex>" 的
// 规范字符串表示，hex 为 64 个小写字符。任何其它算法、大小写形式或长度
// 均被拒绝，以保证不同格式/算法的标识不会混用。
package digest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// Algorithm 为受支持的摘要算法标识。
type Algorithm string

const (
	// SHA256 是唯一受支持的摘要算法。
	SHA256 Algorithm = "sha256"
)

// hexLen 为 sha256 的十六进制长度。
const hexLen = 64

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
	// ErrMismatch 表示校验时实际摘要与期望摘要不一致。
	ErrMismatch = errors.New("digest: digest mismatch")
)

// New 由已验证的算法与十六进制体构造摘要；非法输入返回错误。
func New(alg Algorithm, h string) (Digest, error) {
	if alg != SHA256 {
		return Digest{}, fmt.Errorf("%w: %q", ErrUnsupportedAlgorithm, alg)
	}
	if len(h) != hexLen {
		return Digest{}, fmt.Errorf("%w: want %d hex chars, got %d", ErrInvalidEncoding, hexLen, len(h))
	}
	if _, err := hex.DecodeString(h); err != nil {
		return Digest{}, fmt.Errorf("%w: %v", ErrInvalidEncoding, err)
	}
	for i := 0; i < len(h); i++ {
		c := h[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			return Digest{}, fmt.Errorf("%w: non-lowercase-hex character %q", ErrInvalidEncoding, c)
		}
	}
	return Digest{alg: alg, hex: h}, nil
}

// Parse 解析规范形式的摘要字符串 "sha256:<hex>"。
func Parse(s string) (Digest, error) {
	alg, h, ok := bytes.Cut([]byte(s), []byte{':'})
	if !ok {
		return Digest{}, fmt.Errorf("%w: missing ':' in %q", ErrInvalidEncoding, s)
	}
	return New(Algorithm(alg), string(h))
}

// FromBytes 计算字节切片的摘要。
func FromBytes(b []byte) Digest {
	sum := sha256.Sum256(b)
	return Digest{alg: SHA256, hex: hex.EncodeToString(sum[:])}
}

// FromReader 读取 r 的全部内容并计算摘要。
func FromReader(r io.Reader) (Digest, error) {
	h, err := sum(r)
	if err != nil {
		return Digest{}, err
	}
	return Digest{alg: SHA256, hex: h}, nil
}

// String 返回规范字符串形式。
func (d Digest) String() string {
	if d.IsZero() {
		return ""
	}
	return string(d.alg) + ":" + d.hex
}

// MarshalText 实现 encoding.TextMarshaler，使摘要可稳定进入 JSON。
func (d Digest) MarshalText() ([]byte, error) {
	if d.IsZero() {
		return []byte(""), nil
	}
	return []byte(d.String()), nil
}

// UnmarshalText 实现 encoding.TextUnmarshaler，拒绝任何非规范输入。
func (d *Digest) UnmarshalText(b []byte) error {
	if len(b) == 0 {
		*d = Digest{}
		return nil
	}
	parsed, err := Parse(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// Algorithm 返回摘要算法。
func (d Digest) Algorithm() Algorithm { return d.alg }

// Hex 返回小写十六进制摘要体。
func (d Digest) Hex() string { return d.hex }

// IsZero 报告摘要是否为零值。
func (d Digest) IsZero() bool { return d.alg == "" || d.hex == "" }

// Equal 判定两个摘要是否相等。
func (d Digest) Equal(o Digest) bool {
	return d.alg == o.alg && d.hex == o.hex
}

// Verify 校验 r 的内容摘要是否与 d 一致；不一致返回 ErrMismatch。
func (d Digest) Verify(r io.Reader) error {
	if d.IsZero() {
		return ErrEmpty
	}
	got, err := FromReader(r)
	if err != nil {
		return err
	}
	if !got.Equal(d) {
		return fmt.Errorf("%w: expected %s, got %s", ErrMismatch, d, got)
	}
	return nil
}

// VerifyBytes 校验字节切片摘要是否与 d 一致。
func (d Digest) VerifyBytes(b []byte) error {
	return d.Verify(bytes.NewReader(b))
}

// sum 计算 sha256 十六进制字符串（内部辅助）。
func sum(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
