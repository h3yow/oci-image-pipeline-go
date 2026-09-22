// Package digest 定义内容摘要的口径与解析、计算、校验工具。
//
// 摘要口径（稳定、不得混用）：
//   - 算法固定为 sha256，摘要字符串形如 "sha256:<64个小写十六进制字符>"。
//   - 摘要一律对“对象的原始字节”计算；层与配置按其各自的 tar/config 字节计算，
//     镜像摘要即清单 JSON 规范字节（见 manifest 包）的摘要。
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	// AlgorithmSHA256 是本系统唯一支持的摘要算法标识。
	AlgorithmSHA256 = "sha256"
	// hexLen sha256 摘要的十六进制长度。
	hexLen = 64
)

// ErrInvalid 摘要字符串无法按口径解析（算法不受支持或编码非法）。
var ErrInvalid = errors.New("digest: invalid digest")

// Digest 内容寻址标识，不可变，字符串形态为 "sha256:<hex>"。
type Digest string

// Algorithm 返回摘要算法部分（如 "sha256"）。
func (d Digest) Algorithm() string {
	algo, _, _ := strings.Cut(string(d), ":")
	return algo
}

// Hex 返回冒号后的十六进制部分；格式非法时返回空串。
func (d Digest) Hex() string {
	_, h, ok := strings.Cut(string(d), ":")
	if !ok {
		return ""
	}
	return h
}

// String 返回摘要的规范字符串形态。
func (d Digest) String() string { return string(d) }

// Short 返回用于日志的短标识（sha256: 前缀 + 前 12 位十六进制）。
func (d Digest) Short() string {
	h := d.Hex()
	if len(h) > 12 {
		h = h[:12]
	}
	return d.Algorithm() + ":" + h
}

// IsZero 报告摘要是否为空值。
func (d Digest) IsZero() bool { return d == "" }

// Parse 按口径解析并严格校验摘要字符串：
//   - 必须为 "<algo>:<hex>" 形态；
//   - algo 仅接受 sha256（其他算法明确拒绝，不做猜测/映射）；
//   - hex 必须为 64 个小写十六进制字符。
func Parse(s string) (Digest, error) {
	algo, h, ok := strings.Cut(strings.TrimSpace(s), ":")
	if !ok || algo != AlgorithmSHA256 || len(h) != hexLen {
		return "", fmt.Errorf("%w: %q", ErrInvalid, s)
	}
	if _, err := hex.DecodeString(h); err != nil {
		return "", fmt.Errorf("%w: %q: %v", ErrInvalid, s, err)
	}
	// DecodeString 同时接受大写十六进制，口径要求小写，显式排除。
	if h != strings.ToLower(h) {
		return "", fmt.Errorf("%w: %q: uppercase hex", ErrInvalid, s)
	}
	return Digest(s), nil
}

// FromBytes 按口径计算字节内容的摘要。
func FromBytes(data []byte) Digest {
	sum := sha256.Sum256(data)
	return Digest(AlgorithmSHA256 + ":" + hex.EncodeToString(sum[:]))
}

// Sum 按口径计算 reader 全部字节的摘要。
func Sum(r io.Reader) (Digest, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return Digest(AlgorithmSHA256 + ":" + hex.EncodeToString(h.Sum(nil))), nil
}

// VerifyBytes 校验给定字节的摘要是否与 d 一致；d 本身非法时返回 ErrInvalid。
func (d Digest) VerifyBytes(data []byte) error {
	want, err := Parse(string(d))
	if err != nil {
		return err
	}
	got := FromBytes(data)
	if got != want {
		return fmt.Errorf("%w: expected %s, got %s", ErrMismatch, want, got)
	}
	return nil
}

// ErrMismatch 内容计算出的摘要与声明摘要不一致。
var ErrMismatch = errors.New("digest: content digest mismatch")
