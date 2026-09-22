// Package descriptor 定义 OCI 风格的内容描述符与平台标识。
package descriptor

import (
	"errors"
	"fmt"
	"strings"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// MediaType 标识对象的具体格式。不同格式的标识不得混用。
type MediaType string

const (
	// MediaTypeLayerTar 未压缩 tar 层（本实现唯一支持的层格式）。
	MediaTypeLayerTar MediaType = "application/vnd.oci.image.layer.v1.tar"
	// MediaTypeImageConfig v1 镜像运行时配置。
	MediaTypeImageConfig MediaType = "application/vnd.oci.image.config.v1+json"
	// MediaTypeImageManifest v1 镜像清单。
	MediaTypeImageManifest MediaType = "application/vnd.oci.image.manifest.v1+json"
	// MediaTypeImageIndex v1 多架构索引。
	MediaTypeImageIndex MediaType = "application/vnd.oci.image.index.v1+json"

	// MediaTypeLegacyManifest 旧版（v0）清单：仅允许读取/兼容引用，
	// 不再允许新写入。
	MediaTypeLegacyManifest MediaType = "application/vnd.example.manifest.v0+json"
)

// Descriptor 描述一个内容寻址对象。
type Descriptor struct {
	MediaType MediaType     `json:"mediaType"`
	Digest    digest.Digest `json:"digest"`
	Size      int64         `json:"size"`
}

// Platform 标识清单适用的目标平台。
type Platform struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	// Variant 为架构变体（如 arm64 的 v7/v8）；空字符串表示未指定。
	Variant string `json:"variant,omitempty"`
}

// String 返回平台的稳定字符串形式 os/arch[/variant]。
// 字段统一转为小写，保证 "linux/AMD64" 与 "linux/amd64" 结论一致。
func (p Platform) String() string {
	var b strings.Builder
	b.WriteString(strings.ToLower(p.OS))
	b.WriteByte('/')
	b.WriteString(strings.ToLower(p.Architecture))
	if v := strings.ToLower(p.Variant); v != "" {
		b.WriteByte('/')
		b.WriteString(v)
	}
	return b.String()
}

// Normalize 返回字段归一化为小写且去除首尾空白后的平台副本。
func (p Platform) Normalize() Platform {
	return Platform{
		OS:           strings.ToLower(strings.TrimSpace(p.OS)),
		Architecture: strings.ToLower(strings.TrimSpace(p.Architecture)),
		Variant:      strings.ToLower(strings.TrimSpace(p.Variant)),
	}
}

// Match 判定 target 是否满足当前选择条件 p：
//   - os、architecture 必须完全相等（小写归一化后）；
//   - p.Variant 为空表示"不约束变体"，target 任意变体均满足；
//     p.Variant 非空时，target.Variant 必须与其相等；
//   - target 自身 Variant 为空而 p 要求非空时不匹配。
func (p Platform) Match(target Platform) bool {
	c := p.Normalize()
	t := target.Normalize()
	if c.OS == "" || c.Architecture == "" {
		return false
	}
	if c.OS != t.OS || c.Architecture != t.Architecture {
		return false
	}
	if c.Variant != "" && c.Variant != t.Variant {
		return false
	}
	return true
}

var (
	// ErrEmptyDigest 表示描述符缺少摘要。
	ErrEmptyDigest = errors.New("descriptor: digest is empty")
	// ErrInvalidSize 表示描述符大小为负。
	ErrInvalidSize = errors.New("descriptor: size is negative")
	// ErrEmptyMediaType 表示描述符缺少 mediaType。
	ErrEmptyMediaType = errors.New("descriptor: mediaType is empty")
)

// Validate 校验描述符自身字段的完整性（不触碰存储内容）。
func (d Descriptor) Validate() error {
	if d.MediaType == "" {
		return ErrEmptyMediaType
	}
	if d.Digest.IsZero() {
		return fmt.Errorf("%w (mediaType=%s)", ErrEmptyDigest, d.MediaType)
	}
	if d.Size < 0 {
		return fmt.Errorf("%w: %d (digest=%s)", ErrInvalidSize, d.Size, d.Digest)
	}
	return nil
}
