// Package descriptor 定义 OCI 风格的内容描述符与平台标识。
package descriptor

import (
	"errors"

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

// String 返回 platform 的稳定字符串形式 os/arch[/variant]。
func (p Platform) String() string { return "" }

// Match 判定目标平台是否与当前选择条件相等。
// variant 为空表示"不约束"：条件方 variant 为空时匹配任意变体。
func (p Platform) Match(target Platform) bool { return false }

var (
	// ErrEmptyDigest 表示描述符缺少摘要。
	ErrEmptyDigest = errors.New("descriptor: digest is empty")
	// ErrInvalidSize 表示描述符大小为负。
	ErrInvalidSize = errors.New("descriptor: size is negative")
)

// Validate 校验描述符自身字段的完整性。
func (d Descriptor) Validate() error { return nil }
