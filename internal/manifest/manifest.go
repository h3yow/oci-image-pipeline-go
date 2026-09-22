// Package manifest 实现镜像配置、清单（v1 与旧版 v0）及多架构索引的
// 编码、解码与版本兼容判定。
//
// 兼容范围（稳定约定）：
//   - manifestVersion=1：当前格式，可读可写；
//   - manifestVersion=0：旧格式，仅可读（用于兼容旧引用），解码时缺失字段
//     按"零值缺省"填充（created 缺省为零时间；annotations 缺省为空 map），
//     但不允许以 v0 重新编码写入；
//   - 其它版本与未知/混用 mediaType：明确拒绝（ErrUnsupportedFormat）。
//   - JSON 中出现的未知字段：保留往返（解码进入 Unknown 再原样输出），
//     不影响摘要计算之外的语义；摘要一律以"规范化编码后的字节"为准。
package manifest

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// Version 为清单格式版本。
type Version int

const (
	// Version0 旧版清单（只读兼容）。
	Version0 Version = 0
	// Version1 当前清单格式。
	Version1 Version = 1
)

// ImageConfig 为镜像运行时配置（v1）。
type ImageConfig struct {
	Created    *time.Time        `json:"created,omitempty"`
	Architecture string          `json:"architecture,omitempty"`
	OS         string            `json:"os,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
	// RootFSDiffIDs 为各层在构建上下文中的 diff id（sha256，未压缩口径）。
	RootFSDiffIDs []string `json:"rootfs.diff_ids,omitempty"`
}

// Layer 为清单中的层条目。
type Layer struct {
	Descriptor descriptor.Descriptor
}

// Manifest 为与版本无关的内存表示。
type Manifest struct {
	Version     Version
	SchemaVer   int
	Config      *descriptor.Descriptor // v1 有；v0 可能缺失
	Layers      []descriptor.Descriptor
	Annotations map[string]string
	// unknown 保留 JSON 中未识别字段，保证旧/新字段往返稳定。
	unknown map[string]json.RawMessage
}

// Index 为多架构索引。
type Index struct {
	Manifests   []IndexEntry
	Annotations map[string]string
	unknown     map[string]json.RawMessage
}

// IndexEntry 为索引中的一条清单引用。
type IndexEntry struct {
	Descriptor descriptor.Descriptor
	Platform   *descriptor.Platform
}

var (
	// ErrUnsupportedFormat 版本超出支持范围或 mediaType 混用。
	ErrUnsupportedFormat = errors.New("manifest: unsupported format/version")
	// ErrMissingField 必填字段缺失。
	ErrMissingField = errors.New("manifest: required field missing")
	// ErrDuplicatePlatform 索引中存在重复平台条目。
	ErrDuplicatePlatform = errors.New("manifest: duplicate platform in index")
	// ErrPlatformNotFound 索引中没有任何条目匹配请求平台。
	ErrPlatformNotFound = errors.New("manifest: no manifest matches platform")
	// ErrAmbiguousPlatform 多个条目以相同优先级匹配请求平台。
	ErrAmbiguousPlatform = errors.New("manifest: ambiguous platform match")
	// ErrPlatformMissing 清单/索引条目缺少平台信息。
	ErrPlatformMissing = errors.New("manifest: platform missing")
)

// EncodeV1 将内存清单规范化编码为 v1 JSON 字节。
// 规范化口径：字段顺序固定、无多余空白、UTF-8；相同逻辑内容必产生相同字节，
// 进而产生相同摘要。
func EncodeV1(m *Manifest) ([]byte, error) { return nil, nil }

// Decode 依据 mediaType 解码清单字节。v0 仅允许读入并转换为内存表示，
// 字段缺失按既定缺省语义填充。
func Decode(mediaType descriptor.MediaType, raw []byte) (*Manifest, error) {
	return nil, nil
}

// EncodeIndex 将索引规范化编码为 v1 JSON 字节。
func EncodeIndex(idx *Index) ([]byte, error) { return nil, nil }

// DecodeIndex 解码索引字节并校验：平台缺失/重复按规则报错。
func DecodeIndex(raw []byte) (*Index, descriptor.MediaType, error) {
	return nil, "", nil
}

// SelectPlatform 在索引中为 p 选择唯一清单：
//   - 无匹配：ErrPlatformNotFound；
//   - 多个条目完全相等（含 variant）：ErrDuplicatePlatform；
//   - 多个条目在"variant 不约束"规则下同等匹配：ErrAmbiguousPlatform；
//   - 命中唯一条目：返回该条目。
func SelectPlatform(idx *Index, p descriptor.Platform) (IndexEntry, error) {
	return IndexEntry{}, nil
}

// EncodeConfig 将镜像配置规范化编码。
func EncodeConfig(cfg *ImageConfig) ([]byte, error) { return nil, nil }

// DecodeConfig 解码镜像配置。
func DecodeConfig(raw []byte) (*ImageConfig, error) { return nil, nil }

// DigestOf 返回给定字节的规范摘要（内部口径统一入口）。
func DigestOf(raw []byte) digest.Digest { return digest.FromBytes(raw) }
