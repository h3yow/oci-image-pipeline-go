// Package manifest 实现镜像配置、清单（v1 与旧版 v0）及多架构索引的
// 编码、解码与版本兼容判定。
//
// 兼容范围（稳定约定）：
//   - manifestVersion=1：当前格式，可读可写；
//   - manifestVersion=0：旧格式，仅可读（用于兼容旧引用），解码时缺失字段
//     按"零值缺省"填充（created 缺省为零时间；annotations 缺省为空 map），
//     但不允许以 v0 重新编码写入（EncodeV1 会拒绝 Version0）；
//   - 其它版本与未知/混用 mediaType：明确拒绝（ErrUnsupportedFormat）；
//   - JSON 中出现的未知字段：解码时保留在 Raw 中并在规范化重编码时原样
//     保留（键排序），不改变既有语义；
//   - 摘要一律以"规范化编码后的字节"为唯一口径。
package manifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	Created      *time.Time        `json:"created,omitempty"`
	Architecture string            `json:"architecture,omitempty"`
	OS           string            `json:"os,omitempty"`
	Config       map[string]string `json:"config,omitempty"`
	// RootFSDiffIDs 为各层在构建上下文中的 diff id（sha256，未压缩口径）。
	RootFSDiffIDs []string `json:"rootfs.diff_ids,omitempty"`
}

// Layer 为清单中的层条目（当前与 descriptor.Descriptor 等价，保留显式类型
// 以便后续扩展带来源信息）。
type Layer struct {
	Descriptor descriptor.Descriptor
}

// Manifest 为与版本无关的内存表示。
type Manifest struct {
	Version   Version
	Config    *descriptor.Descriptor // v1 必填；v0 可能为 nil（缺省语义）
	Layers    []descriptor.Descriptor
	Annotations map[string]string
	// raw 保留原始 JSON 的有序键值，用于稳定往返与摘要复算。
	raw orderedObject
}

// Index 为多架构索引。
type Index struct {
	Manifests   []IndexEntry
	Annotations map[string]string
	raw         orderedObject
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
	// ErrDuplicatePlatform 索引中存在完全相同的重复平台条目。
	ErrDuplicatePlatform = errors.New("manifest: duplicate platform in index")
	// ErrPlatformNotFound 索引中没有任何条目匹配请求平台。
	ErrPlatformNotFound = errors.New("manifest: no manifest matches platform")
	// ErrAmbiguousPlatform 多个条目在 variant 不约束规则下同等匹配。
	ErrAmbiguousPlatform = errors.New("manifest: ambiguous platform match")
	// ErrPlatformMissing 索引条目缺少平台信息。
	ErrPlatformMissing = errors.New("manifest: platform missing")
)

// ---- 规范化 JSON 原语 -------------------------------------------------------

// orderedObject 以固定键序保存 JSON 对象，保证编码字节确定。
type orderedObject struct {
	keys []string
	vals map[string]json.RawMessage
}

func (o *orderedObject) set(key string, raw json.RawMessage) {
	if o.vals == nil {
		o.vals = map[string]json.RawMessage{}
	}
	if _, ok := o.vals[key]; !ok {
		o.keys = append(o.keys, key)
	}
	o.vals[key] = raw
}

func (o orderedObject) get(key string) (json.RawMessage, bool) {
	v, ok := o.vals[key]
	return v, ok
}

// encode 按键名升序输出 JSON 对象（未知字段同样参与排序），无多余空白。
func (o orderedObject) encode() ([]byte, error) {
	keys := append([]string(nil), o.keys...)
	sort.Strings(keys)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		raw := bytes.TrimSpace(o.vals[k])
		if len(raw) == 0 {
			raw = json.RawMessage("null")
		}
		buf.Write(raw)
	}
	buf.WriteByte('}')
	// 校验产物本身为合法 JSON。
	var into any
	dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	if err := dec.Decode(&into); err != nil {
		return nil, fmt.Errorf("manifest: internal canonical encode produced invalid JSON: %w", err)
	}
	return buf.Bytes(), nil
}

// decodeObject 将 JSON 解为 orderedObject；非对象报错；保留重复键的最后值
// 但键序按首次出现（编码时仍统一排序，因此重复键不影响结果）。
func decodeObject(raw []byte) (orderedObject, error) {
	var o orderedObject
	if err := json.Unmarshal(raw, &o); err != nil {
		return o, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return o, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return o, fmt.Errorf("%w: expected JSON object", ErrMissingField)
	}
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return o, err
		}
		key := t.(string)
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return o, err
		}
		o.set(key, val)
	}
	return o, nil
}

func marshalRaw(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// ---- 镜像配置 ---------------------------------------------------------------

// EncodeConfig 将镜像配置规范化编码（键名升序）。
func EncodeConfig(cfg *ImageConfig) ([]byte, error) {
	if cfg == nil {
		return nil, fmt.Errorf("%w: nil config", ErrMissingField)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	obj, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	return obj.encode()
}

// DecodeConfig 解码镜像配置；未知字段被忽略（配置不做往返保留）。
func DecodeConfig(raw []byte) (*ImageConfig, error) {
	var cfg ImageConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("manifest: decode image config: %w", err)
	}
	return &cfg, nil
}

// ---- 清单 -------------------------------------------------------------------

// wireManifestV1 为 v1 清单的线上结构。
type wireManifestV1 struct {
	MediaType   descriptor.MediaType   `json:"mediaType"`
	Version     int                    `json:"manifestVersion"`
	Config      *descriptor.Descriptor `json:"config,omitempty"`
	Layers      []descriptor.Descriptor `json:"layers"`
	Annotations map[string]string      `json:"annotations,omitempty"`
}

// wireManifestV0 为旧版清单：无 config、无 mediaType 时按旧 mediaType 处理。
type wireManifestV0 struct {
	Version     int                    `json:"manifestVersion"`
	Layers      []descriptor.Descriptor `json:"layers"`
	Annotations map[string]string      `json:"annotations,omitempty"`
}

// EncodeV1 将内存清单规范化编码为 v1 JSON。拒绝 Version0（旧格式不可再写）。
func EncodeV1(m *Manifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrMissingField)
	}
	if m.Version == Version0 {
		return nil, fmt.Errorf("%w: cannot re-encode legacy v0 manifest", ErrUnsupportedFormat)
	}
	if m.Config == nil {
		return nil, fmt.Errorf("%w: config descriptor", ErrMissingField)
	}
	if err := m.Config.Validate(); err != nil {
		return nil, err
	}
	for i := range m.Layers {
		if err := m.Layers[i].Validate(); err != nil {
			return nil, fmt.Errorf("manifest: layer[%d]: %w", i, err)
		}
	}
	if m.Layers == nil {
		m.Layers = []descriptor.Descriptor{}
	}
	wire := wireManifestV1{
		MediaType:   descriptor.MediaTypeImageManifest,
		Version:     int(Version1),
		Config:      m.Config,
		Layers:      m.Layers,
		Annotations: m.Annotations,
	}
	raw, err := json.Marshal(&wire)
	if err != nil {
		return nil, err
	}
	obj, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	// 保留解码时见到的未知字段（新字段前向兼容，字节稳定）。
	for _, k := range m.raw.keys {
		if _, known := obj.vals[k]; !known {
			obj.set(k, m.raw.vals[k])
		}
	}
	return obj.encode()
}

// Decode 依据 mediaType 解码清单字节。
//
// v0（MediaTypeLegacyManifest 或字节内 manifestVersion=0 且无 v1 特征）：
// 仅读入，config 缺省为 nil、annotations 缺省为空 map。
func Decode(mediaType descriptor.MediaType, raw []byte) (*Manifest, error) {
	obj, err := decodeObject(raw)
	if err != nil {
		return nil, err
	}
	verRaw, hasVer := obj.get("manifestVersion")
	var version int
	if hasVer {
		if err := json.Unmarshal(verRaw, &version); err != nil {
			return nil, fmt.Errorf("manifest: manifestVersion: %w", err)
		}
	}

	switch {
	case mediaType == descriptor.MediaTypeImageManifest || (mediaType == "" && version == int(Version1)):
		return decodeV1(obj, raw)
	case mediaType == descriptor.MediaTypeLegacyManifest || (mediaType == "" && version == int(Version0)):
		return decodeV0(obj)
	default:
		return nil, fmt.Errorf("%w: mediaType=%q manifestVersion=%d",
			ErrUnsupportedFormat, mediaType, version)
	}
}

func decodeV1(obj orderedObject, raw []byte) (*Manifest, error) {
	var wire wireManifestV1
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	if wire.Version != int(Version1) {
		return nil, fmt.Errorf("%w: v1 manifest with manifestVersion=%d",
			ErrUnsupportedFormat, wire.Version)
	}
	if wire.MediaType != "" && wire.MediaType != descriptor.MediaTypeImageManifest {
		// 标识混用：v1 结构却挂着别的 mediaType。
		return nil, fmt.Errorf("%w: mediaType=%q inside v1 manifest",
			ErrUnsupportedFormat, wire.MediaType)
	}
	if wire.Config == nil {
		return nil, fmt.Errorf("%w: config descriptor", ErrMissingField)
	}
	if err := wire.Config.Validate(); err != nil {
		return nil, err
	}
	if wire.Layers == nil {
		wire.Layers = []descriptor.Descriptor{}
	}
	for i := range wire.Layers {
		if err := wire.Layers[i].Validate(); err != nil {
			return nil, fmt.Errorf("manifest: layer[%d]: %w", i, err)
		}
	}
	ann := wire.Annotations
	if ann == nil {
		ann = map[string]string{}
	}
	return &Manifest{
		Version:     Version1,
		Config:      wire.Config,
		Layers:      wire.Layers,
		Annotations: ann,
		raw:         obj,
	}, nil
}

func decodeV0(obj orderedObject) (*Manifest, error) {
	var wire wireManifestV0
	raw, err := obj.encode()
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, err
	}
	if wire.Version != int(Version0) {
		return nil, fmt.Errorf("%w: legacy mediaType carries manifestVersion=%d",
			ErrUnsupportedFormat, wire.Version)
	}
	ann := wire.Annotations
	if ann == nil {
		ann = map[string]string{} // 确定的缺省语义
	}
	layers := wire.Layers
	if layers == nil {
		layers = []descriptor.Descriptor{}
	}
	return &Manifest{
		Version:     Version0,
		Config:      nil, // 旧格式无配置：缺省 nil，而非伪造描述符
		Layers:      layers,
		Annotations: ann,
		raw:         obj,
	}, nil
}

// ---- 多架构索引 -------------------------------------------------------------

// wireIndex 为索引的线上结构。
type wireIndex struct {
	MediaType   descriptor.MediaType `json:"mediaType"`
	Manifests   []wireIndexEntry     `json:"manifests"`
	Annotations map[string]string    `json:"annotations,omitempty"`
}

type wireIndexEntry struct {
	descriptor.Descriptor
	Platform *descriptor.Platform `json:"platform,omitempty"`
}

// EncodeIndex 将索引规范化编码为 v1 JSON 字节。
func EncodeIndex(idx *Index) ([]byte, error) {
	if idx == nil {
		return nil, fmt.Errorf("%w: nil index", ErrMissingField)
	}
	if len(idx.Manifests) == 0 {
		return nil, fmt.Errorf("%w: index has no manifests", ErrMissingField)
	}
	entries := make([]wireIndexEntry, 0, len(idx.Manifests))
	seen := map[string]int{}
	for i, e := range idx.Manifests {
		if err := e.Descriptor.Validate(); err != nil {
			return nil, fmt.Errorf("index: manifests[%d]: %w", i, err)
		}
		if e.Descriptor.MediaType != descriptor.MediaTypeImageManifest &&
			e.Descriptor.MediaType != descriptor.MediaTypeLegacyManifest {
			return nil, fmt.Errorf("%w: index entry must reference a manifest, got %q",
				ErrUnsupportedFormat, e.Descriptor.MediaType)
		}
		if e.Platform == nil {
			return nil, fmt.Errorf("%w: manifests[%d] digest=%s",
				ErrPlatformMissing, i, e.Descriptor.Digest)
		}
		key := e.Platform.Normalize().String()
		if prev, dup := seen[key]; dup {
			return nil, fmt.Errorf("%w: %s (entries %d and %d)",
				ErrDuplicatePlatform, key, prev, i)
		}
		seen[key] = i
		entries = append(entries, wireIndexEntry{Descriptor: e.Descriptor, Platform: e.Platform})
	}
	wire := wireIndex{
		MediaType:   descriptor.MediaTypeImageIndex,
		Manifests:   entries,
		Annotations: idx.Annotations,
	}
	rb, err := json.Marshal(&wire)
	if err != nil {
		return nil, err
	}
	obj, err := decodeObject(rb)
	if err != nil {
		return nil, err
	}
	for _, k := range idx.raw.keys {
		if _, known := obj.vals[k]; !known {
			obj.set(k, idx.raw.vals[k])
		}
	}
	return obj.encode()
}

// DecodeIndex 解码索引字节并校验：平台缺失/重复按规则报错。
func DecodeIndex(raw []byte) (*Index, descriptor.MediaType, error) {
	obj, err := decodeObject(raw)
	if err != nil {
		return nil, "", err
	}
	var wire wireIndex
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, "", err
	}
	if wire.MediaType != "" && wire.MediaType != descriptor.MediaTypeImageIndex {
		return nil, "", fmt.Errorf("%w: mediaType=%q inside index",
			ErrUnsupportedFormat, wire.MediaType)
	}
	if len(wire.Manifests) == 0 {
		return nil, "", fmt.Errorf("%w: index has no manifests", ErrMissingField)
	}
	idx := &Index{Annotations: map[string]string{}, raw: obj}
	if wire.Annotations != nil {
		idx.Annotations = wire.Annotations
	}
	seen := map[string]int{}
	for i, we := range wire.Manifests {
		if err := we.Descriptor.Validate(); err != nil {
			return nil, "", fmt.Errorf("index: manifests[%d]: %w", i, err)
		}
		if we.Platform == nil {
			return nil, "", fmt.Errorf("%w: manifests[%d] digest=%s",
				ErrPlatformMissing, i, we.Descriptor.Digest)
		}
		key := we.Platform.Normalize().String()
		if prev, dup := seen[key]; dup {
			return nil, "", fmt.Errorf("%w: %s (entries %d and %d)",
				ErrDuplicatePlatform, key, prev, i)
		}
		seen[key] = i
		idx.Manifests = append(idx.Manifests, IndexEntry{
			Descriptor: we.Descriptor,
			Platform:   we.Platform,
		})
	}
	return idx, descriptor.MediaTypeImageIndex, nil
}

// SelectPlatform 在索引中为 p 选择唯一清单。
//
// 匹配与歧义规则（确定且可解释）：
//  1. 先收集精确匹配（含变体完全相等）的条目；
//  2. 若无精确匹配，则在 p.Variant 为空（不约束变体）时，收集同 os/arch
//     的条目，若多于一个则为歧义（ErrAmbiguousPlatform），绝不任选；
//  3. 一个都没有返回 ErrPlatformNotFound。
//
// 完全重复平台已在编解码阶段拒绝，这里不再产生 ErrDuplicatePlatform。
func SelectPlatform(idx *Index, p descriptor.Platform) (IndexEntry, error) {
	if idx == nil {
		return IndexEntry{}, fmt.Errorf("%w: nil index", ErrMissingField)
	}
	req := p.Normalize()
	if req.OS == "" || req.Architecture == "" {
		return IndexEntry{}, fmt.Errorf("%w: request %q", ErrPlatformMissing, p)
	}

	var exact []IndexEntry
	var variantWildcard []IndexEntry
	for _, e := range idx.Manifests {
		if e.Platform == nil {
			continue
		}
		t := e.Platform.Normalize()
		if t.OS != req.OS || t.Architecture != req.Architecture {
			continue
		}
		if t.Variant == req.Variant {
			exact = append(exact, e)
			continue
		}
		if req.Variant == "" {
			// 仅当请求不约束变体时，才允许用具体变体条目作为候选。
			variantWildcard = append(variantWildcard, e)
		}
	}

	if len(exact) == 1 {
		return exact[0], nil
	}
	if len(exact) > 1 {
		// 正常不会发生（重复平台已在编解码拒绝）；仍确定性拒绝。
		return IndexEntry{}, fmt.Errorf("%w: %s matches %d entries",
			ErrAmbiguousPlatform, req, len(exact))
	}
	if len(variantWildcard) == 1 {
		return variantWildcard[0], nil
	}
	if len(variantWildcard) > 1 {
		digs := make([]string, len(variantWildcard))
		for i, e := range variantWildcard {
			digs[i] = e.Descriptor.Digest.String()
		}
		sort.Strings(digs)
		return IndexEntry{}, fmt.Errorf("%w: %s matches %d variant entries %v",
			ErrAmbiguousPlatform, req, len(variantWildcard), digs)
	}
	return IndexEntry{}, fmt.Errorf("%w: %s", ErrPlatformNotFound, req)
}

// DigestOf 返回给定字节的规范摘要（统一口径入口）。
func DigestOf(raw []byte) digest.Digest { return digest.FromBytes(raw) }

// DescriptorFor 用给定 mediaType 与规范化字节构造描述符。
func DescriptorFor(mediaType descriptor.MediaType, raw []byte) descriptor.Descriptor {
	return descriptor.Descriptor{
		MediaType: mediaType,
		Digest:    DigestOf(raw),
		Size:      int64(len(raw)),
	}
}

// 占位：保证 time 包在所有构建配置下均被使用（Created 字段已使用，保留无碍）。
var _ = time.Time{}
