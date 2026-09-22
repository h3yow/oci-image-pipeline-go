// Package manifest 定义镜像清单与多架构索引的序列化口径与兼容边界。
package manifest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// 兼容范围（稳定，超出即明确拒绝）：
//   - application/vnd.oci.image.manifest.v1+json（单镜像清单）
//   - application/vnd.oci.image.index.v1+json（多架构索引）
//
// 任何其他 mediaType（含 Docker v2 schema1/schema2、未知 OCI 版本）一律
// ErrUnsupportedFormat；schemaVersion 仅接受 2，字段缺失时缺省按 2 解释，
// 出现其他值则拒绝（不猜测）。
const (
	MediaTypeManifest = "application/vnd.oci.image.manifest.v1+json"
	MediaTypeIndex    = "application/vnd.oci.image.index.v1+json"
	// MediaTypeConfig 镜像配置 JSON。
	MediaTypeConfig = "application/vnd.oci.image.config.v1+json"
	// MediaTypeLayer 镜像层 tar。
	MediaTypeLayer = "application/vnd.oci.image.layer.v1.tar"
	// SchemaVersion 是受支持的唯一清单结构版本。
	SchemaVersion = 2
)

// 受支持的描述符 mediaType 集合；集合外的标识一律拒绝，绝不混用。
var supportedMediaTypes = map[string]bool{
	MediaTypeManifest: true,
	MediaTypeIndex:    true,
	MediaTypeConfig:   true,
	MediaTypeLayer:    true,
}

var (
	// ErrUnsupportedFormat 格式超出支持范围（旧版/未知 mediaType 或结构版本）。
	ErrUnsupportedFormat = errors.New("manifest: unsupported format")
	// ErrMalformed JSON 无法解析或结构自相矛盾（如 mediaType 与结构不符）。
	ErrMalformed = errors.New("manifest: malformed document")
)

// Descriptor 内容描述符：摘要 + 大小 + 类型三要素。
// Platform 仅允许出现在索引的 manifests 条目中，清单内描述符不得携带。
type Descriptor struct {
	MediaType string        `json:"mediaType"`
	Digest    digest.Digest `json:"digest"`
	Size      int64         `json:"size"`
	Platform  *Platform     `json:"platform,omitempty"`
}

// Validate 校验描述符三要素合法，且 mediaType 属于受支持集合。
// allowIndex 控制是否允许该位置指向索引（仅索引条目嵌套场景）。
// 调用方还应按位置（config/layers/manifests）核对具体 mediaType。
func (d Descriptor) Validate(allowIndex bool) error {
	if _, err := digest.Parse(string(d.Digest)); err != nil {
		return fmt.Errorf("%w: descriptor digest: %v", cas.ErrInvalidDigest, err)
	}
	if d.Size < 0 {
		return fmt.Errorf("%w: negative size", ErrMalformed)
	}
	if !supportedMediaTypes[d.MediaType] {
		return fmt.Errorf("%w: descriptor mediaType %q", ErrUnsupportedFormat, d.MediaType)
	}
	if d.MediaType == MediaTypeIndex && !allowIndex {
		return fmt.Errorf("%w: index descriptor used where index is not allowed", ErrMalformed)
	}
	return nil
}

// Platform 平台标识：os/arch 必填，variant 可空（空即不参与匹配）。
type Platform struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	Variant      string `json:"variant,omitempty"`
}

func (p Platform) Valid() error {
	if p.OS == "" || p.Architecture == "" {
		return fmt.Errorf("%w: platform requires os and architecture", ErrMalformed)
	}
	return nil
}

// Equal 平台精确相等（含 variant）。
func (p Platform) Equal(o Platform) bool {
	return p == o
}

// Manifest 单镜像清单（OCI v1）。
type Manifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType,omitempty"`
	Config        Descriptor   `json:"config"`
	Layers        []Descriptor `json:"layers"`
}

// Index 多架构索引（OCI v1）。
type Index struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType,omitempty"`
	Manifests     []Descriptor `json:"manifests"`
}

// wireEnvelope 用于从字节探测结构；未知字段一律忽略（前向新增字段的
// 缺省语义稳定：读入忽略，写出不保留，不影响既有内容摘要）。
type wireEnvelope struct {
	SchemaVersion int              `json:"schemaVersion"`
	MediaType     string           `json:"mediaType"`
	Config        *json.RawMessage `json:"config"`
	Layers        *json.RawMessage `json:"layers"`
	Manifests     *json.RawMessage `json:"manifests"`
}

// CanonicalJSON 返回用于摘要计算与去重的规范字节：
//
//   - 紧凑无空白；结构体按固定字段顺序；map 键按字典序；
//   - 仅输出本版本已知字段，未知字段不参与（读入即丢弃）；
//   - 相同语义内容恒得相同字节，从而得到相同摘要。
func CanonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("manifest: canonical encode: %w", err)
	}
	// Encode 追加一个换行；摘要口径必须稳定，显式裁掉。
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// ParseKind 仅依据字节探测其受支持的种类并做结构校验，返回 mediaType。
//
// 缺省语义：schemaVersion 缺失时按 2 解释；mediaType 缺失时按结构推断
// （有 manifests 即索引，有 config/layers 即清单），两者必须一致；
// 无法归类或自相矛盾一律拒绝。
func ParseKind(data []byte) (string, error) {
	var env wireEnvelope
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&env); err != nil {
		return "", fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	// 明确拒绝尾随垃圾，避免把两份文档拼接误当一份。
	if dec.More() {
		return "", fmt.Errorf("%w: trailing data after document", ErrMalformed)
	}

	structural := ""
	switch {
	case env.Manifests != nil:
		structural = MediaTypeIndex
		if env.Config != nil || env.Layers != nil {
			return "", fmt.Errorf("%w: both index and manifest fields present", ErrMalformed)
		}
	case env.Config != nil || env.Layers != nil:
		structural = MediaTypeManifest
	}

	mt := env.MediaType
	if mt == "" {
		mt = structural // 缺省：按结构推断，语义稳定
	}
	if structural != "" && mt != structural {
		return "", fmt.Errorf("%w: mediaType %q does not match document structure",
			ErrUnsupportedFormat, mt)
	}
	switch mt {
	case MediaTypeManifest, MediaTypeIndex:
	default:
		return "", fmt.Errorf("%w: mediaType %q", ErrUnsupportedFormat, mt)
	}

	sv := env.SchemaVersion
	if sv == 0 {
		sv = SchemaVersion // 字段缺失的稳定缺省
	}
	if sv != SchemaVersion {
		return "", fmt.Errorf("%w: schemaVersion %d (only %d supported)",
			ErrUnsupportedFormat, sv, SchemaVersion)
	}
	return mt, nil
}

// EncodeManifest 校验并规范序列化一个清单。
func EncodeManifest(m *Manifest) ([]byte, error) {
	if m.SchemaVersion == 0 {
		m.SchemaVersion = SchemaVersion
	}
	if m.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: schemaVersion %d", ErrUnsupportedFormat, m.SchemaVersion)
	}
	m.MediaType = MediaTypeManifest // 统一口径：由本系统写出的清单必带受支持 mediaType
	if err := m.Config.Validate(false); err != nil {
		return nil, err
	}
	if m.Config.MediaType != MediaTypeConfig {
		return nil, fmt.Errorf("%w: config descriptor mediaType %q", ErrMalformed, m.Config.MediaType)
	}
	if len(m.Layers) == 0 {
		return nil, fmt.Errorf("%w: manifest requires at least one layer", ErrMalformed)
	}
	for i, ld := range m.Layers {
		if err := ld.Validate(false); err != nil {
			return nil, fmt.Errorf("layers[%d]: %w", i, err)
		}
		if ld.MediaType != MediaTypeLayer {
			return nil, fmt.Errorf("%w: layers[%d] mediaType %q", ErrMalformed, i, ld.MediaType)
		}
		if ld.Platform != nil {
			return nil, fmt.Errorf("%w: layer descriptor must not carry platform", ErrMalformed)
		}
	}
	if m.Config.Platform != nil {
		return nil, fmt.Errorf("%w: config descriptor must not carry platform", ErrMalformed)
	}
	return CanonicalJSON(m)
}

// EncodeIndex 校验并规范序列化一个索引。
func EncodeIndex(idx *Index) ([]byte, error) {
	if idx.SchemaVersion == 0 {
		idx.SchemaVersion = SchemaVersion
	}
	if idx.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: schemaVersion %d", ErrUnsupportedFormat, idx.SchemaVersion)
	}
	idx.MediaType = MediaTypeIndex
	seen := make(map[Platform]int, len(idx.Manifests))
	for i, md := range idx.Manifests {
		if err := md.Validate(true); err != nil {
			return nil, fmt.Errorf("manifests[%d]: %w", i, err)
		}
		if md.MediaType != MediaTypeManifest && md.MediaType != MediaTypeIndex {
			return nil, fmt.Errorf("%w: manifests[%d] must reference manifest/index, got %q",
				ErrMalformed, i, md.MediaType)
		}
		if md.Platform == nil {
			return nil, fmt.Errorf("%w: index manifests[%d] requires platform",
				ErrMalformed, i)
		}
		if err := md.Platform.Valid(); err != nil {
			return nil, fmt.Errorf("manifests[%d]: %w", i, err)
		}
		if prev, dup := seen[*md.Platform]; dup {
			return nil, fmt.Errorf("%w: platform %s/%s%s duplicates manifests[%d]",
				ErrDuplicatePlatform, md.Platform.OS, md.Platform.Architecture,
				variantSuffix(md.Platform.Variant), prev)
		}
		seen[*md.Platform] = i
	}
	return CanonicalJSON(idx)
}

func variantSuffix(v string) string {
	if v == "" {
		return ""
	}
	return "/" + v
}

// ErrDuplicatePlatform 索引中出现完全相同的平台条目。
var ErrDuplicatePlatform = errors.New("manifest: duplicate platform in index")

// ParseManifest 解析并校验清单字节。
func ParseManifest(data []byte) (*Manifest, error) {
	mt, err := ParseKind(data)
	if err != nil {
		return nil, err
	}
	if mt != MediaTypeManifest {
		return nil, fmt.Errorf("%w: expected manifest, got %s", ErrUnsupportedFormat, mt)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if m.SchemaVersion == 0 {
		m.SchemaVersion = SchemaVersion
	}
	return &m, nil
}

// ParseIndex 解析并校验索引字节（含平台重复检测）。
func ParseIndex(data []byte) (*Index, error) {
	mt, err := ParseKind(data)
	if err != nil {
		return nil, err
	}
	if mt != MediaTypeIndex {
		return nil, fmt.Errorf("%w: expected index, got %s", ErrUnsupportedFormat, mt)
	}
	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if idx.SchemaVersion == 0 {
		idx.SchemaVersion = SchemaVersion
	}
	seen := map[Platform]bool{}
	for i, md := range idx.Manifests {
		if md.Platform == nil {
			return nil, fmt.Errorf("%w: manifests[%d] missing platform", ErrMalformed, i)
		}
		if seen[*md.Platform] {
			return nil, fmt.Errorf("%w: %s/%s%s",
				ErrDuplicatePlatform, md.Platform.OS, md.Platform.Architecture,
				variantSuffix(md.Platform.Variant))
		}
		seen[*md.Platform] = true
	}
	return &idx, nil
}

// PutManifest 将已是规范形态的字节写入 CAS；写入前复核其确实可被解析为
// 声明的 mediaType，返回描述符。不同格式的标识不会混用：描述符的
// mediaType 取自通过校验的文档。
func PutManifest(ctx context.Context, store *cas.Store, canonical []byte) (Descriptor, error) {
	mt, err := ParseKind(canonical)
	if err != nil {
		return Descriptor{}, err
	}
	d, err := store.PutBytes(ctx, canonical)
	if err != nil {
		return Descriptor{}, err
	}
	return Descriptor{MediaType: mt, Digest: d, Size: int64(len(canonical))}, nil
}

// GetManifest 读取并校验一个清单/索引对象，返回其种类与原始字节。
// 损坏/缺失由 cas 层给出可区分错误；格式不受支持返回 ErrUnsupportedFormat。
func GetManifest(ctx context.Context, store *cas.Store, d digest.Digest) (string, []byte, error) {
	data, err := store.GetBytes(ctx, d)
	if err != nil {
		return "", nil, err
	}
	mt, err := ParseKind(data)
	if err != nil {
		return "", nil, err
	}
	return mt, data, nil
}
