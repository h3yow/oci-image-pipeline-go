// Package registry 用本地目录模拟镜像仓库：维护"名称:标签"到内容描述符的
// 引用（tag/index），支持 v1 清单与多架构索引，并以明确策略兼容旧版 v0 引用。
//
// 引用语义：
//   - tag 指向一个清单或索引描述符；引用文件原子写入，绝不半写可见；
//   - v0 引用：仅允许解析/拉取（透明读取旧格式），Push 新 v0 被拒绝；
//   - 一个名称:标签的解析结果唯一；多架构通过索引描述符表达，
//     平台选择复用 manifest.SelectPlatform，仓库层绝不"任选一个"；
//   - 标签记录带 kind 字段（manifest/index），清单与索引标识不得混用。
package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// Registry 为本地目录模拟的镜像仓库。
type Registry struct {
	dir string
	st  *store.Store

	mu sync.RWMutex
}

// Ref 为一个镜像引用 name:tag（tag 缺省为 latest）。
type Ref struct {
	Name string
	Tag  string
}

// TagRecord 为标签的持久化记录。
type TagRecord struct {
	Descriptor descriptor.Descriptor `json:"descriptor"`
	// Kind 为 "manifest" 或 "index"，防止清单/索引标识混用。
	Kind RecordKind `json:"kind"`
}

// RecordKind 标识标签指向对象的类别。
type RecordKind string

const (
	// KindManifest 标签指向单个清单。
	KindManifest RecordKind = "manifest"
	// KindIndex 标签指向多架构索引。
	KindIndex RecordKind = "index"

	defaultTag = "latest"
)

var (
	// ErrInvalidRef 引用语法非法。
	ErrInvalidRef = errors.New("registry: invalid image reference")
	// ErrUnknownTag 标签不存在。
	ErrUnknownTag = errors.New("registry: tag not found")
	// ErrKindMismatch 标签记录类型与请求类型不一致（清单/索引混用）。
	ErrKindMismatch = errors.New("registry: referenced object kind mismatch")
	// ErrLegacyRejected 旧格式只允许读，不允许新写入。
	ErrLegacyRejected = errors.New("registry: legacy v0 manifest is read-only")
	// ErrBadRecord 标签记录自身字段非法或内容已损坏。
	ErrBadRecord = errors.New("registry: malformed tag record")
)

// ParseRef 解析 "name" / "name:tag" 形式引用。
// name 允许小写字母、数字、'.'、'-'、'/'（段不得以符号首尾）；
// tag 允许字母数字及 ".-_"，长度 1..128；空 tag 缺省为 latest。
func ParseRef(s string) (Ref, error) {
	if s == "" {
		return Ref{}, fmt.Errorf("%w: empty", ErrInvalidRef)
	}
	name, tag, found := strings.Cut(s, ":")
	if !found {
		tag = defaultTag
	}
	if err := validateName(name); err != nil {
		return Ref{}, err
	}
	if err := validateTag(tag); err != nil {
		return Ref{}, err
	}
	return Ref{Name: name, Tag: tag}, nil
}

func validateName(name string) error {
	if name == "" || len(name) > 256 {
		return fmt.Errorf("%w: name %q empty or too long", ErrInvalidRef, name)
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == "" {
			return fmt.Errorf("%w: empty segment in %q", ErrInvalidRef, name)
		}
		if strings.HasPrefix(seg, ".") || strings.HasPrefix(seg, "-") ||
			strings.HasSuffix(seg, ".") || strings.HasSuffix(seg, "-") {
			return fmt.Errorf("%w: segment %q in %q has invalid boundary", ErrInvalidRef, seg, name)
		}
		for _, c := range seg {
			ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-'
			if !ok {
				return fmt.Errorf("%w: illegal character %q in name %q", ErrInvalidRef, c, name)
			}
		}
	}
	return nil
}

func validateTag(tag string) error {
	if tag == "" || len(tag) > 128 {
		return fmt.Errorf("%w: tag %q empty or too long", ErrInvalidRef, tag)
	}
	for _, c := range tag {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '-' || c == '_'
		if !ok {
			return fmt.Errorf("%w: illegal character %q in tag %q", ErrInvalidRef, c, tag)
		}
	}
	return nil
}

// String 返回规范引用字符串。
func (r Ref) String() string {
	if r.Tag == "" {
		return r.Name + ":" + defaultTag
	}
	return r.Name + ":" + r.Tag
}

// Open 打开 dir 下的本地仓库（与 CAS st 关联）。
func Open(dir string, st *store.Store) (*Registry, error) {
	if st == nil {
		return nil, errors.New("registry: nil store")
	}
	if err := os.MkdirAll(filepath.Join(dir, "tags"), 0o755); err != nil {
		return nil, fmt.Errorf("registry: init %s: %w", dir, err)
	}
	return &Registry{dir: dir, st: st}, nil
}

// Dir 返回仓库目录。
func (r *Registry) Dir() string { return r.dir }

func (r *Registry) tagPath(ref Ref) string {
	// name 段已由 ParseRef 校验，仅含安全字符，可直接作为路径。
	return filepath.Join(r.dir, "tags", filepath.FromSlash(ref.Name), ref.Tag+".json")
}

// PushManifest 将标签指向清单描述符。
// desc.MediaType 为旧版 v0 时拒绝写入（ErrLegacyRejected）；
// 非清单 mediaType 一律拒绝，防止标识混用。
func (r *Registry) PushManifest(ctx context.Context, ref Ref, desc descriptor.Descriptor) error {
	if desc.MediaType == descriptor.MediaTypeLegacyManifest {
		return fmt.Errorf("%w: digest=%s", ErrLegacyRejected, desc.Digest)
	}
	if desc.MediaType != descriptor.MediaTypeImageManifest {
		return fmt.Errorf("%w: push manifest with mediaType=%q", ErrKindMismatch, desc.MediaType)
	}
	if err := desc.Validate(); err != nil {
		return err
	}
	// 指向的对象必须真实存在且通过完整性检查。
	if err := r.checkObject(ctx, desc); err != nil {
		return err
	}
	return r.writeRecord(ref, TagRecord{Descriptor: desc, Kind: KindManifest})
}

// PushIndex 将标签指向索引描述符。
func (r *Registry) PushIndex(ctx context.Context, ref Ref, desc descriptor.Descriptor) error {
	if desc.MediaType != descriptor.MediaTypeImageIndex {
		return fmt.Errorf("%w: push index with mediaType=%q", ErrKindMismatch, desc.MediaType)
	}
	if err := desc.Validate(); err != nil {
		return err
	}
	if err := r.checkObject(ctx, desc); err != nil {
		return err
	}
	return r.writeRecord(ref, TagRecord{Descriptor: desc, Kind: KindIndex})
}

// checkObject 从 CAS 读取对象：存在性、大小、摘要在此一次性区分校验。
func (r *Registry) checkObject(ctx context.Context, desc descriptor.Descriptor) error {
	rc, size, err := r.st.Get(ctx, desc.Digest)
	if err != nil {
		return err // ErrNotFound / ErrDigestMismatch / ErrSizeMismatch 原样透出
	}
	defer rc.Close()
	if size != desc.Size {
		return fmt.Errorf("%w: descriptor size %d != stored size %d",
			store.ErrSizeMismatch, desc.Size, size)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(rc); err != nil {
		return err
	}
	return nil
}

// Resolve 解析标签，返回其记录（manifest/index）。
func (r *Registry) Resolve(ctx context.Context, ref Ref) (*TagRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.readRecord(ref)
}

func (r *Registry) readRecord(ref Ref) (*TagRecord, error) {
	raw, err := os.ReadFile(r.tagPath(ref))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrUnknownTag, ref)
		}
		return nil, err
	}
	var rec TagRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrBadRecord, ref, err)
	}
	if rec.Kind != KindManifest && rec.Kind != KindIndex {
		return nil, fmt.Errorf("%w: %s: unknown kind %q", ErrBadRecord, ref, rec.Kind)
	}
	if err := rec.Descriptor.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrBadRecord, ref, err)
	}
	return &rec, nil
}

// writeRecord 在锁内规范化编码并原子替换标签文件。
func (r *Registry) writeRecord(ref Ref, rec TagRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	path := r.tagPath(ref)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tag-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// PullManifest 解析 manifest 类标签并从 CAS 解码其清单。
// 标签实际指向索引时报 ErrKindMismatch；旧版 v0 引用可透明读出。
func (r *Registry) PullManifest(ctx context.Context, ref Ref) (*manifest.Manifest, *TagRecord, error) {
	rec, err := r.Resolve(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if rec.Kind != KindManifest {
		return nil, rec, fmt.Errorf("%w: %s is %s, want manifest",
			ErrKindMismatch, ref, rec.Kind)
	}
	m, err := r.loadManifest(ctx, rec.Descriptor)
	if err != nil {
		return nil, rec, err
	}
	return m, rec, nil
}

func (r *Registry) loadManifest(ctx context.Context, desc descriptor.Descriptor) (*manifest.Manifest, error) {
	rc, size, err := r.st.Get(ctx, desc.Digest)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	if size != desc.Size {
		return nil, fmt.Errorf("%w: descriptor=%d stored=%d",
			store.ErrSizeMismatch, desc.Size, size)
	}
	raw := new(bytes.Buffer)
	if _, err := raw.ReadFrom(rc); err != nil {
		return nil, err
	}
	m, err := manifest.Decode(desc.MediaType, raw.Bytes())
	if err != nil {
		return nil, err
	}
	// 重新计算摘要必须与描述符一致（CAS Get 已校验内容；这里复核引用口径）。
	if recomputed := manifest.DigestOf(raw.Bytes()); !recomputed.Equal(desc.Digest) {
		return nil, fmt.Errorf("%w: %s recomputes to %s",
			store.ErrDigestMismatch, desc.Digest, recomputed)
	}
	return m, nil
}

// PullIndex 解析 index 类标签并解码索引。
func (r *Registry) PullIndex(ctx context.Context, ref Ref) (*manifest.Index, *TagRecord, error) {
	rec, err := r.Resolve(ctx, ref)
	if err != nil {
		return nil, nil, err
	}
	if rec.Kind != KindIndex {
		return nil, rec, fmt.Errorf("%w: %s is %s, want index",
			ErrKindMismatch, ref, rec.Kind)
	}
	rc, size, err := r.st.Get(ctx, rec.Descriptor.Digest)
	if err != nil {
		return nil, rec, err
	}
	defer rc.Close()
	if size != rec.Descriptor.Size {
		return nil, rec, fmt.Errorf("%w: descriptor=%d stored=%d",
			store.ErrSizeMismatch, rec.Descriptor.Size, size)
	}
	raw := new(bytes.Buffer)
	if _, err := raw.ReadFrom(rc); err != nil {
		return nil, rec, err
	}
	idx, mt, err := manifest.DecodeIndex(raw.Bytes())
	if err != nil {
		return nil, rec, err
	}
	_ = mt
	if recomputed := manifest.DigestOf(raw.Bytes()); !recomputed.Equal(rec.Descriptor.Digest) {
		return nil, rec, fmt.Errorf("%w: %s recomputes to %s",
			store.ErrDigestMismatch, rec.Descriptor.Digest, recomputed)
	}
	return idx, rec, nil
}

// PullForPlatform 解析引用并按平台选出唯一清单：
//   - 标签指向索引：执行 manifest.SelectPlatform（缺失/歧义原样报错）；
//   - 标签指向单清单：要求其配置平台与 p 相等，否则 ErrPlatformNotFound；
//   - 标签为旧版 v0：无配置平台信息，按平台拉取明确拒绝（避免任选）。
//
// 返回清单正文及其在 CAS 中的摘要。
func (r *Registry) PullForPlatform(ctx context.Context, ref Ref, p descriptor.Platform) (*manifest.Manifest, digest.Digest, error) {
	rec, err := r.Resolve(ctx, ref)
	if err != nil {
		return nil, digest.Digest{}, err
	}
	if rec.Kind == KindIndex {
		idx, _, err := r.PullIndex(ctx, ref)
		if err != nil {
			return nil, digest.Digest{}, err
		}
		entry, err := manifest.SelectPlatform(idx, p)
		if err != nil {
			return nil, digest.Digest{}, err
		}
		m, err := r.loadManifest(ctx, entry.Descriptor)
		if err != nil {
			return nil, digest.Digest{}, err
		}
		return m, entry.Descriptor.Digest, nil
	}

	m, _, err := r.PullManifest(ctx, ref)
	if err != nil {
		return nil, digest.Digest{}, err
	}
	if m.Version == manifest.Version0 {
		return nil, digest.Digest{}, fmt.Errorf(
			"%w: legacy v0 tag %s carries no platform metadata; resolve by digest instead",
			manifest.ErrPlatformMissing, ref)
	}
	cfg, err := r.loadConfig(ctx, m)
	if err != nil {
		return nil, digest.Digest{}, err
	}
	have := descriptor.Platform{OS: cfg.OS, Architecture: cfg.Architecture}
	if !p.Match(have) {
		return nil, digest.Digest{}, fmt.Errorf("%w: want %s, tag %s is %s",
			manifest.ErrPlatformNotFound, p.Normalize(), ref, have)
	}
	return m, rec.Descriptor.Digest, nil
}

func (r *Registry) loadConfig(ctx context.Context, m *manifest.Manifest) (*manifest.ImageConfig, error) {
	if m.Config == nil {
		return nil, fmt.Errorf("%w: manifest without config", manifest.ErrMissingField)
	}
	rc, size, err := r.st.Get(ctx, m.Config.Digest)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	if size != m.Config.Size {
		return nil, fmt.Errorf("%w: config descriptor=%d stored=%d",
			store.ErrSizeMismatch, m.Config.Size, size)
	}
	raw := new(bytes.Buffer)
	if _, err := raw.ReadFrom(rc); err != nil {
		return nil, err
	}
	return manifest.DecodeConfig(raw.Bytes())
}

// Tags 列出某名称下全部标签（排序后返回）。
func (r *Registry) Tags(ctx context.Context, name string) ([]string, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}
	dir := filepath.Join(r.dir, "tags", filepath.FromSlash(name))
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: name %s", ErrUnknownTag, name)
		}
		return nil, err
	}
	var tags []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		tags = append(tags, strings.TrimSuffix(e.Name(), ".json"))
	}
	sortStrings(tags)
	return tags, nil
}

// AllRecords 枚举仓库内全部标签记录（供 gc 建立引用集）。
func (r *Registry) AllRecords(ctx context.Context) ([]TagRecord, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	root := filepath.Join(r.dir, "tags")
	var recs []TagRecord
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}
		ref, perr := refFromPath(root, path)
		if perr != nil {
			return nil // 非本仓库规范文件，跳过
		}
		rec, rerr := r.readRecord(ref)
		if rerr != nil {
			return rerr
		}
		recs = append(recs, *rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return recs, nil
}

// refFromPath 由标签文件路径还原引用（失败说明文件不符合命名约定）。
func refFromPath(root, path string) (Ref, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return Ref{}, err
	}
	rel = filepath.ToSlash(rel)
	if !strings.HasSuffix(rel, ".json") {
		return Ref{}, fmt.Errorf("not a tag file: %s", rel)
	}
	rel = strings.TrimSuffix(rel, ".json")
	idx := strings.LastIndex(rel, "/")
	var name, tag string
	if idx < 0 {
		return Ref{}, fmt.Errorf("tag without name: %s", rel)
	}
	name, tag = rel[:idx], rel[idx+1:]
	return Ref{Name: name, Tag: tag}, nil
}
