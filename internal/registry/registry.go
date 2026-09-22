// Package registry 用本地目录模拟镜像仓库：维护"名称:标签"到内容描述符的
// 引用（tag/index），支持 v1 清单与多架构索引，并以明确策略兼容旧版 v0 引用。
//
// 引用语义：
//   - tag 指向一个清单或索引描述符；引用文件原子写入，绝不半写可见；
//   - v0 引用：仅允许解析/拉取（透明读取旧格式），Push 新 v0 被拒绝；
//   - 一个名称的标签解析结果唯一；多架构通过索引描述符表达，
//     平台选择复用 manifest.SelectPlatform，不允许仓库层"任选一个"。
package registry

import (
	"context"
	"errors"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// Registry 为本地目录模拟的镜像仓库。
type Registry struct {
	dir string
	st  *store.Store
}

// Ref 为一个镜像引用 name:tag（tag 缺省为 latest）。
type Ref struct {
	Name string
	Tag  string
}

// ParseRef 解析 "name" / "name:tag" 形式引用。
func ParseRef(s string) (Ref, error) { return Ref{}, nil }

// String 返回规范引用字符串。
func (r Ref) String() string { return "" }

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
)

// Open 打开 dir 下的本地仓库（与 CAS st 关联）。
func Open(dir string, st *store.Store) (*Registry, error) {
	return &Registry{dir: dir, st: st}, nil
}

// PushManifest 将标签指向清单描述符。
// 若 desc 声明为旧版 mediaType 则拒绝（ErrLegacyRejected）。
func (r *Registry) PushManifest(ctx context.Context, ref Ref, desc descriptor.Descriptor) error {
	return nil
}

// PushIndex 将标签指向索引描述符。
func (r *Registry) PushIndex(ctx context.Context, ref Ref, desc descriptor.Descriptor) error {
	return nil
}

// Resolve 解析标签，返回其记录（manifest/index）。
func (r *Registry) Resolve(ctx context.Context, ref Ref) (*TagRecord, error) {
	return nil, nil
}

// PullManifest 解析 manifest 类标签并从 CAS 解码其清单；
// 标签实际指向索引或类别不符时报 ErrKindMismatch。
// 旧版 v0 引用可被读出（返回的清单 Version==Version0）。
func (r *Registry) PullManifest(ctx context.Context, ref Ref) (*manifest.Manifest, *TagRecord, error) {
	return nil, nil, nil
}

// PullIndex 解析 index 类标签并解码索引。
func (r *Registry) PullIndex(ctx context.Context, ref Ref) (*manifest.Index, *TagRecord, error) {
	return nil, nil, nil
}

// PullForPlatform 解析引用并按平台选出唯一清单：
// 标签指向索引时做平台选择；指向单清单时要求其配置平台与 p 相等。
func (r *Registry) PullForPlatform(ctx context.Context, ref Ref, p descriptor.Platform) (*manifest.Manifest, digest.Digest, error) {
	return nil, digest.Digest{}, nil
}

// Tags 列出某名称下全部标签。
func (r *Registry) Tags(ctx context.Context, name string) ([]string, error) { return nil, nil }

// AllRecords 枚举仓库内全部标签记录（供 gc 建立引用集）。
func (r *Registry) AllRecords(ctx context.Context) ([]TagRecord, error) { return nil, nil }
