// Package registry 用本地目录模拟镜像仓库：命名引用（name:tag）到
// 清单/索引的可变指针，以及多架构选择与并发协调。
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
)

var (
	// ErrAmbiguous 多架构选择出现多于一个匹配，禁止任选。
	ErrAmbiguous = errors.New("registry: ambiguous platform match")
	// ErrNoMatch 索引中不存在请求平台的清单。
	ErrNoMatch = errors.New("registry: no manifest for platform")
	// ErrDuplicatePlatform 向索引追加的清单与既有项平台完全重复。
	ErrDuplicatePlatform = manifest.ErrDuplicatePlatform
	// ErrNoSuchRef 引用不存在。
	ErrNoSuchRef = errors.New("registry: no such reference")
	// ErrWrongKind 引用目标种类与操作要求不符（如对清单做平台选择）。
	ErrWrongKind = errors.New("registry: reference target kind mismatch")
)

// refFormatVersion 是引用文件的当前结构版本。
//
// 兼容口径（稳定）：
//   - version 缺失：按 1 解释（最早版本无此字段）；
//   - version == 1：本系统结构；
//   - version > CurrentRefFormatVersion：来自更新格式，返回
//     ErrUnsupportedReferenceVersion 明确拒绝，绝不猜测字段含义。
const CurrentRefFormatVersion = 1

// refRecord 是引用文件的磁盘结构（原子替换）。
type refRecord struct {
	FormatVersion int                 `json:"formatVersion,omitempty"`
	Target        manifest.Descriptor `json:"target"`
}

// Registry 本地模拟仓库。
//
// 引用存储布局：
//
//	<root>/refs/<name-path>/<tag>   文件内容为 refRecord（tmp+rename 原子替换）
type Registry struct {
	root  string
	store *cas.Store
	mu    sync.Mutex // 串行化同一进程内对引用的读改写（如 AttachPlatform）
}

// New 打开 root 下的本地仓库。
func New(store *cas.Store) (*Registry, error) {
	root := filepath.Join(store.Root(), "refs")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("registry: init refs: %w", err)
	}
	return &Registry{root: root, store: store}, nil
}

// parseRef 校验并映射 name:tag 到安全的相对文件路径，禁止路径逃逸。
//
// 允许 name 含 "/"（如 ns/app:latest），映射为 refs/ns/app/latest；
// tag 为最后一段，不允许 "/"、":" 与 ".."。
func parseRef(ref string) (string, error) {
	idx := strings.LastIndex(ref, ":")
	if idx <= 0 || idx == len(ref)-1 {
		return "", fmt.Errorf("registry: invalid reference %q (want name:tag)", ref)
	}
	name, tag := ref[:idx], ref[idx+1:]
	if strings.Contains(tag, "/") || strings.Contains(tag, ":") {
		return "", fmt.Errorf("registry: invalid tag in reference %q", ref)
	}
	namePath := filepath.Clean(filepath.FromSlash(name))
	tagPath := filepath.Clean(tag)
	if namePath == "." || namePath == ".." || strings.HasPrefix(namePath, ".."+string(filepath.Separator)) ||
		tagPath == ".." || filepath.IsAbs(namePath) {
		return "", fmt.Errorf("registry: invalid or escaping reference %q", ref)
	}
	return filepath.Join(namePath, tagPath), nil
}

func (r *Registry) refPath(ref string) (string, error) {
	rel, err := parseRef(ref)
	if err != nil {
		return "", err
	}
	return filepath.Join(r.root, rel), nil
}

// RefPathForTest 返回引用文件路径，仅供测试构造存量损坏/旧格式场景。
func (r *Registry) RefPathForTest(ref string) (string, error) { return r.refPath(ref) }

// atomicWriteRef 同目录临时文件 + 原子 rename 发布引用，杜绝半写入可见。
func (r *Registry) atomicWriteRef(p string, doc []byte) (err error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	dir, base := filepath.Dir(p), filepath.Base(p)
	tmp, err := os.CreateTemp(dir, ".ref-"+base+".*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(doc); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		return err
	}
	committed = true
	return nil
}

// loadRecord 读取并校验引用文件，落实版本兼容语义。
func (r *Registry) loadRecord(ref string) (*refRecord, error) {
	p, err := r.refPath(ref)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNoSuchRef, ref)
	}
	if err != nil {
		return nil, err
	}
	var rec refRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		// 损坏的引用文件不伪装成“不存在”。
		return nil, fmt.Errorf("%w: reference %s unreadable: %v", ErrCorruptRef, ref, err)
	}
	switch {
	case rec.FormatVersion == 0:
		rec.FormatVersion = 1 // 缺失字段的稳定缺省：等同 version 1（旧格式引用）
	case rec.FormatVersion == CurrentRefFormatVersion:
	default:
		return nil, fmt.Errorf("%w: reference %s version %d > supported %d",
			ErrUnsupportedReferenceVersion, ref, rec.FormatVersion, CurrentRefFormatVersion)
	}
	if err := rec.Target.Validate(true); err != nil {
		return nil, fmt.Errorf("%w: reference %s target: %v", ErrCorruptRef, ref, err)
	}
	return &rec, nil
}

// ErrCorruptRef 引用文件存在但无法解析/自相矛盾。
var ErrCorruptRef = errors.New("registry: corrupt reference")

// Resolve 返回引用当前指向的对象描述符。
func (r *Registry) Resolve(_ context.Context, ref string) (manifest.Descriptor, error) {
	rec, err := r.loadRecord(ref)
	if err != nil {
		return manifest.Descriptor{}, err
	}
	return rec.Target, nil
}

// Tag 将引用原子指向 desc。发布前对 desc 的整个闭包做完整性校验：
// 成功后引用一定指向“从头到尾完整可用”的镜像；校验不过则引用不动。
// desc 三要素或闭包不合法时返回错误（可用 errors.Is 区分原因）。
func (r *Registry) Tag(ctx context.Context, ref string, desc manifest.Descriptor) error {
	if err := desc.Validate(true); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	releaseGate := r.store.Lock().BeginCommit()
	defer releaseGate()
	return r.publishLocked(ctx, ref, desc)
}

// publishLocked 在已持有 r.mu 的条件下原子发布“ref -> desc”：
//  1. 对 desc 整个可达闭包按描述符声明大小逐个校验（缺失/截断/篡改），
//     校验期间对每个对象先 pin；
//  2. 全部通过才把引用文件同目录临时文件 + rename 原子替换；
//  3. 无论成功失败，结束时释放本函数登记的全部 pin。
//
// 因此“校验通过的对象在引用 rename 之前被并发回收删掉”的窗口不存在：
// GC 只能删除既不可达又未被 pin 的对象，而这些对象在 rename 前由 pin
// 保护、rename 后由新引用的可达性保护。
func (r *Registry) publishLocked(ctx context.Context, ref string, desc manifest.Descriptor) error {
	_, release, err := r.VerifyClosurePinned(ctx, desc.Digest, desc.Size)
	if err != nil {
		return err
	}
	defer release()

	p, err := r.refPath(ref)
	if err != nil {
		return err
	}
	doc, err := json.Marshal(refRecord{FormatVersion: CurrentRefFormatVersion, Target: desc})
	if err != nil {
		return err
	}
	return r.atomicWriteRef(p, append(doc, '\n'))
}

// publishIfBaseLocked 在 publishLocked 基础上增加乐观并发控制：仅当引用
// 当前基线仍等于 base（不存在基线与“引用不存在”一一对应）时才发布；
// 若期间已被另一事务改到别处，返回 ErrCommitConflict，调用方应重新读取、
// 重新组装后重试。这保证并发平台追加不会互相覆盖（lost update）。
func (r *Registry) publishIfBaseLocked(ctx context.Context, ref string, desc manifest.Descriptor, base refBase) error {
	cur, err := r.currentBaseLocked(ref)
	if err != nil {
		return err
	}
	if cur.exists != base.exists || cur.root != base.root {
		curShort := digest.Digest("")
		if cur.exists {
			curShort = cur.root
		}
		return fmt.Errorf("%w: %s changed during commit (base %s, now %s)",
			ErrCommitConflict, ref, base.root.Short(), curShort.Short())
	}
	return r.publishLocked(ctx, ref, desc)
}

// attachLocked 在持锁条件下把“已在存储中”的平台清单并入引用索引并原子
// 条件发布。
//
// 它与 CommitPlatformPlan 的差别仅在于清单对象已提前由调用方写好：新平台
// 闭包在事务内先 pin + 校验，旧闭包由 beginCommit pin，新索引摘要先 pin
// 再落地，发布前比对基线。若调用方无法保证“写入 -> 提交”期间没有并发
// 回收，应改用 CommitPlatformPlan（写前即 pin）。
func (r *Registry) attachLocked(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
	ps, base, err := r.beginCommit(ctx, ref)
	if err != nil {
		return err
	}
	defer ps.release()

	// 新平台闭包 pin + 校验。
	if err := ps.pinClosure(ctx, r, m.Digest, m.Size); err != nil {
		return err
	}

	idx, err := r.loadIndexForAppendLocked(ctx, ref, m, p)
	if err != nil {
		return err
	}
	canonical, err := manifest.EncodeIndex(idx)
	if err != nil {
		return err
	}
	ps.add(r.store.Lock().Pin(digest.FromBytes(canonical)))
	desc, err := manifest.PutManifest(ctx, r.store, canonical)
	if err != nil {
		return err
	}
	return r.publishIfBaseLocked(ctx, ref, desc, base)
}

// loadIndexForAppendLocked 读取 ref 当前索引（不存在则新建），并把平台 m
// 追加进去，返回待编码的索引结构。平台重复 / 目标不是索引在此报错。
func (r *Registry) loadIndexForAppendLocked(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) (*manifest.Index, error) {
	var idx *manifest.Index
	if rec, err := r.loadRecord(ref); err == nil {
		if rec.Target.MediaType != manifest.MediaTypeIndex {
			return nil, fmt.Errorf("%w: %s points at %s, not an index",
				ErrWrongKind, ref, rec.Target.MediaType)
		}
		_, data, gerr := manifest.GetManifest(ctx, r.store, rec.Target.Digest)
		if gerr != nil {
			return nil, gerr
		}
		parsed, perr := manifest.ParseIndex(data)
		if perr != nil {
			return nil, perr
		}
		idx = parsed
	} else if errors.Is(err, ErrNoSuchRef) {
		idx = &manifest.Index{SchemaVersion: manifest.SchemaVersion}
	} else {
		return nil, err
	}
	for _, existing := range idx.Manifests {
		if existing.Platform != nil && existing.Platform.Equal(p) {
			return nil, fmt.Errorf("%w: %s/%s%s already attached",
				ErrDuplicatePlatform, p.OS, p.Architecture, variantOf(p.Variant))
		}
	}
	idx.Manifests = append(idx.Manifests, entryFor(m, p))
	return idx, nil
}

// entryFor 给清单描述符附上平台标注。
func entryFor(m manifest.Descriptor, p manifest.Platform) manifest.Descriptor {
	entry := m
	entry.Platform = &p
	return entry
}

// AttachPlatform 将一个已在存储中的平台清单挂到 ref 指向的索引下；
// ref 尚不存在时创建索引。平台完全重复 -> ErrDuplicatePlatform。
//
// 注意：本方法假定清单对象已写好。存在并发回收时，调用方若无法保证
// “写入 -> 提交”之间对象不被回收，应改用 CommitPlatformPlan。
func (r *Registry) AttachPlatform(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
	if err := m.Validate(true); err != nil {
		return err
	}
	if m.MediaType != manifest.MediaTypeManifest {
		return fmt.Errorf("%w: only manifests can be attached to index, got %s",
			ErrWrongKind, m.MediaType)
	}
	if err := p.Valid(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	releaseGate := r.store.Lock().BeginCommit()
	defer releaseGate()
	if err := r.attachLocked(ctx, ref, m, p); err != nil {
		return classifyCommitError(err)
	}
	return nil
}

// SelectPlatform 在 ref 指向的索引中按平台选择唯一清单描述符：
// 0 个匹配 -> ErrNoMatch；多个匹配 -> ErrAmbiguous；恰好 1 个返回之。
func (r *Registry) SelectPlatform(ctx context.Context, ref string, p manifest.Platform) (manifest.Descriptor, error) {
	if err := p.Valid(); err != nil {
		return manifest.Descriptor{}, err
	}
	r.mu.Lock()
	rec, err := r.loadRecord(ref)
	r.mu.Unlock()
	if err != nil {
		return manifest.Descriptor{}, err
	}
	if rec.Target.MediaType != manifest.MediaTypeIndex {
		return manifest.Descriptor{}, fmt.Errorf("%w: %s is %s",
			ErrWrongKind, ref, rec.Target.MediaType)
	}
	_, data, err := manifest.GetManifest(ctx, r.store, rec.Target.Digest)
	if err != nil {
		return manifest.Descriptor{}, err
	}
	idx, err := manifest.ParseIndex(data)
	if err != nil {
		return manifest.Descriptor{}, err
	}

	var matches []manifest.Descriptor
	for _, d := range idx.Manifests {
		if d.Platform == nil {
			continue
		}
		if d.Platform.OS != p.OS || d.Platform.Architecture != p.Architecture {
			continue
		}
		if p.Variant != "" {
			// 请求显式指定 variant：必须精确相等。
			if d.Platform.Variant == p.Variant {
				matches = append(matches, d)
			}
			continue
		}
		// 请求未指定 variant：同 os/arch 的候选全部视为匹配；
		// 若多于一个（如 arm/v7 与 arm/v8 并存），交由下方判定为歧义，
		// 绝不在多个架构变体间任选。
		matches = append(matches, d)
	}
	switch len(matches) {
	case 0:
		return manifest.Descriptor{}, fmt.Errorf("%w: %s/%s%s in %s",
			ErrNoMatch, p.OS, p.Architecture, variantOf(p.Variant), ref)
	case 1:
		if p.Variant == "" && matches[0].Platform != nil && matches[0].Platform.Variant != "" {
			// 唯一候选带 variant 而请求未指定：结果唯一确定，返回之。
		}
		return matches[0], nil
	default:
		digs := make([]string, len(matches))
		for i, m := range matches {
			digs[i] = string(m.Digest)
		}
		return manifest.Descriptor{}, fmt.Errorf("%w: %s/%s%s matches %d manifests %s",
			ErrAmbiguous, p.OS, p.Architecture, variantOf(p.Variant), len(matches),
			strings.Join(digs, ","))
	}
}

func variantOf(v string) string {
	if v == "" {
		return ""
	}
	return "/" + v
}

// reachableFrom 计算某对象的完整可达闭包（索引->清单->config/layers），
// 起始对象本身也计入。任何引用对象缺失/截断/篡改都按统一三类口径作为错误
// 返回，不静默忽略（校验与提交前检查走同一份 VerifyClosure 判定）。
func (r *Registry) reachableFrom(ctx context.Context, start digest.Digest, declaredSize int64, into map[digest.Digest]struct{}) error {
	objs, err := r.VerifyClosure(ctx, start, declaredSize)
	if err != nil {
		return err
	}
	for _, o := range objs {
		into[o.Digest] = struct{}{}
	}
	return nil
}

// ReferencedBlobs 返回 ref 当前可达的全部对象摘要，供回收判定。
func (r *Registry) ReferencedBlobs(ctx context.Context, ref string) (map[digest.Digest]struct{}, error) {
	rec, err := r.loadRecord(ref)
	if err != nil {
		return nil, err
	}
	out := map[digest.Digest]struct{}{}
	if err := r.reachableFrom(ctx, rec.Target.Digest, rec.Target.Size, out); err != nil {
		return nil, err
	}
	return out, nil
}

// AllRefs 返回仓库中全部引用名（name:tag），用于全量回收。
func (r *Registry) AllRefs(_ context.Context) ([]string, error) {
	var refs []string
	err := filepath.WalkDir(r.root, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".ref-") {
			return nil // 临时文件
		}
		rel, err := filepath.Rel(r.root, p)
		if err != nil {
			return err
		}
		parts := strings.Split(filepath.ToSlash(rel), "/")
		if len(parts) >= 2 {
			name := strings.Join(parts[:len(parts)-1], "/")
			refs = append(refs, name+":"+parts[len(parts)-1])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}
