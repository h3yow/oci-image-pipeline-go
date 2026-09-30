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
	// gcMu 协调“引用发布（提交）”与“整轮回收”：
	//   - 一次提交（BeginCommit）在“组装对象 -> 校验目标闭包 -> 原子
	//     发布引用”的整段区间内持有读锁（同时持 mu 串行化引用变更）；
	//   - gc.Run 持写锁，因此提交与回收互不穿插。
	// 这消除了“对象已写完但引用尚未发布”窗口里对象被回收、
	// 从而让新引用指向缺失对象的可能。纯读取（Resolve/Select/...）
	// 不需要这两把锁，因为对象一经引用即受可达闭包长期保护。
	gcMu sync.RWMutex
}

// BeginGC / EndGC 由回收器在一轮回收前后成对调用：回收期间任何引用发布
// （提交）都会等待本轮结束，提交期间回收也不会开始。
func (r *Registry) BeginGC() { r.gcMu.Lock() }

// EndGC 结束一轮回收的互斥区间。
func (r *Registry) EndGC() { r.gcMu.Unlock() }

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

// Tag 将引用原子指向 desc。
//
// 这是一次“提交”：desc 的整棵闭包（清单/config/层，或索引下全部平台）
// 必须先通过统一完整性校验（VerifyClosure），随后才原子替换引用文件。
// 校验与发布在提交门（BeginCommit：互斥引用变更、且与 gc.Run 互斥）
// 保护下完成；任一步失败则引用保持原状，外部绝不会读到指向缺失/损坏
// 对象的引用。
func (r *Registry) Tag(ctx context.Context, ref string, desc manifest.Descriptor) error {
	release := r.BeginCommit()
	defer release()
	return r.tagTxn(ctx, ref, desc)
}

// BeginCommit 进入一次提交区间并返回释放函数：
//   - 与 gc.Run 整轮互斥（gcMu.RLock vs gcMu.Lock）；
//   - 串行化同一进程内的引用变更（mu），避免并发提交互相穿插。
//
// 需要在“组装对象”之前就进入提交区间的高层编排（commit.Committer）
// 使用它，以消除“对象已写完但引用尚未发布”窗口内被回收的可能。
// 返回的 release 必须且只能调用一次。
func (r *Registry) BeginCommit() (release func()) {
	r.mu.Lock()
	r.gcMu.RLock()
	return func() {
		r.gcMu.RUnlock()
		r.mu.Unlock()
	}
}

// TagInCommit 在调用方已通过 BeginCommit 持有的提交区间内执行 Tag。
// 适用于“先组装、后发布”的单次原子提交编排。
func (r *Registry) TagInCommit(ctx context.Context, ref string, desc manifest.Descriptor) error {
	return r.tagTxn(ctx, ref, desc)
}

// tagTxn 执行“校验闭包 -> 原子发布引用”；调用方必须已进入提交区间
// （Tag 自取，或经 BeginCommit + TagInCommit）。
func (r *Registry) tagTxn(ctx context.Context, ref string, desc manifest.Descriptor) error {
	if err := desc.Validate(true); err != nil {
		return err
	}
	p, err := r.refPath(ref)
	if err != nil {
		return err
	}
	doc, err := json.Marshal(refRecord{FormatVersion: CurrentRefFormatVersion, Target: desc})
	if err != nil {
		return err
	}
	doc = append(doc, '\n')
	if err := r.VerifyClosure(ctx, desc); err != nil {
		return fmt.Errorf("commit %s: %w", ref, err)
	}
	return r.atomicWriteRef(p, doc)
}

// attachLocked 在已进入提交区间的条件下把平台清单并入引用目标索引。
//
// 新索引对象写入 CAS 后、原子替换引用前，必须通过 VerifyClosure 整闭包
// 校验。因此一次 AttachPlatform 与 Tag 一样具备提交原子性：成功则引用
// 指向一棵从头到尾完整的对象树，失败则引用保持追加前的旧索引不变。
func (r *Registry) attachLocked(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
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
	entry := m
	entry.Platform = &p

	var idx *manifest.Index
	if rec, err := r.loadRecord(ref); err == nil {
		if rec.Target.MediaType != manifest.MediaTypeIndex {
			return fmt.Errorf("%w: %s points at %s, not an index",
				ErrWrongKind, ref, rec.Target.MediaType)
		}
		_, data, gerr := manifest.GetManifest(ctx, r.store, rec.Target.Digest)
		if gerr != nil {
			return gerr
		}
		idx, err = manifest.ParseIndex(data)
		if err != nil {
			return err
		}
	} else if errors.Is(err, ErrNoSuchRef) {
		idx = &manifest.Index{SchemaVersion: manifest.SchemaVersion}
	} else {
		return err
	}

	// 平台重复检测：不得任选其一，直接拒绝。
	for _, existing := range idx.Manifests {
		if existing.Platform != nil && existing.Platform.Equal(p) {
			return fmt.Errorf("%w: %s/%s%s already attached",
				ErrDuplicatePlatform, p.OS, p.Architecture, variantOf(p.Variant))
		}
	}
	idx.Manifests = append(idx.Manifests, entry)

	canonical, err := manifest.EncodeIndex(idx)
	if err != nil {
		return err
	}
	desc, err := manifest.PutManifest(ctx, r.store, canonical)
	if err != nil {
		return err
	}
	p2, err := r.refPath(ref)
	if err != nil {
		return err
	}
	doc, err := json.Marshal(refRecord{FormatVersion: CurrentRefFormatVersion, Target: desc})
	if err != nil {
		return err
	}
	// 提交闸门：新索引整闭包（含全部既有平台与新平台）完整后才发布。
	if err := r.VerifyClosure(ctx, desc); err != nil {
		return fmt.Errorf("commit %s: %w", ref, err)
	}
	return r.atomicWriteRef(p2, append(doc, '\n'))
}

// AttachPlatform 将一个平台清单挂到 ref 指向的索引下；
// ref 尚不存在时创建索引。平台完全重复 -> ErrDuplicatePlatform。
// 与 Tag 同为一次原子提交（内部经 BeginCommit 进入提交区间）。
func (r *Registry) AttachPlatform(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
	release := r.BeginCommit()
	defer release()
	return r.attachLocked(ctx, ref, m, p)
}

// AttachPlatformInCommit 在调用方已通过 BeginCommit 持有的提交区间内
// 执行 AttachPlatform，用于“先组装、后追加发布”的单次原子提交编排。
func (r *Registry) AttachPlatformInCommit(ctx context.Context, ref string, m manifest.Descriptor, p manifest.Platform) error {
	return r.attachLocked(ctx, ref, m, p)
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

// walkClosure 校验并遍历某对象的完整可达闭包（索引->清单->config/layers），
// 起始对象本身也计入。完整性判定一律走 store.Inspect 的统一口径，
// 并带上描述符声明的大小，使三类结论（不存在/长度不符/摘要不符）
// 在回收、读取与提交前检查中完全一致；任何引用对象损坏/缺失都作为
// 错误返回，不静默忽略或跳过。
//
// where 标注 d 在引用结构中的位置（如 "index manifests[0]"、"config"），
// 仅用于让错误能对上具体位置。
func (r *Registry) walkClosure(ctx context.Context, where string, d manifest.Descriptor, into map[digest.Digest]struct{}) error {
	if err := d.Validate(true); err != nil {
		return fmt.Errorf("%w: %s descriptor: %v", manifest.ErrMalformed, where, err)
	}
	if _, seen := into[d.Digest]; seen {
		return nil
	}
	if _, err := r.store.Inspect(ctx, d.Digest, d.Size); err != nil {
		return fmt.Errorf("%s %s: %w", where, d.Digest.Short(), err)
	}
	into[d.Digest] = struct{}{}

	// config / 层是闭包叶子：只做存在性/大小/摘要校验，不解析为清单。
	if d.MediaType == manifest.MediaTypeConfig || d.MediaType == manifest.MediaTypeLayer {
		return nil
	}

	mt, data, err := manifest.GetManifest(ctx, r.store, d.Digest)
	if err != nil {
		return fmt.Errorf("%s %s: %w", where, d.Digest.Short(), err)
	}
	// 描述符标识与对象实际种类必须一致，不允许混用。
	if mt != d.MediaType {
		return fmt.Errorf("%w: %s %s declared mediaType %s but content is %s",
			manifest.ErrMalformed, where, d.Digest.Short(), d.MediaType, mt)
	}
	switch mt {
	case manifest.MediaTypeIndex:
		idx, perr := manifest.ParseIndex(data)
		if perr != nil {
			return fmt.Errorf("%s %s: %w", where, d.Digest.Short(), perr)
		}
		for i, child := range idx.Manifests {
			cwhere := fmt.Sprintf("%s manifests[%d]", where, i)
			if err := r.walkClosure(ctx, cwhere, child, into); err != nil {
				return err
			}
		}
	case manifest.MediaTypeManifest:
		m, perr := manifest.ParseManifest(data)
		if perr != nil {
			return fmt.Errorf("%s %s: %w", where, d.Digest.Short(), perr)
		}
		if m.Config.MediaType != manifest.MediaTypeConfig {
			return fmt.Errorf("%w: %s config mediaType %s", manifest.ErrMalformed, where, m.Config.MediaType)
		}
		targets := []struct {
			where string
			d     manifest.Descriptor
		}{
			{where + " config", m.Config},
		}
		for i, l := range m.Layers {
			if l.MediaType != manifest.MediaTypeLayer {
				return fmt.Errorf("%w: %s layers[%d] mediaType %s",
					manifest.ErrMalformed, where, i, l.MediaType)
			}
			targets = append(targets, struct {
				where string
				d     manifest.Descriptor
			}{fmt.Sprintf("%s layers[%d]", where, i), l})
		}
		for _, tgt := range targets {
			if err := r.walkClosure(ctx, tgt.where, tgt.d, into); err != nil {
				return err
			}
		}
	}
	return nil
}

// VerifyClosure 完整校验“把 target 挂到引用上”所需的整棵对象树：
// target 自身、索引选中的每个清单、清单的 config 与每个层都必须存在、
// 长度与声明一致、摘要通过。这是提交动作发布引用前的强制闸门，
// 也是回收可达性判定共用的同一套口径。
//
// target 的 mediaType 必须是清单或索引；描述符自身非法或闭包中任一
// 对象的三类完整性错误都会原样返回（可用 errors.Is 精确归类）。
func (r *Registry) VerifyClosure(ctx context.Context, target manifest.Descriptor) error {
	if target.MediaType != manifest.MediaTypeManifest && target.MediaType != manifest.MediaTypeIndex {
		return fmt.Errorf("%w: reference target must be manifest or index, got %s",
			manifest.ErrUnsupportedFormat, target.MediaType)
	}
	seen := map[digest.Digest]struct{}{}
	return r.walkClosure(ctx, "target", target, seen)
}

// ReferencedBlobs 返回 ref 当前可达的全部对象摘要，供回收判定。
func (r *Registry) ReferencedBlobs(ctx context.Context, ref string) (map[digest.Digest]struct{}, error) {
	rec, err := r.loadRecord(ref)
	if err != nil {
		return nil, err
	}
	out := map[digest.Digest]struct{}{}
	if err := r.walkClosure(ctx, "reference "+ref, rec.Target, out); err != nil {
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
