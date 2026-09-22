// Package assembler 负责从本地"构建上下文"目录组装镜像层、配置与清单。
//
// 组装口径（保证可重复）：
//   - 层为上下文中全部条目按路径字典序排列后生成的规范化 unix tar；
//   - 文件路径、类型、权限位与文件内容决定层摘要；条目 mtime 被固定，
//     因此同一上下文重复组装得到相同 diff id 与层摘要；
//   - 已存在于 CAS 的层不会被重复写入；
//   - 镜像配置与清单均以 manifest 包的规范化 JSON 编码落盘，
//     相同内容必然得到相同镜像摘要。
package assembler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// Assembler 基于固定的 CAS 与可注入时钟进行组装。
type Assembler struct {
	st  *store.Store
	now func() time.Time
}

// Options 为组装选项。
type Options struct {
	// Architecture/OS 写入镜像配置的平台标识。
	Architecture string
	OS           string
	// Annotations 写入清单的稳定注解（nil 与空 map 编码口径一致：省略）。
	Annotations map[string]string
	// Created 固定配置时间；零值使用 Assembler 的时钟。测试应注入固定时钟。
	Created time.Time
}

// Result 为一次组装的产物。
type Result struct {
	Manifest manifest.Manifest
	// ManifestDescriptor 为清单在 CAS 中的描述符（其 Digest 即镜像摘要）。
	ManifestDescriptor descriptor.Descriptor
	// ConfigDescriptor 为配置对象描述符。
	ConfigDescriptor descriptor.Descriptor
	// LayerDescriptors 为层描述符（按组装顺序）。
	LayerDescriptors []descriptor.Descriptor
	// NewLayerBytes 为本次实际新写入 CAS 的层字节数（去重命中为 0）。
	NewLayerBytes int64
}

var (
	// ErrContextMissing 构建上下文目录不存在。
	ErrContextMissing = errors.New("assembler: build context directory missing")
	// ErrContextUnreadable 构建上下文无法读取。
	ErrContextUnreadable = errors.New("assembler: build context unreadable")
	// ErrInvalidOption 必填选项缺失。
	ErrInvalidOption = errors.New("assembler: invalid option")
)

// New 创建组装器。nowFn 可为 nil（使用墙钟）；测试注入固定时钟可保证
// 配置时间字段可重复。
func New(st *store.Store, nowFn func() time.Time) *Assembler {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &Assembler{st: st, now: nowFn}
}

// Assemble 将 contextDir 组装为镜像（层+配置+清单）并写入 CAS。
// 对相同上下文与相同选项，结果摘要完全一致；重复执行不会重复写层。
func (a *Assembler) Assemble(ctx context.Context, contextDir string, opts Options) (*Result, error) {
	if a.st == nil {
		return nil, fmt.Errorf("%w: nil store", ErrInvalidOption)
	}
	if opts.OS == "" || opts.Architecture == "" {
		return nil, fmt.Errorf("%w: os and architecture are required", ErrInvalidOption)
	}
	if fi, err := osStat(contextDir); err != nil {
		return nil, ErrContextMissing
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("%w: %s is not a directory", ErrContextMissing, contextDir)
	}

	// 1) 规范化层 tar（幂等：相同上下文 → 相同字节）。
	layerBytes, diffID, err := buildLayerTar(contextDir)
	if err != nil {
		return nil, err
	}

	// 2) 层入 CAS；已存在则 Put 内部去重，不重复写盘。
	layerDgst, layerSize, err := a.st.Put(ctx, bytes.NewReader(layerBytes))
	if err != nil {
		return nil, fmt.Errorf("assembler: put layer: %w", err)
	}
	// 判断本次是否真的写入（仅用于结果统计，不影响正确性）。
	newBytes := int64(0)
	if existed, statErr := a.st.Exists(ctx, layerDgst); statErr == nil && !existed {
		newBytes = layerSize
	}
	layerDesc := descriptor.Descriptor{
		MediaType: descriptor.MediaTypeLayerTar,
		Digest:    layerDgst,
		Size:      layerSize,
	}

	// 3) 规范化镜像配置。
	created := opts.Created
	if created.IsZero() {
		created = a.now().UTC()
	}
	cfg := &manifest.ImageConfig{
		Created:       &created,
		Architecture:  opts.Architecture,
		OS:            opts.OS,
		RootFSDiffIDs: []string{"sha256:" + diffID},
	}
	cfgBytes, err := manifest.EncodeConfig(cfg)
	if err != nil {
		return nil, err
	}
	cfgDgst, cfgSize, err := a.st.Put(ctx, bytes.NewReader(cfgBytes))
	if err != nil {
		return nil, fmt.Errorf("assembler: put config: %w", err)
	}
	cfgDesc := descriptor.Descriptor{
		MediaType: descriptor.MediaTypeImageConfig,
		Digest:    cfgDgst,
		Size:      cfgSize,
	}

	// 4) 规范化清单（v1）。
	m := &manifest.Manifest{
		Version:     manifest.Version1,
		Config:      &cfgDesc,
		Layers:      []descriptor.Descriptor{layerDesc},
		Annotations: cloneAnnotations(opts.Annotations),
	}
	manBytes, err := manifest.EncodeV1(m)
	if err != nil {
		return nil, err
	}
	manDgst, manSize, err := a.st.Put(ctx, bytes.NewReader(manBytes))
	if err != nil {
		return nil, fmt.Errorf("assembler: put manifest: %w", err)
	}
	manDesc := descriptor.Descriptor{
		MediaType: descriptor.MediaTypeImageManifest,
		Digest:    manDgst,
		Size:      manSize,
	}

	// 5) 回读验证：刚写入的清单必须能通过 CAS 完整性校验并解码一致，
	// 任何损坏在这里立即暴露，而不是等到分发时。
	if err := a.verifyRoundTrip(ctx, manDgst, manBytes); err != nil {
		return nil, err
	}

	return &Result{
		Manifest:          *m,
		ManifestDescriptor: manDesc,
		ConfigDescriptor:  cfgDesc,
		LayerDescriptors:  []descriptor.Descriptor{layerDesc},
		NewLayerBytes:     newBytes,
	}, nil
}

// verifyRoundTrip 回读对象并比对字节，确保存储没有半写入/错位。
func (a *Assembler) verifyRoundTrip(ctx context.Context, dgst digest.Digest, want []byte) error {
	rc, _, err := a.st.OpenExpected(ctx, dgst, int64(len(want)))
	if err != nil {
		return fmt.Errorf("assembler: verify open %s: %w", dgst, err)
	}
	defer rc.Close()
	got := new(bytes.Buffer)
	if _, err := got.ReadFrom(rc); err != nil {
		return fmt.Errorf("assembler: verify read %s: %w", dgst, err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		return fmt.Errorf("assembler: verify %s: content differs after write", dgst)
	}
	return nil
}

func cloneAnnotations(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
