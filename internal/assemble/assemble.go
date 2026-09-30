// Package assemble 负责从本地构建上下文组装镜像层、配置与清单。
//
// 一致性口径：
//   - 层 tar 字节由 tarball.Build 确定性生成（路径稳定排序、零时间戳、
//     固定 uid/gid），仅取决于文件路径、权限与内容；
//   - 同一构建上下文重复组装得到相同层标识与镜像摘要；
//   - 内容相同的层（即使名称不同）得到相同摘要；已存在的层不重复写入；
//   - 配置与清单均使用 manifest.CanonicalJSON 规范序列化；
//   - config.created 固定省略（不接受易变输入），保证跨机器/跨时刻可复现。
package assemble

import (
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/tarball"
)

// File 构建上下文中进入某一层的文件。
type File = tarball.File

// LayerSpec 描述待组装的一个层。
//
// Name 仅用于日志解释，不参与摘要；Files 决定层 tar 的确定性内容。
type LayerSpec struct {
	Name  string
	Files []File
}

// ImageSpec 一次镜像组装的输入。
type ImageSpec struct {
	// Config 镜像配置的语义内容（如 os/architecture/config 等）。
	// 顶层键按字典序规范序列化；"created" 键被忽略（由系统固定省略），
	// 其余字段原样进入 config 摘要，因此相同输入恒得相同 config 摘要。
	Config map[string]any
	// Layers 按顺序构成镜像的层。
	Layers []LayerSpec
}

// Assembler 在给定 CAS 上组装镜像。
type Assembler struct {
	store *cas.Store
}

// New 创建组装器。
func New(store *cas.Store) *Assembler { return &Assembler{store: store} }

// LayerDecision 单层的组装判定（用于可解释日志）。
type LayerDecision struct {
	Name     string `json:"name"`
	Digest   string `json:"digest"`
	Size     int64  `json:"size"`
	Reused   bool   `json:"reused"`
	InputSHA string `json:"inputSha256"`
}

// Report 记录一次组装中每个对象的输入特征与判定依据。
type Report struct {
	ConfigDigest   string              `json:"configDigest"`
	ManifestDesc   manifest.Descriptor `json:"manifest"`
	LayerDecisions []LayerDecision     `json:"layers"`
}

// AssembleImage 将单个平台镜像写入 CAS，返回清单描述符与组装明细。
//
// 可重复性：对相同 spec 多次调用，返回的层摘要、config 摘要、镜像摘要
// 完全一致；已存在对象全部走去重路径（Report 中 Reused=true）。
func (a *Assembler) AssembleImage(ctx context.Context, spec ImageSpec) (manifest.Descriptor, *Report, error) {
	report := &Report{}
	layerDescs := make([]manifest.Descriptor, 0, len(spec.Layers))

	for i, layer := range spec.Layers {
		tarBytes, err := tarball.Build(layer.Files)
		if err != nil {
			return manifest.Descriptor{}, nil, fmt.Errorf("layer %q (%d): %w", layer.Name, i, err)
		}
		ld := digest.FromBytes(tarBytes)
		inputDigest := digest.FromBytes(flattenInput(layer.Files))

		existed, err := a.store.Has(ld)
		if err != nil {
			return manifest.Descriptor{}, nil, err
		}
		if err := a.store.Put(ctx, ld, bytes.NewReader(tarBytes)); err != nil {
			return manifest.Descriptor{}, nil, fmt.Errorf("write layer %q: %w", layer.Name, err)
		}
		report.LayerDecisions = append(report.LayerDecisions, LayerDecision{
			Name:     layer.Name,
			Digest:   string(ld),
			Size:     int64(len(tarBytes)),
			Reused:   existed,
			InputSHA: string(inputDigest),
		})
		layerDescs = append(layerDescs, manifest.Descriptor{
			MediaType: manifest.MediaTypeLayer,
			Digest:    ld,
			Size:      int64(len(tarBytes)),
		})
	}

	configDoc, err := canonicalConfig(spec.Config)
	if err != nil {
		return manifest.Descriptor{}, nil, err
	}
	configDigest, err := a.store.PutBytes(ctx, configDoc)
	if err != nil {
		return manifest.Descriptor{}, nil, fmt.Errorf("write config: %w", err)
	}
	report.ConfigDigest = string(configDigest)

	m := &manifest.Manifest{
		SchemaVersion: manifest.SchemaVersion,
		Config: manifest.Descriptor{
			MediaType: manifest.MediaTypeConfig,
			Digest:    configDigest,
			Size:      int64(len(configDoc)),
		},
		Layers: layerDescs,
	}
	canonicalManifest, err := manifest.EncodeManifest(m)
	if err != nil {
		return manifest.Descriptor{}, nil, err
	}
	mdesc, err := manifest.PutManifest(ctx, a.store, canonicalManifest)
	if err != nil {
		return manifest.Descriptor{}, nil, err
	}
	report.ManifestDesc = mdesc
	return mdesc, report, nil
}

// canonicalConfig 生成确定性配置字节：忽略 created（固定零值语义），
// 其余 map 键由 encoding/json 按字典序输出，保证可复现。
func canonicalConfig(user map[string]any) ([]byte, error) {
	merged := make(map[string]any, len(user))
	for k, v := range user {
		if k == "created" {
			continue
		}
		merged[k] = v
	}
	return manifest.CanonicalJSON(merged)
}

// flattenInput 将层输入压成确定性字节，仅用于日志中的输入指纹，
// 与层 tar 摘要保持同样的可重复性，但不参与内容寻址。
func flattenInput(files []File) []byte {
	names := make([]string, 0, len(files))
	byName := make(map[string]File, len(files))
	for _, f := range files {
		names = append(names, f.Path)
		byName[f.Path] = f
	}
	sort.Strings(names)
	var buf bytes.Buffer
	for _, n := range names {
		f := byName[n]
		buf.WriteString(n)
		buf.WriteByte(0)
		buf.Write(f.Data)
		buf.WriteByte(0)
	}
	return buf.Bytes()
}
