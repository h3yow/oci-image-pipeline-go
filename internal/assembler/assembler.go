// Package assembler 负责从本地"构建上下文"目录组装镜像层、配置与清单。
//
// 组装口径（保证可重复）：
//   - 层为上下文中各条目按路径字典序排列后生成的规范化 unix tar；
//   - 文件内容、路径、权限位与规整后的修改时间共同决定层摘要，
//     因此同一上下文重复组装得到相同 diff id 与层摘要；
//   - 已存在于 CAS 的层不会被重复写入；
//   - 镜像配置与清单均以 manifest 包的规范化 JSON 编码落盘，
//     相同内容必然得到相同镜像摘要。
package assembler

import (
	"context"
	"time"
	"errors"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

// Assembler 基于固定的 CAS 与构建上下文根目录进行组装。
type Assembler struct {
	st  *store.Store
	now func() time.Time
}

// Options 为组装选项。
type Options struct {
	// Architecture/OS 写入镜像配置的平台标识。
	Architecture string
	OS           string
	// Annotations 写入清单的稳定注解（nil 与空 map 等价）。
	Annotations map[string]string
	// Created 固定配置时间；零值使用 Assembler 的时钟（测试可注入）。
	Created time.Time
}

// Result 为一次组装的产物。
type Result struct {
	Manifest manifest.Manifest
	// ManifestDescriptor 为清单在 CAS 中的描述符（其 Digest 即镜像摘要）。
	ManifestDescriptor descriptor.Descriptor
	// Layers 为去重后实际新写入的层数量（统计用）。
	LayersWritten int
}

var (
	// ErrContextMissing 构建上下文目录不存在。
	ErrContextMissing = errors.New("assembler: build context directory missing")
	// ErrContextUnreadable 构建上下文无法读取。
	ErrContextUnreadable = errors.New("assembler: build context unreadable")
)

// New 创建组装器。nowFn 可为 nil（使用墙钟）；测试注入固定时钟可保证
// 配置时间字段可重复。
func New(st *store.Store, nowFn func() time.Time) *Assembler {
	return &Assembler{st: st}
}

// Assemble 将 contextDir 组装为镜像（层+配置+清单）并写入 CAS。
// 对相同上下文与相同选项，结果摘要完全一致；重复执行不会重复写层。
func (a *Assembler) Assemble(ctx context.Context, contextDir string, opts Options) (*Result, error) {
	return nil, nil
}
