// Command oci-pipeline 的子命令实现。
package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/assembler"
	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

func openStack(root string) (*store.Store, *registry.Registry, error) {
	st, err := store.Open(root)
	if err != nil {
		return nil, nil, err
	}
	reg, err := registry.Open(filepath.Join(root, "registry"), st)
	if err != nil {
		return nil, nil, err
	}
	// GC conservatively keeps unreferenced objects when no tags exist;
	// init just ensures the directories exist.
	return st, reg, nil
}

func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	root := rootFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, reg, err := openStack(*root)
	if err != nil {
		return err
	}
	fmt.Printf("initialized  CAS=%s registry=%s\n", st.Root(), reg.Dir())
	return nil
}

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	root := rootFlag(fs)
	contextDir := fs.String("context", "", "构建上下文目录（必填）")
	name := fs.String("name", "", "镜像引用 name:tag（必填）")
	goos := fs.String("os", "linux", "目标 OS")
	goarch := fs.String("arch", "amd64", "目标架构")
	at := fs.String("at", "", "固定 created 时间（RFC3339），保证可重复")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *contextDir == "" || *name == "" {
		return fmt.Errorf("--context and --name are required")
	}
	ref, err := registry.ParseRef(*name)
	if err != nil {
		return err
	}
	var created time.Time
	if *at != "" {
		created, err = time.Parse(time.RFC3339, *at)
		if err != nil {
			return fmt.Errorf("parse --at: %w", err)
		}
	}

	ctx := context.Background()
	st, reg, err := openStack(*root)
	if err != nil {
		return err
	}
	a := assembler.New(st, func() time.Time {
		return fixedClock
	})
	var res *assembler.Result
	if err := reg.Publish(ctx, ref, func() (descriptor.Descriptor, error) {
		r, aerr := a.Assemble(ctx, *contextDir, assembler.Options{
			Architecture: *goarch,
			OS:           *goos,
			Created:      created,
		})
		if aerr != nil {
			return descriptor.Descriptor{}, aerr
		}
		res = r
		return r.ManifestDescriptor, nil
	}); err != nil {
		return err
	}
	fmt.Printf("context: %s\n", *contextDir)
	fmt.Printf("layer:    %s size=%d new-bytes=%d\n",
		res.LayerDescriptors[0].Digest, res.LayerDescriptors[0].Size, res.NewLayerBytes)
	fmt.Printf("config:   %s\n", res.ConfigDescriptor.Digest)
	fmt.Printf("image:    %s\n", res.ManifestDescriptor.Digest)
	fmt.Printf("tagged:   %s -> %s\n", ref, res.ManifestDescriptor.Digest)
	return nil
}

func runPushIndex(args []string) error {
	fs := flag.NewFlagSet("push-index", flag.ContinueOnError)
	root := rootFlag(fs)
	name := fs.String("name", "", "索引引用 name:tag（必填）")
	dgstStr := fs.String("digest", "", "索引对象摘要 sha256:...（必填）")
	sizeFlag := fs.Int64("size", -1, "索引对象字节数（必填）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || *dgstStr == "" || *sizeFlag < 0 {
		return fmt.Errorf("--name, --digest and --size are required")
	}
	ref, err := registry.ParseRef(*name)
	if err != nil {
		return err
	}
	dgst, err := digest.Parse(*dgstStr)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, reg, err := openStack(*root)
	if err != nil {
		return err
	}
	desc := descriptor.Descriptor{
		MediaType: descriptor.MediaTypeImageIndex,
		Digest:    dgst,
		Size:      *sizeFlag,
	}
	if err := reg.PushIndex(ctx, ref, desc); err != nil {
		return err
	}
	fmt.Printf("tagged: %s -> index %s\n", ref, dgst)
	return nil
}

func runResolve(args []string) error {
	fs := flag.NewFlagSet("resolve", flag.ContinueOnError)
	root := rootFlag(fs)
	name := fs.String("name", "", "镜像引用 name:tag（必填）")
	goos := fs.String("os", "", "请求平台 OS（可选）")
	goarch := fs.String("arch", "", "请求平台架构（与 --os 同时给出）")
	variant := fs.String("variant", "", "架构变体（可选）")
	showDigest := fs.Bool("digest", true, "输出清单摘要（默认开启）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" {
		return fmt.Errorf("--name is required")
	}
	ref, err := registry.ParseRef(*name)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, reg, err := openStack(*root)
	if err != nil {
		return err
	}

	if *goos != "" || *goarch != "" {
		if *goos == "" || *goarch == "" {
			return fmt.Errorf("--os and --arch must be given together")
		}
		p := descriptor.Platform{OS: *goos, Architecture: *goarch, Variant: *variant}
		m, mdgst, err := reg.PullForPlatform(ctx, ref, p)
		if err != nil {
			return err
		}
		fmt.Printf("reference: %s\n", ref)
		fmt.Printf("platform:  %s\n", p.Normalize())
		fmt.Printf("selected:  manifest %s version=%d layers=%d\n",
			mdgst, m.Version, len(m.Layers))
		return nil
	}

	rec, err := reg.Resolve(ctx, ref)
	if err != nil {
		return err
	}
	fmt.Printf("reference: %s\n", ref)
	fmt.Printf("kind:      %s\n", rec.Kind)
	if *showDigest {
		fmt.Printf("digest:    %s\n", rec.Descriptor.Digest)
	}
	return nil
}

func runGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ContinueOnError)
	root := rootFlag(fs)
	verbose := fs.Bool("verbose", false, "打印每个删除/保留对象")
	if err := fs.Parse(args); err != nil {
		return err
	}
	ctx := context.Background()
	st, reg, err := openStack(*root)
	if err != nil {
		return err
	}
	col := gc.New(st, reg)
	rep, err := col.Run(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("scanned=%d kept=%d deleted=%d freed-bytes=%d\n",
		rep.Scanned, rep.Kept, len(rep.Deleted), rep.FreedBytes)
	if *verbose {
		for _, d := range rep.Deleted {
			fmt.Printf("deleted %s\n", d)
		}
	}
	return nil
}

// fixedClock 为 build 命令提供固定 created 时间（CLI 口径默认可重复；
// 需要墙钟时间时显式使用 --at 之外的方式由库支持）。
var fixedClock = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

var _ = manifest.Version0
