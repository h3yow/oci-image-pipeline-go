// Command pipeline 提供本地验证用的最小命令行：
//
//	pipeline init    <dir>                                    初始化仓库目录
//	pipeline build   <dir> <name:tag> <ctxdir> [--os --arch]  组装单平台镜像并打标
//	pipeline attach  <dir> <name:tag> <ctxdir> [--os --arch --variant]  追加平台到索引
//	pipeline select  <dir> <name:tag> [--os --arch --variant]           选择平台清单
//	pipeline resolve <dir> <name:tag>                         查看引用目标
//	pipeline gc      <dir>                                    执行回收并打印判定明细
//	pipeline verify  <dir>                                    全量校验存储完整性
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: pipeline <init|build|attach|select|resolve|gc|verify> ...")
	}
	switch args[0] {
	case "init":
		return cmdInit(args[1:])
	case "build":
		return cmdBuild(args[1:])
	case "attach":
		return cmdAttach(args[1:])
	case "select":
		return cmdSelect(args[1:])
	case "resolve":
		return cmdResolve(args[1:])
	case "gc":
		return cmdGC(args[1:])
	case "verify":
		return cmdVerify(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func openAll(dir string) (*cas.Store, *registry.Registry, error) {
	store, err := cas.NewStore(dir)
	if err != nil {
		return nil, nil, err
	}
	reg, err := registry.New(store)
	if err != nil {
		return nil, nil, err
	}
	return store, reg, nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func cmdInit(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: pipeline init <dir>")
	}
	_, _, err := openAll(args[0])
	return err
}

// loadContext 把本地目录读成构建上下文文件集（确定性排序由 tarball 保证）。
func loadContext(dir string) ([]assemble.File, error) {
	var files []assemble.File
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("context %s: only regular files supported", p)
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files = append(files, assemble.File{
			Path: filepath.ToSlash(rel),
			Data: data,
			Mode: int64(info.Mode().Perm()),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func cmdBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	osName := fs.String("os", "linux", "target OS")
	arch := fs.String("arch", "amd64", "target architecture")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return errors.New("usage: pipeline build <dir> <name:tag> <ctxdir> [--os --arch]")
	}
	dir, ref, ctxDir := pos[0], pos[1], pos[2]

	store, reg, err := openAll(dir)
	if err != nil {
		return err
	}
	files, err := loadContext(ctxDir)
	if err != nil {
		return err
	}
	spec := assemble.ImageSpec{
		Config: map[string]any{"os": *osName, "architecture": *arch},
		Layers: []assemble.LayerSpec{{Name: "context", Files: files}},
	}
	desc, report, err := assemble.New(store).AssembleImage(context.Background(), spec)
	if err != nil {
		return err
	}
	if err := reg.Tag(context.Background(), ref, desc); err != nil {
		return err
	}
	return printJSON(report)
}

func cmdAttach(args []string) error {
	fs := flag.NewFlagSet("attach", flag.ContinueOnError)
	osName := fs.String("os", "linux", "target OS")
	arch := fs.String("arch", "amd64", "target architecture")
	variant := fs.String("variant", "", "target variant")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return errors.New("usage: pipeline attach <dir> <name:tag> <ctxdir> [--os --arch --variant]")
	}
	dir, ref, ctxDir := pos[0], pos[1], pos[2]

	store, reg, err := openAll(dir)
	if err != nil {
		return err
	}
	files, err := loadContext(ctxDir)
	if err != nil {
		return err
	}
	spec := assemble.ImageSpec{
		Config: map[string]any{"os": *osName, "architecture": *arch},
		Layers: []assemble.LayerSpec{{Name: "context", Files: files}},
	}
	desc, report, err := assemble.New(store).AssembleImage(context.Background(), spec)
	if err != nil {
		return err
	}
	platform := manifest.Platform{OS: *osName, Architecture: *arch, Variant: *variant}
	if err := reg.AttachPlatform(context.Background(), ref, desc, platform); err != nil {
		return err
	}
	return printJSON(report)
}

func cmdSelect(args []string) error {
	fs := flag.NewFlagSet("select", flag.ContinueOnError)
	osName := fs.String("os", "linux", "target OS")
	arch := fs.String("arch", "amd64", "target architecture")
	variant := fs.String("variant", "", "target variant")
	pos, err := parseInterspersed(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return errors.New("usage: pipeline select <dir> <name:tag> [--os --arch --variant]")
	}
	_, reg, err := openAll(pos[0])
	if err != nil {
		return err
	}
	platform := manifest.Platform{OS: *osName, Architecture: *arch, Variant: *variant}
	desc, err := reg.SelectPlatform(context.Background(), pos[1], platform)
	if err != nil {
		return err
	}
	return printJSON(desc)
}

func cmdResolve(args []string) error {
	if len(args) != 2 {
		return errors.New("usage: pipeline resolve <dir> <name:tag>")
	}
	_, reg, err := openAll(args[0])
	if err != nil {
		return err
	}
	desc, err := reg.Resolve(context.Background(), args[1])
	if err != nil {
		return err
	}
	return printJSON(desc)
}

func cmdGC(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: pipeline gc <dir>")
	}
	store, reg, err := openAll(args[0])
	if err != nil {
		return err
	}
	res, err := gc.New(store, reg).Run(context.Background())
	if err != nil {
		return err
	}
	return printJSON(res)
}

func cmdVerify(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: pipeline verify <dir>")
	}
	store, _, err := openAll(args[0])
	if err != nil {
		return err
	}
	all, err := store.List(context.Background())
	if err != nil {
		return err
	}
	bad := 0
	for _, d := range all {
		if _, err := store.GetBytes(context.Background(), d); err != nil {
			bad++
			fmt.Printf("BAD  %s: %v\n", d.Short(), err)
		} else {
			fmt.Printf("OK   %s\n", d.Short())
		}
	}
	if bad > 0 {
		return fmt.Errorf("%d of %d objects failed verification", bad, len(all))
	}
	fmt.Printf("all %d objects verified\n", len(all))
	return nil
}
