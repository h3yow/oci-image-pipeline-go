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
	"github.com/h3yow/oci-image-pipeline-go/internal/commit"
	"github.com/h3yow/oci-image-pipeline-go/internal/gc"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
	"github.com/h3yow/oci-image-pipeline-go/internal/verify"
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
	// build 即一次原子提交：组装 + 整闭包校验 + 原子挂引用。
	res, err := commit.New(store, reg).Commit(context.Background(), commit.Request{
		Ref:   ref,
		Image: spec,
	})
	if err != nil {
		return err
	}
	return printJSON(res)
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
	// attach 即一次原子提交：组装 + 追加进索引，发布前整闭包校验。
	res, err := commit.New(store, reg).Commit(context.Background(), commit.Request{
		Ref:      ref,
		Image:    spec,
		Platform: manifest.Platform{OS: *osName, Architecture: *arch, Variant: *variant},
		AsIndex:  true,
	})
	if err != nil {
		return err
	}
	return printJSON(res)
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
	jsonOut := false
	rest := args
	// 支持 pipeline verify [--json] <dir>。
	if len(rest) >= 2 && rest[0] == "--json" {
		jsonOut = true
		rest = rest[1:]
	}
	if len(rest) != 1 {
		return errors.New("usage: pipeline verify [--json] <dir>")
	}
	store, reg, err := openAll(rest[0])
	if err != nil {
		return err
	}
	rep, err := verify.New(store, reg).Run(context.Background())
	if err != nil {
		return err
	}
	if jsonOut {
		// JSON 模式：stdout 只输出结构化报告，保证可直接被工具解析；
		// 人读汇总走 stderr，退出码仍体现成败。
		if err := printJSON(rep); err != nil {
			return err
		}
	} else {
		for _, f := range rep.Findings {
			switch f.Status {
			case verify.StatusOK:
				fmt.Printf("OK      %s\n", f.Digest.Short())
			case verify.StatusMissing:
				fmt.Printf("MISSING %s declared=%d: 被引用但对象不存在\n", f.Digest.Short(), f.DeclaredSize)
			case verify.StatusSizeMismatch:
				fmt.Printf("TRUNC   %s declared=%d actual=%d: %s\n",
					f.Digest.Short(), f.DeclaredSize, f.ActualSize, f.Error)
			case verify.StatusDigestMismatch:
				fmt.Printf("TAMPER  %s size=%d: %s\n", f.Digest.Short(), f.ActualSize, f.Error)
			}
		}
		// 引用逐条结论：明确指出是哪条引用、属于哪一类问题。
		for _, f := range rep.RefFindings {
			switch f.Status {
			case verify.RefStatusOK:
				fmt.Printf("REF-OK      %s\n", f.Ref)
			case verify.RefStatusCorruptRef:
				fmt.Printf("REF-CORRUPT %s %s: 引用文件读不出来或结构不合法: %s\n",
					f.Ref, f.Where, f.Error)
			case verify.RefStatusUnsupportedVersion:
				fmt.Printf("REF-VERSION %s %s: 引用格式版本超出支持范围: %s\n",
					f.Ref, f.Where, f.Error)
			case verify.RefStatusMissing:
				fmt.Printf("REF-MISSING %s @ %s %s declared=%d: 引用指向的对象不存在: %s\n",
					f.Ref, f.Where, f.Digest.Short(), f.DeclaredSize, f.Error)
			case verify.RefStatusSizeMismatch:
				fmt.Printf("REF-TRUNC   %s @ %s %s declared=%d: 引用闭包对象长度不符: %s\n",
					f.Ref, f.Where, f.Digest.Short(), f.DeclaredSize, f.Error)
			case verify.RefStatusDigestMismatch:
				fmt.Printf("REF-TAMPER  %s @ %s %s: 引用闭包对象内容被改: %s\n",
					f.Ref, f.Where, f.Digest.Short(), f.Error)
			case verify.RefStatusMalformed:
				fmt.Printf("REF-MALFORM %s @ %s %s: 引用闭包清单/索引结构不合法: %s\n",
					f.Ref, f.Where, f.Digest.Short(), f.Error)
			}
		}
	}
	counts := rep.Counts()
	bad := counts[verify.StatusMissing] + counts[verify.StatusSizeMismatch] + counts[verify.StatusDigestMismatch]
	summaryOut := os.Stdout
	if jsonOut {
		summaryOut = os.Stderr // JSON 模式下 stdout 只能是纯 JSON
	}
	fmt.Fprintf(summaryOut, "verify 汇总: ok=%d missing=%d size-mismatch=%d digest-mismatch=%d\n",
		counts[verify.StatusOK], counts[verify.StatusMissing],
		counts[verify.StatusSizeMismatch], counts[verify.StatusDigestMismatch])
	rc := rep.RefCounts()
	refBad := 0
	for st, n := range rc {
		if st != verify.RefStatusOK {
			refBad += n
		}
	}
	fmt.Fprintf(summaryOut, "verify 引用: ok=%d corrupt-ref=%d unsupported-version=%d missing=%d size-mismatch=%d digest-mismatch=%d malformed=%d\n",
		rc[verify.RefStatusOK], rc[verify.RefStatusCorruptRef],
		rc[verify.RefStatusUnsupportedVersion], rc[verify.RefStatusMissing],
		rc[verify.RefStatusSizeMismatch], rc[verify.RefStatusDigestMismatch],
		rc[verify.RefStatusMalformed])
	if bad+refBad > 0 {
		return fmt.Errorf("verify: %d object(s) and %d reference finding(s) failed verification", bad, refBad)
	}
	return nil
}
