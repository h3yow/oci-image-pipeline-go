package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/manifest"
	"github.com/h3yow/oci-image-pipeline-go/internal/registry"
)

// stringList 支持重复的 --entry 标志。
type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// runIndex 用多个清单条目构建多架构索引并打标签。
//
// 条目语法：--entry os/arch[/variant]:sha256:<hex>[,size=N]
// size 缺省时从 CAS 读取实际大小；mediaType 固定为 v1 清单。
func runIndex(args []string) error {
	fs := flag.NewFlagSet("index", flag.ContinueOnError)
	root := rootFlag(fs)
	name := fs.String("name", "", "索引引用 name:tag（必填）")
	var rawEntries stringList
	fs.Var(&rawEntries, "entry", "平台清单条目，可重复：os/arch[/variant]:sha256:<hex>")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *name == "" || len(rawEntries) == 0 {
		return fmt.Errorf("--name and at least one --entry are required")
	}
	ref, err := registry.ParseRef(*name)
	if err != nil {
		return err
	}

	ctx := context.Background()
	st, reg, err := openStack(*root)
	if err != nil {
		return err
	}

	entries, err := parseEntries(rawEntries)
	if err != nil {
		return err
	}
	for i := range entries {
		size, gerr := st.Stat(ctx, entries[i].Descriptor.Digest)
		if gerr != nil {
			return fmt.Errorf("entry[%d]: %w", i, gerr)
		}
		entries[i].Descriptor.Size = size
	}
	desc, err := reg.BuildAndPushIndex(ctx, ref, entries)
	if err != nil {
		return err
	}
	fmt.Printf("index: %s entries=%d\n", desc.Digest, len(entries))
	fmt.Printf("tagged: %s -> %s\n", ref, desc.Digest)
	return nil
}

func parseEntries(raw []string) ([]manifest.IndexEntry, error) {
	out := make([]manifest.IndexEntry, 0, len(raw))
	for i, item := range raw {
		platPart, dgstPart, ok := strings.Cut(item, ":sha256:")
		if !ok {
			return nil, fmt.Errorf("entry[%d] %q: want os/arch[/variant]:sha256:<hex>", i, item)
		}
		segs := strings.Split(platPart, "/")
		if len(segs) != 2 && len(segs) != 3 {
			return nil, fmt.Errorf("entry[%d] platform %q: want os/arch[/variant]", i, platPart)
		}
		p := descriptor.Platform{OS: segs[0], Architecture: segs[1]}
		if len(segs) == 3 {
			p.Variant = segs[2]
		}
		hexPart := dgstPart
		if comma := strings.Index(hexPart, ","); comma >= 0 {
			tail := hexPart[comma+1:]
			hexPart = hexPart[:comma]
			if !strings.HasPrefix(tail, "size=") {
				return nil, fmt.Errorf("entry[%d]: unsupported suffix %q", i, tail)
			}
			if _, err := strconv.ParseInt(strings.TrimPrefix(tail, "size="), 10, 64); err != nil {
				return nil, fmt.Errorf("entry[%d] size: %w", i, err)
			}
		}
		dgst, err := digest.Parse("sha256:" + hexPart)
		if err != nil {
			return nil, fmt.Errorf("entry[%d]: %w", i, err)
		}
		out = append(out, manifest.IndexEntry{
			Descriptor: descriptor.Descriptor{
				MediaType: descriptor.MediaTypeImageManifest,
				Digest:    dgst,
			},
			Platform: &p,
		})
	}
	return out, nil
}
