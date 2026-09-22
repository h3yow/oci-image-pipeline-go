package assembler

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// fixedModTime 为层内所有条目写入的统一修改时间（UTC）。
// 构建上下文在磁盘上的 mtime 不参与摘要，保证不同机器/不同时间重复
// 组装同一内容得到完全一致的 tar 字节与层摘要。
var fixedModTime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// contextEntry 为遍历构建上下文得到的一个规整条目。
type contextEntry struct {
	// relPath 为相对上下文根的 POSIX 风格路径（无前导 "./"）。
	relPath string
	// mode 为权限位（仅低 9 位 + 类型位参与摘要）。
	mode fs.FileMode
	// content 为常规文件内容；目录与符号链接为空。
	content []byte
	// linkname 为符号链接目标。
	linkname string
	isDir    bool
}

// walkContext 按路径字典序遍历 contextDir，产出确定性的条目序列。
// 仅收录常规文件、目录与符号链接；设备/套接字等非常规类型导致错误。
func walkContext(contextDir string) ([]contextEntry, error) {
	var entries []contextEntry
	err := filepath.WalkDir(contextDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return ErrContextMissing
			}
			return fmt.Errorf("%w: %s: %v", ErrContextUnreadable, path, err)
		}
		rel, err := filepath.Rel(contextDir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		e := contextEntry{relPath: rel, mode: mode.Perm()}

		switch {
		case mode.IsDir():
			e.isDir = true
			// 目录条目统一以 "/" 结尾，便于稳定识别。
			if !strings.HasSuffix(e.relPath, "/") {
				e.relPath += "/"
			}
		case mode.IsRegular():
			b, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("%w: %s: %v", ErrContextUnreadable, path, err)
			}
			e.content = b
		case mode&fs.ModeSymlink != 0:
			target, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("%w: readlink %s: %v", ErrContextUnreadable, path, err)
			}
			e.linkname = target
		default:
			return fmt.Errorf("%w: unsupported file type for %s (mode=%v)",
				ErrContextUnreadable, rel, mode)
		}
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrContextMissing) {
			return nil, ErrContextMissing
		}
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].relPath < entries[j].relPath })
	return entries, nil
}

// writeTar 将条目序列写成规范化 unix tar 到 w。
// 规范化口径：
//   - 条目按 relPath 字典序（调用方已排序）；
//   - 仅 Typeflag/Name/LinkName/Mode(低9位)/Size/固定 Uid,Gid=0/固定
//     Modtime/内容 参与；Unames/Gnames 为空、PAX/USTAR 扩展不写入；
//   - 目录大小为 0 且以 '/' 结尾。
func writeTar(w io.Writer, entries []contextEntry) error {
	tw := tar.NewWriter(w)
	for _, e := range entries {
		hdr := &tar.Header{
			Name:    e.relPath,
			ModTime: fixedModTime,
			Uid:     0,
			Gid:     0,
			Uname:   "",
			Gname:   "",
			Mode:    int64(e.mode.Perm()),
		}
		switch {
		case e.isDir:
			hdr.Typeflag = tar.TypeDir
			hdr.Mode |= int64(0o755 & 0o111) // 目录至少保留一个执行位口径：取自身 perm
			if hdr.Mode == 0 {
				hdr.Mode = 0o755
			}
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
		case e.linkname != "":
			hdr.Typeflag = tar.TypeSymlink
			hdr.Linkname = e.linkname
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
		default:
			hdr.Typeflag = tar.TypeReg
			hdr.Size = int64(len(e.content))
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			if _, err := tw.Write(e.content); err != nil {
				return err
			}
		}
	}
	return tw.Close()
}

// buildLayerTar 从上下文目录构建规范化 tar，并返回：
//   - tar 字节（用于写入 CAS，其摘要即层 digest，压缩口径：未压缩 tar）；
//   - diffID（对未压缩 tar 的 sha256，进入镜像配置 rootfs.diff_ids）。
//
// 本实现中层 digest 与 diff id 字节口径相同（均为未压缩 tar 的 sha256），
// 但二者语义字段分开存放，不混用标识。
func buildLayerTar(contextDir string) ([]byte, string, error) {
	entries, err := walkContext(contextDir)
	if err != nil {
		return nil, "", err
	}
	var buf bytes.Buffer
	if err := writeTar(&buf, entries); err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:]), nil
}
