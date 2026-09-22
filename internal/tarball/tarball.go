// Package tarball 生成确定性（可复现）的 tar 归档，用于镜像层。
//
// 确定性口径：
//   - 文件条目按相对路径字典序排列；
//   - 所有时间戳固定为 Unix 纪元零值，uid/gid/uname/gname 固定；
//   - 仅写入目录与常规文件，模式取规范化后的权限位；
//   - 不写入 PAX/GNU 扩展头，输出仅取决于文件路径、权限与内容。
package tarball

import (
	"archive/tar"
	"bytes"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"
)

// File 归档中的一个常规文件。
type File struct {
	Path string // 相对根的 POSIX 路径，禁止绝对路径与 ".." 越界
	Data []byte
	Mode int64 // 仅低 9 位权限有效；0 缺省为 0644
}

var (
	errEmptyPath = errors.New("tarball: empty file path")
	errBadPath   = errors.New("tarball: absolute or escaping path")
)

func cleanRel(p string) (string, error) {
	if p == "" {
		return "", errEmptyPath
	}
	if path.IsAbs(p) || strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: %q", errBadPath, p)
	}
	c := path.Clean(p)
	if c == "." || c == ".." || strings.HasPrefix(c, "../") {
		return "", fmt.Errorf("%w: %q", errBadPath, p)
	}
	return c, nil
}

// Build 按确定性口径生成 tar 字节。重复输入恒得相同字节。
var epoch = time.Unix(0, 0).UTC()

func Build(files []File) ([]byte, error) {
	cleaned := make([]File, 0, len(files))
	seen := make(map[string]bool, len(files))
	for i, f := range files {
		c, err := cleanRel(f.Path)
		if err != nil {
			return nil, fmt.Errorf("files[%d]: %w", i, err)
		}
		if seen[c] {
			return nil, fmt.Errorf("tarball: duplicate path %q", c)
		}
		seen[c] = true
		mode := f.Mode & 0o777
		if mode == 0 {
			mode = 0o644
		}
		cleaned = append(cleaned, File{Path: c, Data: f.Data, Mode: mode})
	}
	sort.Slice(cleaned, func(i, j int) bool { return cleaned[i].Path < cleaned[j].Path })

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	// 先补齐全目录条目（同样确定性排序、零时间戳）。
	dirs := map[string]bool{}
	for _, f := range cleaned {
		for d := path.Dir(f.Path); d != "." && d != "/"; d = path.Dir(d) {
			dirs[d] = true
		}
	}
	dirList := make([]string, 0, len(dirs))
	for d := range dirs {
		dirList = append(dirList, d)
	}
	sort.Strings(dirList)
	for _, d := range dirList {
		hdr := &tar.Header{
			Name:     d + "/",
			Mode:     0o755,
			ModTime:  epoch,
			Typeflag: tar.TypeDir,
			Format:   tar.FormatUSTAR,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
	}

	for _, f := range cleaned {
		hdr := &tar.Header{
			Name:     f.Path,
			Mode:     f.Mode,
			Size:     int64(len(f.Data)),
			ModTime:  epoch,
			Typeflag: tar.TypeReg,
			Format:   tar.FormatUSTAR,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, fmt.Errorf("tarball: header %s: %w", f.Path, err)
		}
		if _, err := io.Copy(tw, bytes.NewReader(f.Data)); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
