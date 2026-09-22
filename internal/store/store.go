// Package store 实现基于本地目录的内容寻址存储（CAS）。
//
// 所有对象以其内容的 sha256 摘要命名，存放于 blobs/sha256/<hex>。
// 写入采用"临时文件 + fsync + 原子重命名"，读取时重新计算摘要并与名称比对，
// 因此：摘要不匹配返回 ErrDigestMismatch，对象缺失返回 ErrNotFound，
// 声明大小与实际不符（截断）返回 ErrSizeMismatch，三者严格区分，
// 损坏内容绝不会被当作未命中静默处理，也绝不会在未报错的情况下返回。
//
// 并发：单进程内以 RWMutex 串行化变更操作；重命名的原子性保证任何进程
// 的读者都只会看到"完整旧对象"或"完整新对象"，不会看到半写入文件。
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// Store 为本地内容寻址存储。
type Store struct {
	root string

	mu sync.RWMutex
}

// 可区分的错误原因。
var (
	// ErrNotFound 对象不存在。
	ErrNotFound = errors.New("cas: object not found")
	// ErrDigestMismatch 读取校验时实际摘要与期望摘要不一致（内容损坏/被替换）。
	ErrDigestMismatch = errors.New("cas: digest mismatch")
	// ErrSizeMismatch 实际大小与声明大小不一致（对象被截断）。
	ErrSizeMismatch = errors.New("cas: size mismatch (truncated)")
	// ErrInvalidDigest 调用方提供的摘要无法解析或算法不受支持。
	ErrInvalidDigest = errors.New("cas: invalid digest")
)

// Open 打开（必要时初始化）root 下的存储目录。
func Open(root string) (*Store, error) {
	if root == "" {
		return nil, errors.New("cas: empty root")
	}
	dir := filepath.Join(root, "blobs", "sha256")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("cas: init %s: %w", dir, err)
	}
	tmp := filepath.Join(root, "tmp")
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return nil, fmt.Errorf("cas: init %s: %w", tmp, err)
	}
	return &Store{root: root}, nil
}

// Root 返回存储根目录。
func (s *Store) Root() string { return s.root }

func (s *Store) blobDir() string { return filepath.Join(s.root, "blobs", "sha256") }
func (s *Store) tmpDir() string  { return filepath.Join(s.root, "tmp") }

// path 返回摘要对象的目标路径，并校验摘要合法性。
func (s *Store) path(dgst digest.Digest) (string, error) {
	if dgst.IsZero() || dgst.Algorithm() != digest.SHA256 {
		return "", fmt.Errorf("%w: %q", ErrInvalidDigest, dgst)
	}
	return filepath.Join(s.blobDir(), dgst.Hex()), nil
}

// Put 以内容寻址方式写入 r：成功后返回内容摘要与写入字节数。
// 若相同摘要对象已存在，则不重复写入（去重），返回其摘要与内容大小。
//
// 流程：写临时文件并同步计算 sha256 → fsync → 在锁内检查目标是否已存在
// → 原子 rename。失败时临时文件必被清理。
func (s *Store) Put(ctx context.Context, r io.Reader) (digest.Digest, int64, error) {
	if r == nil {
		return digest.Digest{}, 0, errors.New("cas: nil reader")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	// 用进程内唯一的临时名，避免并发 Put 相互覆盖。
	tmp, err := os.CreateTemp(s.tmpDir(), "put-*")
	if err != nil {
		return digest.Digest{}, 0, fmt.Errorf("cas: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		tmp.Close()
		cleanup()
		return digest.Digest{}, 0, fmt.Errorf("cas: spool content: %w", err)
	}
	if err := ctx.Err(); err != nil {
		tmp.Close()
		cleanup()
		return digest.Digest{}, 0, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return digest.Digest{}, 0, fmt.Errorf("cas: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return digest.Digest{}, 0, fmt.Errorf("cas: close temp: %w", err)
	}

	dgst, err := digest.New(digest.SHA256, hex.EncodeToString(h.Sum(nil)))
	if err != nil {
		cleanup()
		return digest.Digest{}, 0, err
	}
	final, err := s.path(dgst)
	if err != nil {
		cleanup()
		return digest.Digest{}, 0, err
	}

	// 去重：目标已存在且大小一致则直接复用，绝不重复写入。
	if fi, err := os.Stat(final); err == nil {
		if fi.Mode().IsRegular() && fi.Size() == n {
			cleanup()
			return dgst, n, nil
		}
		// 同名却大小异常：保留新内容重命名前先移除损坏占位。
		if err := os.Remove(final); err != nil {
			cleanup()
			return digest.Digest{}, 0, fmt.Errorf("cas: remove corrupt placeholder: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		cleanup()
		return digest.Digest{}, 0, fmt.Errorf("cas: stat target: %w", err)
	}

	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return digest.Digest{}, 0, fmt.Errorf("cas: commit rename: %w", err)
	}
	if err := syncDir(s.blobDir()); err != nil {
		// 重命名已完成；目录同步失败不影响本次内容可见性。
		return dgst, n, nil
	}
	return dgst, n, nil
}

// Get 打开 dgst 对应对象供读取，期望大小以磁盘 stat 为准（适用于调用方
// 没有描述符、仅按摘要读取的场景）。需要按"描述符声明大小"区分截断时，
// 使用 OpenExpected。
func (s *Store) Get(ctx context.Context, dgst digest.Digest) (io.ReadCloser, int64, error) {
	return s.OpenExpected(ctx, dgst, -1)
}

// OpenExpected 打开 dgst 对象，并以 declaredSize 作为完整性判据之一：
//   - declaredSize >= 0：磁盘实际大小与之不符（截断/追加）立即返回
//     ErrSizeMismatch；读取结束时若字节数不足同样为 ErrSizeMismatch；
//   - declaredSize < 0：以磁盘 stat 大小为期望大小；
//   - 读取全程计算 sha256，EOF 时与 dgst 不符返回 ErrDigestMismatch；
//   - 对象不存在返回 ErrNotFound。
//
// 三类失败严格区分：截断绝不会被误报为摘要不符，损坏绝不会被当作未命中。
func (s *Store) OpenExpected(ctx context.Context, dgst digest.Digest, declaredSize int64) (io.ReadCloser, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	final, err := s.path(dgst)
	if err != nil {
		return nil, 0, err
	}
	fi, err := os.Stat(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, fmt.Errorf("%w: %s", ErrNotFound, dgst)
		}
		return nil, 0, err
	}
	if !fi.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("%w: not a regular file: %s", ErrNotFound, dgst)
	}
	if declaredSize >= 0 && fi.Size() != declaredSize {
		return nil, fi.Size(), fmt.Errorf(
			"%w: object %s declared %d bytes but stored %d bytes",
			ErrSizeMismatch, dgst, declaredSize, fi.Size())
	}
	f, err := os.Open(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, fmt.Errorf("%w: %s", ErrNotFound, dgst)
		}
		return nil, 0, err
	}
	return &verifyingReader{
		f:        f,
		expect:   dgst,
		wantSize: fi.Size(),
		h:        sha256.New(),
	}, fi.Size(), nil
}

type verifyingReader struct {
	f        *os.File
	expect   digest.Digest
	wantSize int64
	h        hash.Hash
	got      int64
	verified bool
	closed   bool
}

func (v *verifyingReader) Read(p []byte) (int, error) {
	n, err := v.f.Read(p)
	if n > 0 {
		v.h.Write(p[:n])
		v.got += int64(n)
	}
	if errors.Is(err, io.EOF) {
		if verr := v.finish(); verr != nil {
			return n, verr
		}
	}
	return n, err
}

// finish 在完整读取后校验大小与摘要，幂等。
func (v *verifyingReader) finish() error {
	if v.verified {
		return nil
	}
	if v.got != v.wantSize {
		return fmt.Errorf("%w: object %s declared %d bytes but read %d",
			ErrSizeMismatch, v.expect, v.wantSize, v.got)
	}
	gotSum := hex.EncodeToString(v.h.Sum(nil))
	gotDigest, err := digest.New(digest.SHA256, gotSum)
	if err != nil {
		return err
	}
	if !gotDigest.Equal(v.expect) {
		return fmt.Errorf("%w: expected %s, content hashes to %s",
			ErrDigestMismatch, v.expect, gotDigest)
	}
	v.verified = true
	return nil
}

func (v *verifyingReader) Close() error {
	if v.closed {
		return nil
	}
	v.closed = true
	return v.f.Close()
}

// Stat 返回对象在磁盘上的实际大小；缺失时返回 ErrNotFound。
func (s *Store) Stat(ctx context.Context, dgst digest.Digest) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	final, err := s.path(dgst)
	if err != nil {
		return 0, err
	}
	fi, err := os.Stat(final)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, fmt.Errorf("%w: %s", ErrNotFound, dgst)
		}
		return 0, err
	}
	return fi.Size(), nil
}

// Exists 报告对象是否存在且为常规文件（不做内容校验）。
func (s *Store) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	size, err := s.Stat(ctx, dgst)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = size
	return true, nil
}

// Delete 删除对象。缺失返回 ErrNotFound。仍被引用与否由 gc 层判定。
func (s *Store) Delete(ctx context.Context, dgst digest.Digest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	final, err := s.path(dgst)
	if err != nil {
		return err
	}
	if err := os.Remove(final); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrNotFound, dgst)
		}
		return err
	}
	return nil
}

// List 返回存储中全部对象的摘要（文件名即摘要；不校验内容）。
func (s *Store) List(ctx context.Context) ([]digest.Digest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.blobDir())
	if err != nil {
		return nil, err
	}
	var out []digest.Digest
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		d, err := digest.New(digest.SHA256, e.Name())
		if err != nil {
			// 非规范命名文件跳过（不是本存储写入的对象）。
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

// syncDir 对目录执行 fsync，保证重命名落盘。
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
