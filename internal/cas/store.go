package cas

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// Store 基于本地目录的内容寻址存储。
//
// 存储布局：
//
//	<root>/blobs/sha256/<hex>          不可变对象
//	<root>/tmp/                        写入中的临时文件（原子 rename 提交）
//
// 所有方法对并发安全；写入通过“同卷临时文件 + fsync + 原子 rename”保证
// 不会出现半写入可见；回收通过共享锁表保证不会删除正在写入/读取的对象。
type Store struct {
	root    string
	blobs   string
	tmp     string
	locker  *Locker
	gateGen sync.Once
	gates   sync.Map // digest.Digest -> *sync.Mutex，串行化同摘要的进程内写入
}

// NewStore 打开（必要时初始化）root 下的内容寻址存储。
func NewStore(root string) (*Store, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	s := &Store{
		root:   abs,
		blobs:  filepath.Join(abs, "blobs", digest.AlgorithmSHA256),
		tmp:    filepath.Join(abs, "tmp"),
		locker: NewLocker(),
	}
	for _, dir := range []string{s.root, filepath.Join(abs, "blobs"), s.blobs, s.tmp} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("cas: init %s: %w", dir, err)
		}
	}
	return s, nil
}

// Root 返回存储根目录。
func (s *Store) Root() string { return s.root }

// Lock 返回与回收协调用的锁表句柄。
func (s *Store) Lock() *Locker { return s.locker }

// Path 返回某摘要对象的落地路径（仅供诊断与 GC 使用）。
func (s *Store) Path(d digest.Digest) (string, error) {
	if _, err := digest.Parse(string(d)); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	return filepath.Join(s.blobs, d.Hex()), nil
}

// gate 取得某个摘要对应的进程内串行写锁。
func (s *Store) gate(d digest.Digest) *sync.Mutex {
	mu, _ := s.gates.LoadOrStore(d, &sync.Mutex{})
	return mu.(*sync.Mutex)
}

// verifyFile 全量计算已落地文件的摘要并与 expected 比对。
func verifyFile(f *os.File, expected digest.Digest) (int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return n, err
	}
	got := digest.AlgorithmSHA256 + ":" + hex.EncodeToString(h.Sum(nil))
	if digest.Digest(got) != expected {
		return n, fmt.Errorf("%w: %w: expected %s, file computes %s (size=%d)",
			ErrCorrupt, ErrDigestMismatch, expected, got, n)
	}
	return n, nil
}

// Inspect 是全链路共用的单一完整性判定口径，对对象给出三类互斥结论：
//
//   - 不存在：返回 ErrNotFound（对象根本没有）；
//   - 长度不符：返回可 errors.Is(ErrCorrupt) 且 errors.Is(ErrSizeMismatch)
//     的错误（对象在，但字节数与声明大小不一致，疑似截断/追加）；
//   - 内容篡改：返回可 errors.Is(ErrCorrupt) 且 errors.Is(ErrDigestMismatch)
//     的错误（长度一致但摘要不符，内容被改过）。
//
// declaredSize 为描述符声明的字节数；传入负值表示“此处没有大小声明”，
// 此时只按摘要校验，损坏一律归类为 ErrDigestMismatch（无法再区分截断）。
// 校验通过时返回对象的实际字节数。读取、回收、全库校验、提交前检查
// 都必须经由本函数（或内部调用它的 Get/Stat），保证判定一致。
func (s *Store) Inspect(ctx context.Context, d digest.Digest, declaredSize int64) (int64, error) {
	if _, err := digest.Parse(string(d)); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	p, err := s.Path(d)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("%w: %s", ErrNotFound, d.Short())
	}
	if err != nil {
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("%w: %w: %s is a directory", ErrCorrupt, ErrSizeMismatch, p)
	}
	// 先按声明大小判定：长度不符疑似截断，无需再读内容。
	if declaredSize >= 0 && info.Size() != declaredSize {
		return info.Size(), fmt.Errorf(
			"%w: %w: %s declared size %d, actual size %d",
			ErrCorrupt, ErrSizeMismatch, d.Short(), declaredSize, info.Size())
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return 0, fmt.Errorf("%w: %s", ErrNotFound, d.Short())
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	n, verr := verifyFile(f, d)
	if verr != nil {
		return n, fmt.Errorf("object %s: %w", d.Short(), verr)
	}
	if declaredSize >= 0 && n != declaredSize {
		return n, fmt.Errorf("%w: %w: %s declared size %d, read size %d",
			ErrCorrupt, ErrSizeMismatch, d.Short(), declaredSize, n)
	}
	return n, nil
}

// existingIntact 检查对象是否已完整存在：
//
//	true   -> 已存在且完整（去重，不得重复写入）
//	false  -> 不存在
//	error  -> 存在但损坏（Put 不静默覆盖，直接把错误交调用方）
func (s *Store) existingIntact(d digest.Digest) (bool, error) {
	// 按摘要读取没有独立的大小声明，声明大小传 -1：仅做摘要口径校验。
	if _, err := s.Inspect(context.Background(), d, -1); err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ctxReader 在读取过程中响应取消。
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// Put 将 r 的全部内容以期望摘要 expected 写入存储。
//
//   - 内容摘要与 expected 不一致 -> ErrDigestMismatch，对象不落地；
//   - 相同摘要对象已存在且完整 -> 不重复写入，直接返回（去重）；
//   - 并发同摘要写入 -> 进程内串行化，先到者落地，后到者去重；
//     绝不出现半写入（临时文件 + 原子 rename）。
func (s *Store) Put(ctx context.Context, expected digest.Digest, r io.Reader) (err error) {
	if _, perr := digest.Parse(string(expected)); perr != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDigest, perr)
	}

	mu := s.gate(expected)
	mu.Lock()
	defer mu.Unlock()

	if ok, verifyErr := s.existingIntact(expected); verifyErr != nil {
		// 同名对象已存在但损坏：绝不静默覆盖。损坏可能正被某个（旧）引用
		// 指向，静默替换会掩盖完整性问题；统一口径下由提交闸门/校验暴露
		// 为 ErrCorrupt，调用方修复时应先删除损坏对象再重写。
		return verifyErr
	} else if ok {
		return nil // 去重：已存在的完整对象不得重复写入
	}

	s.locker.BeginWrite(expected)
	defer s.locker.EndWrite(expected)

	// 先生成唯一名并 pin，再创建临时文件：保证临时文件自“尚未存在”起
	// 就对 GC 可见，杜绝创建后、登记前被并发 GC 清掉导致 rename ENOENT。
	tmpName, err := reserveTempName(s.tmp)
	if err != nil {
		return fmt.Errorf("cas: reserve temp: %w", err)
	}
	releaseTmp := s.locker.PinTmp(filepath.Base(tmpName))
	tmp, err := os.OpenFile(tmpName, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		releaseTmp()
		return fmt.Errorf("cas: create temp: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tmp.Close()
			_ = os.Remove(tmpName) // 写入失败/取消：临时文件绝不以半写入形态留在 blobs
		}
		releaseTmp()
	}()

	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), &ctxReader{ctx: ctx, r: r})
	if err != nil {
		return fmt.Errorf("cas: staging %s: %w", expected.Short(), err)
	}
	got := digest.Digest(digest.AlgorithmSHA256 + ":" + hex.EncodeToString(h.Sum(nil)))
	if got != expected {
		return fmt.Errorf("%w: refused to commit %s: content computes %s (size=%d)",
			ErrDigestMismatch, expected, got, n)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("cas: fsync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cas: close temp: %w", err)
	}

	dst, derr := s.Path(expected)
	if derr != nil {
		return derr
	}
	// 同卷原子替换：任何并发读者要么看到旧对象，要么看到完整新对象。
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("cas: commit %s: %w", expected.Short(), err)
	}
	committed = true
	return nil
}

// PutBytes 写入字节并返回其摘要（按内容计算，调用方无需预知）。
func (s *Store) PutBytes(ctx context.Context, data []byte) (digest.Digest, error) {
	d := digest.FromBytes(data)
	if err := s.Put(ctx, d, byteReader(data)); err != nil {
		return "", err
	}
	return d, nil
}

type byteSliceReader struct {
	b []byte
	i int
}

func byteReader(b []byte) io.Reader { return &byteSliceReader{b: b} }

func (r *byteSliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// verifyingReadCloser 在“完整读取”结束时复核整段内容摘要，
// 作为除打开前校验之外的第二道防线（防止读取期间被外部篡改）。
// 仅在读到 EOF（流被完整消费）时才校验：提前 Close 的调用方
// （如 Exists/Stat）不应因未读完而被误报损坏。
type verifyingReadCloser struct {
	store    *Store
	file     *os.File
	expected digest.Digest
	hash     interface {
		Write([]byte) (int, error)
		Sum([]byte) []byte
	}
	eof    bool
	closed bool
}

func (v *verifyingReadCloser) Read(p []byte) (int, error) {
	n, err := v.file.Read(p)
	if n > 0 {
		v.hash.Write(p[:n])
	}
	if err == io.EOF {
		v.eof = true
	}
	return n, err
}

func (v *verifyingReadCloser) Close() error {
	if v.closed {
		return nil
	}
	v.closed = true
	v.store.locker.EndRead(v.expected)
	cerr := v.file.Close()
	if !v.eof {
		return cerr // 未完整消费：不做流式复核（打开前已全量校验过）
	}
	sum := digest.AlgorithmSHA256 + ":" + hex.EncodeToString(v.hash.Sum(nil))
	if digest.Digest(sum) != v.expected {
		return fmt.Errorf("%w: stream of %s changed while reading (computed %s): %w",
			ErrCorrupt, v.expected.Short(), sum, ErrDigestMismatch)
	}
	return cerr
}

// Get 返回对象内容的读取器与其字节数；打开前经由统一口径 Inspect 校验，
// 读取期间通过 pin 阻止回收，Close 时再次复核。
//
//   - 对象缺失 -> ErrNotFound；
//   - 对象存在但损坏 -> ErrCorrupt，且按内容长度/摘要可进一步
//     errors.Is 到 ErrSizeMismatch（按摘要读取时无独立大小声明，
//     此处一般体现为 ErrDigestMismatch）。
func (s *Store) Get(ctx context.Context, d digest.Digest) (io.ReadCloser, int64, error) {
	if _, err := digest.Parse(string(d)); err != nil {
		return nil, 0, fmt.Errorf("%w: %v", ErrInvalidDigest, err)
	}
	p, err := s.Path(d)
	if err != nil {
		return nil, 0, err
	}
	s.locker.BeginRead(d)
	// 统一判定入口：缺失/截断/篡改在此一次性归类，绝不静默退化为未命中。
	size, verr := s.Inspect(ctx, d, -1)
	if verr != nil {
		s.locker.EndRead(d)
		return nil, 0, verr
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		s.locker.EndRead(d)
		return nil, 0, fmt.Errorf("%w: %s", ErrNotFound, d.Short())
	}
	if err != nil {
		s.locker.EndRead(d)
		return nil, 0, err
	}
	return &verifyingReadCloser{store: s, file: f, expected: d, hash: sha256.New()}, size, nil
}

// GetBytes 读取对象全部字节并校验完整性。
func (s *Store) GetBytes(ctx context.Context, d digest.Digest) ([]byte, error) {
	rc, _, err := s.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// Stat 返回对象大小；缺失 -> ErrNotFound，损坏/截断 -> ErrCorrupt。
func (s *Store) Stat(ctx context.Context, d digest.Digest) (int64, error) {
	rc, size, err := s.Get(ctx, d)
	if err != nil {
		return 0, err
	}
	return size, rc.Close()
}

// Exists 仅当对象存在且完整时返回 (true,nil)；
// 缺失为 (false,nil)；损坏为 (false,ErrCorrupt)，不得静默当作未命中。
func (s *Store) Exists(ctx context.Context, d digest.Digest) (bool, error) {
	rc, _, err := s.Get(ctx, d)
	if err == nil {
		err = rc.Close()
		return err == nil, err
	}
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	return false, err
}

// Has 仅报告路径上是否存在该摘要的文件，不读取、不校验内容。
// 与 Exists 的区别：Has 不把损坏判为错误，仅供组装阶段区分
// “对象原本就在（去重/修复写）”与“新写入”；完整性结论一律以
// Inspect（读取/回收/校验/提交闸门）为准。
func (s *Store) Has(d digest.Digest) (bool, error) {
	p, err := s.Path(d)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List 返回存储中全部对象的摘要（按文件名解析，不保证内容完整；
// 完整性校验属于 Get/Exists/verify 命令的职责）。
func (s *Store) List(_ context.Context) ([]digest.Digest, error) {
	entries, err := os.ReadDir(s.blobs)
	if err != nil {
		return nil, err
	}
	out := make([]digest.Digest, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		d, perr := digest.Parse(digest.AlgorithmSHA256 + ":" + e.Name())
		if perr != nil {
			// 命名非法的文件不猜测、不返回；由 verify 维护命令报告。
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

// Delete 删除对象；对象缺失返回 ErrNotFound。
// 调用方（GC）必须已确认对象不可达且未被 pin。
func (s *Store) Delete(_ context.Context, d digest.Digest) error {
	p, err := s.Path(d)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("%w: %s", ErrNotFound, d.Short())
		}
		return err
	}
	return nil
}

// reserveTempName 在 tmp 目录中生成一个尚不存在的唯一名（不创建文件），
// 以便调用方在创建前完成 pin。
func reserveTempName(dir string) (string, error) {
	var buf [16]byte
	for attempt := 0; attempt < 100; attempt++ {
		if _, err := cryptorand.Read(buf[:]); err != nil {
			return "", err
		}
		name := filepath.Join(dir, "put-"+hex.EncodeToString(buf[:]))
		if _, err := os.Stat(name); os.IsNotExist(err) {
			return name, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("cas: cannot reserve unique temp name")
}
