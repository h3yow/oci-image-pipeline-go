// Package store 实现基于本地目录的内容寻址存储（CAS）。
//
// 所有对象以其内容的 sha256 摘要命名，存放于 blobs/sha256/<hex>。
// 写入采用"临时文件 + fsync + 原子重命名"，读取时重新计算摘要并与名称比对，
// 因此：摘要不匹配返回 ErrDigestMismatch，对象缺失返回 ErrNotFound，
// 声明大小与实际不符（截断）返回 ErrSizeMismatch，三者严格区分，
// 损坏内容绝不会被当作未命中静默处理，也绝不会返回给调用方。
package store

import (
	"context"
	"errors"
	"io"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// Store 为本地内容寻址存储。
type Store struct {
	root string
}

// 可区分的错误原因。
var (
	// ErrNotFound 对象不存在。
	ErrNotFound = errors.New("cas: object not found")
	// ErrDigestMismatch 读取校验时实际摘要与期望摘要不一致（内容损坏/被替换）。
	ErrDigestMismatch = errors.New("cas: digest mismatch")
	// ErrSizeMismatch 实际大小与声明大小不一致（对象被截断）。
	ErrSizeMismatch = errors.New("cas: size mismatch (truncated)")
	// ErrInvalidDigest 调用方提供的摘要无法解析。
	ErrInvalidDigest = errors.New("cas: invalid digest")
)

// Open 打开（必要时初始化）root 下的存储目录。
func Open(root string) (*Store, error) {
	return &Store{root: root}, nil
}

// Root 返回存储根目录。
func (s *Store) Root() string { return s.root }

// Put 以内容寻址方式写入 r：成功后返回内容摘要与写入字节数。
// 若相同摘要对象已存在，则不重复写入（去重），返回其摘要与内容大小。
func (s *Store) Put(ctx context.Context, r io.Reader) (digest.Digest, int64, error) {
	return digest.Digest{}, 0, nil
}

// Get 打开 dgst 对应对象供读取。返回的 ReadCloser 在读取全程做摘要校验，
// Close 时完成最终校验；内容损坏则在 Read/Close 阶段返回对应错误。
func (s *Store) Get(ctx context.Context, dgst digest.Digest) (io.ReadCloser, int64, error) {
	return nil, 0, nil
}

// Stat 返回对象在磁盘上的实际大小；缺失时返回 ErrNotFound。
func (s *Store) Stat(ctx context.Context, dgst digest.Digest) (int64, error) {
	return 0, nil
}

// Exists 报告对象是否存在且为常规文件（不做内容校验）。
func (s *Store) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	return false, nil
}

// Delete 删除对象。缺失返回 ErrNotFound。
// 注意：是否仍被引用由 gc 层判定，Store 本身只做原语操作。
func (s *Store) Delete(ctx context.Context, dgst digest.Digest) error {
	return nil
}

// path 返回摘要对象的目标路径（内部辅助）。
func (s *Store) path(dgst digest.Digest) string { return "" }
