// Package cas 提供本地目录上的内容寻址存储（Content Addressable Storage）。
package cas

import "errors"

// 可区分的拒绝原因：调用方可通过 errors.Is 精确判定。
//
// 完整性三类口径（读取 / 校验 / 回收 / 提交前检查全程一致）：
//
//   - ErrNotFound：对象根本不存在；
//   - ErrSizeMismatch（同时链 ErrCorrupt）：对象存在，但实际长度与声明
//     大小对不上（疑似被截断/被追加），不再继续猜测内容；
//   - ErrDigestMismatch（同时链 ErrCorrupt）：对象存在、长度也一致，
//     但内容算出的摘要与声明摘要不同（内容被改过）。
//
// 缺失绝不与损坏混淆；截断与篡改也必须能分开判定。
var (
	// ErrDigestMismatch 大小一致但内容摘要与声明摘要不一致（内容被篡改）。
	ErrDigestMismatch = errors.New("cas: digest mismatch")
	// ErrSizeMismatch 对象实际大小与声明大小不一致（疑似截断/追加）。
	ErrSizeMismatch = errors.New("cas: size mismatch")
	// ErrNotFound 对象不存在（缺失不得与损坏混淆）。
	ErrNotFound = errors.New("cas: object not found")
	// ErrCorrupt 对象存在但已损坏：截断（ErrSizeMismatch）或篡改
	// （ErrDigestMismatch）都能 errors.Is 到本哨兵，便于统一兜底。
	ErrCorrupt = errors.New("cas: corrupt object")
	// ErrInvalidDigest 摘要标识本身非法（算法/格式不受支持）。
	ErrInvalidDigest = errors.New("cas: invalid digest")
)
