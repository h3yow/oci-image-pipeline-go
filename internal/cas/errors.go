// Package cas 提供本地目录上的内容寻址存储（Content Addressable Storage）。
package cas

import "errors"

// 可区分的拒绝原因：调用方可通过 errors.Is 精确判定。
//
// 完整性三分类（读取、校验、回收、提交前检查共用同一套口径）：
//   - ErrNotFound：对象根本不存在；
//   - ErrSizeMismatch：对象存在，但字节长度与声明大小不符（疑似截断）；
//   - ErrDigestMismatch：对象存在、长度也对，但内容摘要不符（疑似篡改）。
//
// 后两者同时链 ErrCorrupt，调用方既可用 ErrCorrupt 笼统判定“对象在但坏了”，
// 也可用两个细分原因精确归类。损坏绝不静默退化为 ErrNotFound。
var (
	// ErrDigestMismatch 对象存在、长度一致但内容摘要与声明摘要不一致（内容被篡改）。
	ErrDigestMismatch = errors.New("cas: digest mismatch")
	// ErrSizeMismatch 对象存在但字节长度与声明大小不一致（疑似被截断/追加）。
	ErrSizeMismatch = errors.New("cas: size mismatch")
	// ErrNotFound 对象不存在（缺失不得与损坏混淆）。
	ErrNotFound = errors.New("cas: object not found")
	// ErrCorrupt 对象存在但内容已损坏（长度不符或摘要不符）的总类，
	// ErrSizeMismatch 与 ErrDigestMismatch 均同时可 errors.Is 命中它。
	ErrCorrupt = errors.New("cas: corrupt object")
	// ErrInvalidDigest 摘要标识本身非法（算法/格式不受支持）。
	ErrInvalidDigest = errors.New("cas: invalid digest")
)
