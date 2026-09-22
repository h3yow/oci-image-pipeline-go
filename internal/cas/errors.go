// Package cas 提供本地目录上的内容寻址存储（Content Addressable Storage）。
package cas

import "errors"

// 可区分的拒绝原因：调用方可通过 errors.Is 精确判定。
var (
	// ErrDigestMismatch 写入或校验时内容摘要与声明摘要不一致（含对象被截断/篡改）。
	ErrDigestMismatch = errors.New("cas: digest mismatch")
	// ErrNotFound 对象不存在（缺失不得与损坏混淆）。
	ErrNotFound = errors.New("cas: object not found")
	// ErrCorrupt 对象存在但内容已损坏（摘要不匹配/长度截断），不得静默视为未命中。
	ErrCorrupt = errors.New("cas: corrupt object")
	// ErrInvalidDigest 摘要标识本身非法（算法/格式不受支持）。
	ErrInvalidDigest = errors.New("cas: invalid digest")
)
