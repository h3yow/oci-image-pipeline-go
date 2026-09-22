package registry

import "errors"

// ErrUnsupportedReferenceVersion 引用格式版本超出支持范围（如来自更新的格式）。
var ErrUnsupportedReferenceVersion = errors.New("registry: unsupported reference format version")
