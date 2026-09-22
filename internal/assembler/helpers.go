package assembler

import "os"

// osStat 隔离 os.Stat，保持 assembler.go 主流程简洁。
func osStat(name string) (os.FileInfo, error) { return os.Stat(name) }
