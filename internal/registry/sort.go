package registry

import "sort"

// sortStrings 独立为小函数，避免主文件导入 sort 仅用于一处。
func sortStrings(s []string) { sort.Strings(s) }
