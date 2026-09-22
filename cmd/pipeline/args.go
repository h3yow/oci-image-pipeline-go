package main

import (
	"flag"
	"strings"
)

// parseInterspersed 解析允许标志与位置参数混排的命令行（如
// `pipeline attach <dir> <ref> <ctx> --os linux`）。
//
// 返回位置参数。布尔标志按其自身定义处理；值型标志后跟其值。
func parseInterspersed(fs *flag.FlagSet, args []string) ([]string, error) {
	boolFlags := map[string]bool{}
	fs.VisitAll(func(f *flag.Flag) {
		if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); ok && bf.IsBoolFlag() {
			boolFlags[f.Name] = true
		}
	})
	var flagArgs, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if strings.HasPrefix(a, "-") && a != "-" {
			name := strings.TrimLeft(a, "-")
			if eq := strings.IndexByte(name, '='); eq >= 0 {
				name = name[:eq]
			}
			flagArgs = append(flagArgs, a)
			if !boolFlags[name] && !strings.Contains(a, "=") && i+1 < len(args) {
				flagArgs = append(flagArgs, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}
	return positional, nil
}
