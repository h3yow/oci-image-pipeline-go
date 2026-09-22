// Command oci-pipeline 是本地可运行的容器镜像组装与分发演示/验证入口。
//
// 子命令：
//
//	oci-pipeline init  --root DIR
//	    初始化本地 CAS 与仓库目录。
//	oci-pipeline build --root DIR --context DIR --name NAME:TAG [--os linux]
//	    [--arch amd64] [--at RFC3339]
//	    组装镜像并打标签；输出层/配置/镜像摘要与去重情况。
//	oci-pipeline push-index --root DIR --name NAME:TAG --digest sha256:...
//	    将已存在的索引对象（按摘要）绑到标签。
//	oci-pipeline resolve --root DIR --name NAME:TAG [--os linux --arch amd64
//	    [--variant v7]] [--digest]
//	    解析引用；给出平台时执行唯一平台选择，输出所选清单摘要。
//	oci-pipeline gc --root DIR [--verbose]
//	    标记-清除无引用对象并打印报告。
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	var (
		err error
	)
	switch cmd {
	case "init":
		err = runInit(args)
	case "build":
		err = runBuild(args)
	case "push-index":
		err = runPushIndex(args)
	case "index":
		err = runIndex(args)
	case "resolve":
		err = runResolve(args)
	case "gc":
		err = runGC(args)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: oci-pipeline <init|build|index|push-index|resolve|gc> [flags]")
}

// rootFlag 注册各子命令共用的 --root。
func rootFlag(fs *flag.FlagSet) *string {
	return fs.String("root", "./.ociwork", "本地工作根目录（CAS+仓库）")
}
