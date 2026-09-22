// Command oci-pipeline 是本地可运行的容器镜像组装与分发演示/验证入口。
//
// 子命令：
//
//	oci-pipeline init  --root DIR
//	    初始化本地 CAS 与仓库目录。
//	oci-pipeline build --root DIR --context DIR --name NAME:TAG [--os linux] [--arch amd64]
//	    组装镜像并打标签。
//	oci-pipeline push  --root DIR --name NAME:TAG --index DIGEST
//	    将已存在的索引（按摘要）绑到标签（多架构场景）。
//	oci-pipeline resolve --root DIR --name NAME:TAG [--os linux --arch amd64 [--variant V]]
//	    解析引用；给出平台时执行唯一平台选择。
//	oci-pipeline gc    --root DIR
//	    标记-清除无引用对象并打印报告。
package main

import (
	"flag"
	"fmt"
	"os"
)

// 全局标志（各子命令共用）。
var rootDir string

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.StringVar(&rootDir, "root", "./.ociwork", "本地工作根目录（CAS+仓库）")
	_ = fs.Parse(os.Args[2:])

	var err error
	switch cmd {
	case "init":
		err = cmdInit(fs.Args())
	case "build":
		err = cmdBuild(fs.Args())
	case "push-index":
		err = cmdPushIndex(fs.Args())
	case "resolve":
		err = cmdResolve(fs.Args())
	case "gc":
		err = cmdGC(fs.Args())
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: oci-pipeline <init|build|push-index|resolve|gc> [flags]")
}

func cmdInit(args []string) error  { return nil }
func cmdBuild(args []string) error { return nil }
func cmdPushIndex(args []string) error { return nil }
func cmdResolve(args []string) error { return nil }
func cmdGC(args []string) error  { return nil }
