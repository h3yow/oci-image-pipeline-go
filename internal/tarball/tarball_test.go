package tarball

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"
)

func TestBuildDeterministic(t *testing.T) {
	files := []File{
		{Path: "b.txt", Data: []byte("second"), Mode: 0o600},
		{Path: "dir/a.txt", Data: []byte("first")},
	}
	first, err := Build(files)
	if err != nil {
		t.Fatal(err)
	}
	// 打乱输入顺序、重复构建：字节必须一致。
	again, err := Build([]File{files[1], files[0]})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入路径顺序 [b.txt,dir/a.txt] 与 [dir/a.txt,b.txt]，输出长度=%d", len(first))
	if !bytes.Equal(first, again) {
		t.Fatal("判定依据: 相同输入不同顺序产生了不同 tar 字节")
	}
	// 校验头部时间戳均为零、路径已排序。
	tr := tar.NewReader(bytes.NewReader(first))
	var names []string
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.ModTime.Unix() != 0 || hdr.Uid != 0 || hdr.Gid != 0 || hdr.Uname != "" || hdr.Gname != "" {
			t.Fatalf("条目 %s 含易变元数据: mod=%v uid=%d uname=%q", hdr.Name, hdr.ModTime, hdr.Uid, hdr.Uname)
		}
		names = append(names, hdr.Name)
	}
	if strings.Join(names, ",") != "dir/,b.txt,dir/a.txt" {
		t.Fatalf("条目排序异常: %v", names)
	}
}

func TestBuildContentDistinguishes(t *testing.T) {
	a, _ := Build([]File{{Path: "f", Data: []byte("x")}})
	b, _ := Build([]File{{Path: "f", Data: []byte("y")}})
	c, _ := Build([]File{{Path: "g", Data: []byte("x")}})
	t.Logf("内容差异->%t；路径差异->%t", !bytes.Equal(a, b), !bytes.Equal(a, c))
	if bytes.Equal(a, b) || bytes.Equal(a, c) {
		t.Fatal("不同内容或不同路径必须产生不同 tar")
	}
}

func TestBuildRejectsUnsafePath(t *testing.T) {
	for _, p := range []string{"../escape", "/abs", "a/../../b"} {
		if _, err := Build([]File{{Path: p, Data: nil}}); err == nil {
			t.Fatalf("输入 %q 必须被拒绝", p)
		} else {
			t.Logf("输入 %q -> %v", p, err)
		}
	}
}
