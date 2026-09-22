package manifest

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/cas"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

func layerDesc(b byte) Descriptor {
	data := []byte{b}
	return Descriptor{MediaType: MediaTypeLayer, Digest: digest.FromBytes(data), Size: 1}
}

func configDesc() Descriptor {
	data := []byte(`{"architecture":"amd64","os":"linux"}`)
	return Descriptor{MediaType: MediaTypeConfig, Digest: digest.FromBytes(data), Size: int64(len(data))}
}

func TestCanonicalJSONStableAcrossKeyOrder(t *testing.T) {
	a, err := CanonicalJSON(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalJSON(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("输入键序 {b,a} 与 {a,b} -> 规范字节 %q", a)
	if string(a) != string(b) || string(a) != `{"a":1,"b":2}` {
		t.Fatalf("判定依据: 规范序列化不稳定: %q vs %q", a, b)
	}
	if digest.FromBytes(a) != digest.FromBytes(b) {
		t.Fatal("相同语义内容摘要必须一致")
	}
}

func TestParseKindAcceptsSupported(t *testing.T) {
	m := &Manifest{Config: configDesc(), Layers: []Descriptor{layerDesc('x')}}
	data, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	mt, err := ParseKind(data)
	if err != nil || mt != MediaTypeManifest {
		t.Fatalf("清单识别失败: mt=%q err=%v", mt, err)
	}
	t.Logf("输入清单字节 -> 判定 mediaType=%s", mt)

	idx := &Index{Manifests: []Descriptor{{
		MediaType: MediaTypeManifest,
		Digest:    digest.FromBytes([]byte("m")),
		Size:      1,
		Platform:  &Platform{OS: "linux", Architecture: "amd64"},
	}}}
	idata, err := EncodeIndex(idx)
	if err != nil {
		t.Fatal(err)
	}
	mt, err = ParseKind(idata)
	if err != nil || mt != MediaTypeIndex {
		t.Fatalf("索引识别失败: mt=%q err=%v", mt, err)
	}
	t.Logf("输入索引字节 -> 判定 mediaType=%s", mt)
}

func TestParseKindRejectsUnsupportedFormats(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"docker schema2 manifest", `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"x","digest":"sha256:` + zeros() + `","size":1},"layers":[{"mediaType":"y","digest":"sha256:` + zeros() + `","size":1}]}`},
		{"docker schema1", `{"schemaVersion":1,"name":"x","tag":"latest","architecture":"amd64","fsLayers":[]}`},
		{"unknown oci version", `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v2+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:` + zeros() + `","size":1},"layers":[]}`},
		{"unknown schemaVersion", `{"schemaVersion":3,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:` + zeros() + `","size":1},"layers":[]}`},
		{"empty", ``},
		{"not json", `hello`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseKind([]byte(tc.doc))
			t.Logf("输入 %q -> err=%v", tc.doc, err)
			if err == nil {
				t.Fatalf("判定依据: 不受支持/非法格式必须被拒绝")
			}
			if !errors.Is(err, ErrUnsupportedFormat) && !errors.Is(err, ErrMalformed) {
				t.Fatalf("错误必须归类为 ErrUnsupportedFormat 或 ErrMalformed，got %v", err)
			}
		})
	}
}

func TestDefaultSemanticsStable(t *testing.T) {
	// mediaType 缺失 + schemaVersion 缺失：按结构推断为清单、版本按 2。
	doc := `{"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:` + zeros() + `","size":1},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:` + zeros() + `","size":1}]}`
	mt, err := ParseKind([]byte(doc))
	t.Logf("输入(缺 mediaType/schemaVersion) -> mt=%s err=%v", mt, err)
	if err != nil || mt != MediaTypeManifest {
		t.Fatalf("缺省字段的稳定解释失败: mt=%q err=%v", mt, err)
	}
	m, err := ParseManifest([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if m.SchemaVersion != SchemaVersion {
		t.Fatalf("schemaVersion 缺省应为 %d，got %d", SchemaVersion, m.SchemaVersion)
	}
	t.Logf("判定: schemaVersion 缺省=%d，mediaType 按结构推断为清单", m.SchemaVersion)
}

func TestMediaTypeStructureConflictRejected(t *testing.T) {
	// 声明为清单，结构却是索引（manifests 字段）——不得猜测。
	doc := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","manifests":[]}`
	_, err := ParseKind([]byte(doc))
	t.Logf("输入(声明清单但结构为索引) -> err=%v", err)
	if !errors.Is(err, ErrUnsupportedFormat) && !errors.Is(err, ErrMalformed) {
		t.Fatalf("声明与结构冲突必须拒绝，got %v", err)
	}
}

func TestUnknownFieldsIgnoredForwardCompat(t *testing.T) {
	// 前向新增字段：读入忽略，不影响既有内容判定。
	doc := `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","futureField":{"x":1},"config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":"sha256:` + zeros() + `","size":1},"layers":[{"mediaType":"application/vnd.oci.image.layer.v1.tar","digest":"sha256:` + zeros() + `","size":1}]}`
	m, err := ParseManifest([]byte(doc))
	t.Logf("输入(含未知字段 futureField) -> err=%v", err)
	if err != nil {
		t.Fatalf("未知字段必须被忽略（前向兼容），got %v", err)
	}
	if len(m.Layers) != 1 {
		t.Fatal("已知字段解析受影响")
	}
}

func TestIndexDuplicatePlatformRejected(t *testing.T) {
	p := Platform{OS: "linux", Architecture: "amd64"}
	idx := &Index{Manifests: []Descriptor{
		{MediaType: MediaTypeManifest, Digest: digest.FromBytes([]byte("a")), Size: 1, Platform: &p},
		{MediaType: MediaTypeManifest, Digest: digest.FromBytes([]byte("b")), Size: 1, Platform: &Platform{OS: "linux", Architecture: "amd64"}},
	}}
	_, err := EncodeIndex(idx)
	t.Logf("输入(索引含重复平台 linux/amd64 x2) -> err=%v", err)
	if !errors.Is(err, ErrDuplicatePlatform) {
		t.Fatalf("重复平台必须归类 ErrDuplicatePlatform，got %v", err)
	}
}

func TestPutGetManifestRoundTrip(t *testing.T) {
	store, err := cas.NewStore(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatal(err)
	}
	m := &Manifest{Config: configDesc(), Layers: []Descriptor{layerDesc('q')}}
	data, err := EncodeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	desc, err := PutManifest(context.Background(), store, data)
	if err != nil {
		t.Fatal(err)
	}
	mt, back, err := GetManifest(context.Background(), store, desc.Digest)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("写入清单摘要=%s，读回 mediaType=%s，字节一致=%v", desc.Digest, mt, string(back) == string(data))
	if mt != MediaTypeManifest || string(back) != string(data) {
		t.Fatal("清单读写往返不一致")
	}
	// 重复写入相同清单：描述符一致（内容寻址去重）。
	desc2, err := PutManifest(context.Background(), store, data)
	if err != nil {
		t.Fatal(err)
	}
	if desc2.Digest != desc.Digest {
		t.Fatal("相同清单重复写入必须得到相同摘要")
	}
}

func zeros() string {
	out := ""
	for i := 0; i < 64; i++ {
		out += "0"
	}
	return out
}
