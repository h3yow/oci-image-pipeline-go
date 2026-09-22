package descriptor

import (
	"errors"
	"testing"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

func digestVal(t *testing.T) digest.Digest {
	t.Helper()
	return digest.FromBytes([]byte("x"))
}

func TestPlatformMatchSemantics(t *testing.T) {
	cases := []struct {
		name   string
		query  Platform
		target Platform
		want   bool
	}{
		{"exact", Platform{OS: "linux", Architecture: "amd64"}, Platform{OS: "linux", Architecture: "amd64"}, true},
		{"case-insensitive", Platform{OS: "LINUX", Architecture: "AMD64"}, Platform{OS: "linux", Architecture: "amd64"}, true},
		{"variant exact", Platform{OS: "linux", Architecture: "arm", Variant: "v8"}, Platform{OS: "linux", Architecture: "arm", Variant: "v8"}, true},
		{"variant wildcard query", Platform{OS: "linux", Architecture: "arm"}, Platform{OS: "linux", Architecture: "arm", Variant: "v7"}, true},
		{"variant required target empty", Platform{OS: "linux", Architecture: "arm", Variant: "v8"}, Platform{OS: "linux", Architecture: "arm"}, false},
		{"variant mismatch", Platform{OS: "linux", Architecture: "arm", Variant: "v8"}, Platform{OS: "linux", Architecture: "arm", Variant: "v7"}, false},
		{"arch mismatch", Platform{OS: "linux", Architecture: "amd64"}, Platform{OS: "linux", Architecture: "arm64"}, false},
		{"os mismatch", Platform{OS: "darwin", Architecture: "amd64"}, Platform{OS: "linux", Architecture: "amd64"}, false},
		{"empty query", Platform{}, Platform{OS: "linux", Architecture: "amd64"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.query.Match(tc.target)
			t.Logf("query=%q target=%q -> %v (want %v)", tc.query, tc.target, got, tc.want)
			if got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestPlatformStableString(t *testing.T) {
	p := Platform{OS: "LINUX", Architecture: "AMD64", Variant: "V2"}
	if got, want := p.String(), "linux/amd64/v2"; got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDescriptorValidate(t *testing.T) {
	valid := Descriptor{MediaType: MediaTypeLayerTar}
	if err := valid.Validate(); !errors.Is(err, ErrEmptyDigest) {
		t.Fatalf("empty digest: want ErrEmptyDigest, got %v", err)
	}
	noType := Descriptor{Digest: digestVal(t)}
	if err := noType.Validate(); !errors.Is(err, ErrEmptyMediaType) {
		t.Fatalf("empty mediaType: want ErrEmptyMediaType, got %v", err)
	}
	negative := Descriptor{MediaType: MediaTypeLayerTar, Digest: digestVal(t), Size: -1}
	if err := negative.Validate(); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("negative size: want ErrInvalidSize, got %v", err)
	}
	good := Descriptor{MediaType: MediaTypeLayerTar, Digest: digestVal(t), Size: 10}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid descriptor rejected: %v", err)
	}
}
