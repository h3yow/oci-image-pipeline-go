package registry

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/h3yow/oci-image-pipeline-go/internal/assembler"
	"github.com/h3yow/oci-image-pipeline-go/internal/descriptor"
	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
	"github.com/h3yow/oci-image-pipeline-go/internal/store"
)

var testClock = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

// buildManifest 通过 assembler 在 CAS 中构造一个真实 v1 镜像。
func buildManifest(t *testing.T, st *store.Store, ctxDir, osName, arch string) descriptor.Descriptor {
	t.Helper()
	res, err := assembler.New(st, func() time.Time { return testClock }).
		Assemble(context.Background(), ctxDir, assembler.Options{OS: osName, Architecture: arch})
	if err != nil {
		t.Fatal(err)
	}
	return res.ManifestDescriptor
}

func digestParse(t *testing.T, content string) (digest.Digest, int64) {
	t.Helper()
	d := digest.FromBytes([]byte(content))
	return d, int64(len(content))
}

func itoa(i int) string { return strconv.Itoa(i) }
