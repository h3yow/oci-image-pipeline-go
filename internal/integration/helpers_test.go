package integration_test

import (
	"bytes"
	"io"

	"github.com/h3yow/oci-image-pipeline-go/internal/assemble"
)

func assembleSpec(osName, arch string, files []assemble.File) assemble.ImageSpec {
	return assemble.ImageSpec{
		Config: map[string]any{"os": osName, "architecture": arch},
		Layers: []assemble.LayerSpec{{Name: "rootfs", Files: files}},
	}
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
