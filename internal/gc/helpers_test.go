package gc

import "github.com/h3yow/oci-image-pipeline-go/internal/digest"

func toDigest(v interface{ String() string }) digest.Digest {
	if d, ok := v.(digest.Digest); ok {
		return d
	}
	return digest.Digest{}
}
