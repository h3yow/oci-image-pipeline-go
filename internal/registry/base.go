package registry

import (
	"errors"

	"github.com/h3yow/oci-image-pipeline-go/internal/digest"
)

// refBase 记录事务开始时引用的基线：root 为其当前指向对象的摘要，
// size 为声明大小；exists=false 表示引用尚不存在。
type refBase struct {
	root   digest.Digest
	size   int64
	exists bool
}

// readBaseLocked 在持锁条件下读取引用基线。引用损坏/超版本时返回其原始
// 错误（调用方据此归类为状态冲突）。
func (r *Registry) readBaseLocked(ref string) (refBase, error) {
	rec, err := r.loadRecord(ref)
	if err != nil {
		if errors.Is(err, ErrNoSuchRef) {
			return refBase{exists: false}, nil
		}
		return refBase{}, err
	}
	return refBase{root: rec.Target.Digest, size: rec.Target.Size, exists: true}, nil
}

// currentRootLocked 返回引用此刻指向的根摘要及其存在性。
func (r *Registry) currentBaseLocked(ref string) (refBase, error) {
	return r.readBaseLocked(ref)
}
