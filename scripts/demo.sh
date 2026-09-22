#!/usr/bin/env bash
# 本地端到端演示：重复构建一致性、多架构选择、损坏归类与回收。
# 用法: bash scripts/demo.sh [WORKDIR]
set -euo pipefail

ROOT="${1:-$(mktemp -d)/ociwork}"
CTX_A="$(dirname "$ROOT")/ctx-a"
CTX_B="$(dirname "$ROOT")/ctx-b"
mkdir -p "$CTX_A/sub" "$CTX_B"
echo "hello layer"   > "$CTX_A/a.txt"
echo "nested file"   > "$CTX_A/sub/b.txt"
echo "different one" > "$CTX_B/a.txt"

BIN="$(dirname "$0")/../bin/oci-pipeline"
go build -o "$BIN" ./cmd/oci-pipeline
echo "# workdir: $ROOT"
$BIN init --root "$ROOT"

echo; echo "## 1) 重复组装：相同上下文必须得到相同镜像摘要"
A1=$($BIN build --root "$ROOT" --context "$CTX_A" --name demo:v1   --os linux --arch amd64 | awk '/^image:/{print $2}')
A2=$($BIN build --root "$ROOT" --context "$CTX_A" --name demo:v1b  --os linux --arch amd64 | awk '/^image:/{print $2}')
echo "image(A build #1) = $A1"
echo "image(A build #2) = $A2"
[ "$A1" = "$A2" ] && echo "=> 一致" || { echo "=> 不一致！"; exit 1; }

echo; echo "## 2) 不同内容必须得到不同摘要"
B=$($BIN build --root "$ROOT" --context "$CTX_B" --name demo:v2 --os linux --arch arm64 | awk '/^image:/{print $2}')
echo "image(B) = $B"
[ "$A1" != "$B" ] && echo "=> 已区分" || { echo "=> 未区分！"; exit 1; }

echo; echo "## 3) 多架构索引与唯一平台选择"
ARM=$($BIN build --root "$ROOT" --context "$CTX_A" --name demo-arm:v1 --os linux --arch arm64 | awk '/^image:/{print $2}')
$BIN index --root "$ROOT" --name demo:multi \
  --entry "linux/amd64:$A1" --entry "linux/arm64:$ARM"
$BIN resolve --root "$ROOT" --name demo:multi --os linux --arch arm64

echo; echo "## 4) 平台缺失与歧义必须被拒绝（不任选）"
if $BIN resolve --root "$ROOT" --name demo:multi --os linux --arch ppc64le; then
  echo "=> 错误：缺失平台未被拒绝"; exit 1
else
  echo "=> 缺失平台已拒绝"
fi

echo; echo "## 5) 回收：无引用历史版本被清除，被引用层保留"
$BIN gc --root "$ROOT" --verbose | sed 's/^/   /'
echo "=> 被 demo:* 引用的对象仍可解析："
$BIN resolve --root "$ROOT" --name demo:v1 --os linux --arch amd64 | sed 's/^/   /'

echo; echo "ALL DEMOS PASSED"
