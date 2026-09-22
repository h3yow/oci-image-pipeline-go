# oci-image-pipeline-go

一套**本地可运行**的容器镜像组装与分发流程：用本地目录同时模拟内容寻址
存储（镜像仓库的 blob 端）与标签引用（registry 的 tag 端），用本地目录
作为构建上下文。目标是让镜像内容在**重复构建、格式演进与并发访问**下的
结论一致、可复现、可解释。

无外部依赖，仅使用 Go 标准库；Go >= 1.25。

---

## 1. 目录结构与包职责

```
internal/
  digest/     摘要类型：仅 sha256，规范字符串 sha256:<64位小写hex>
  descriptor/ 内容描述符 Descriptor 与平台 Platform（含匹配语义）
  store/      本地内容寻址存储（CAS）：原子写、去重、读取校验
  manifest/   镜像配置 / v1 清单 / 旧版 v0 清单 / 多架构索引的规范编解码
  assembler/  从构建上下文生成确定性 tar 层、配置、清单
  registry/   name:tag 引用、原子标签文件、按平台选择、旧引用只读兼容
  gc/         标记-清除：引用闭包可达性、保守回收、空间上界
  integration/ 端到端测试（重复组装/损坏归类/多架构歧义/并发回收）
cmd/oci-pipeline/  本地 CLI
scripts/demo.sh    端到端冒烟脚本
```

磁盘布局（`--root` 指向的工作目录）：

```
<root>/blobs/sha256/<hex>   内容寻址对象（层/配置/清单/索引）
<root>/tmp/                 写入用临时目录（rename 前不可见）
<root>/registry/tags/<name>/<tag>.json   标签记录（原子替换）
```

---

## 2. 内容寻址与摘要口径（稳定约定）

1. **算法唯一**：仅支持 `sha256`。摘要的规范形式是
   `sha256:<64 个小写十六进制字符>`。其它算法（sha512 等）、大写 hex、
   长度不对或缺冒号一律拒绝（`ErrUnsupportedAlgorithm` /
   `ErrInvalidEncoding`），从标识层面杜绝不同算法混用。
2. **对象命名即摘要**：blob 文件名为其内容摘要。写入流程为
   “写临时文件 → 同步计算 sha256 → fsync → 锁内去重检查 → 原子 rename”。
   相同内容命中已存在对象时**不重复写盘**（`Put` 返回既有摘要与大小）。
3. **摘要以规范化字节为准**：配置、清单、索引都经过确定性 JSON 编码
   （字段按 UTF-8 键名升序、无多余空白）。相同逻辑内容 ⇒ 相同字节 ⇒
   相同摘要。层为规范化 unix tar：条目按路径字典序、固定 mtime
   （2020-01-01T00:00:00Z）、Uid/Gid=0、只保留类型/权限/内容，因此
   上下文文件在磁盘上的 mtime 不影响层摘要。
4. **层 digest 与 diff id 分字段存放**：本实现层为未压缩 tar，二者字节
   口径相同（均为未压缩 tar 的 sha256），但层摘要在 manifest 中、
   diff id 在 config 的 `rootfs.diff_ids` 中，语义标识不混用。
5. **读取必校验**：按描述符读取时（`OpenExpected`）同时校验
   *声明大小*与*内容摘要*。三类失败严格区分，互不混淆：

   | 现象 | 错误 | 说明 |
   |---|---|---|
   | 对象不存在 | `store.ErrNotFound` | 明确的未命中 |
   | 被截断/被追加（大小与声明不符） | `store.ErrSizeMismatch` | 打开即拒绝 |
   | 同长度篡改（摘要不符） | `store.ErrDigestMismatch` | EOF 校验拒绝 |

   损坏内容**不会**被静默当作未命中，也**不会**在不报错的情况下返回给
   调用方；GC 遇到“被引用对象损坏”会**中止**，而不是把它当垃圾清掉。

---

## 3. 清单与索引的兼容范围

### 3.1 清单版本

| 版本 | mediaType | 读 | 写 |
|---|---|---|---|
| v1（当前） | `application/vnd.oci.image.manifest.v1+json` | ✅ | ✅ |
| v0（旧版） | `application/vnd.example.manifest.v0+json` | ✅ 只读兼容 | ❌ 禁止新写 |
| 其它版本 / 未知或混用 mediaType | — | ❌ 明确拒绝 `ErrUnsupportedFormat` | ❌ |

- **旧引用兼容**：历史遗留的 v0 标签仍可解析/拉取（透明读），解码出的
  内存表示 `Version==Version0`；但向仓库新推 v0 返回
  `registry.ErrLegacyRejected`，也无法再编码为 v1 写出。
- **字段缺省语义确定**：v0 无 `config` ⇒ 解码后 `Config == nil`（不伪造
  描述符）；缺失的 `annotations` ⇒ 空 map（非 nil）。因 v0 没有平台
  元数据，对其做“按平台拉取”明确报 `ErrPlatformMissing`，不会任选。
- **未知字段前向兼容**：JSON 中出现的未知字段在解码时保留，规范化重
  编码时按键序稳定保留；规范化编码幂等（编解码往返后字节不变）。
- v1 结构若挂着不相称的 mediaType，或 mediaType/version 互相矛盾，按
  `ErrUnsupportedFormat` 拒绝——不同格式标识不得混用。

### 3.2 多架构索引

- 索引（`...index.v1+json`）的每个条目都必须带 `platform`；**平台缺失**
  在编码/解码阶段即报 `ErrPlatformMissing`。
- **完全相同的平台条目**（小写归一化后）报 `ErrDuplicatePlatform`；
  `"LINUX/AMD64"` 与 `"linux/amd64"` 视为同一平台。
- 选择规则（`manifest.SelectPlatform`，确定性、不任选）：
  1. `os` 与 `architecture` 必须相等；
  2. 请求带 `variant` 时要求精确相等；请求 `variant` 为空表示“不约束
     变体”；
  3. 无任何匹配 ⇒ `ErrPlatformNotFound`；
  4. 在“不约束变体”下命中多个不同变体 ⇒ `ErrAmbiguousPlatform`
     （错误信息附带候选摘要，便于解释）。
- 标签记录带 `kind`（`manifest` / `index`）。把索引当清单拉取返回
  `ErrKindMismatch`，防止两类标识混用。

---

## 4. 回收（GC）与并发语义

### 4.1 标记-清除

- 根：仓库内全部标签记录。可达闭包为
  `tag → index → manifest(允许 v0) → config(v1) → layers`。
- 标记阶段对闭包中**每个对象**做大小+摘要校验；任一被引用对象损坏即
  中止本次 GC（保守失败，绝不误删）。
- 清除只删除不在可达闭包中的对象；仍被任意标签（含经索引间接、或与其
  它镜像共享层）引用的层**绝不会**被删除。
- **空间上界**：占用随“当前引用闭包”增长，而不随历史写入总量增长。
  长期反复发布同一标签、周期性 GC，稳定后目录只保留当前闭包对象
  （测试中 12 版历史回收后仅剩 3 个 blob：manifest/config/layer）。
- 仓库没有任何标签时拒绝整库清除（`gc.ErrNoTags`），避免删掉正在构建、
  尚未发布的对象。

### 4.2 并发

- 单进程内：CAS 的 `Put/Delete` 由互斥锁串行；blob 与标签文件都通过
  “临时文件 + fsync + 原子 rename”发布，任何读者只能看到“完整旧版”
  或“完整新版”，不存在半写入可见或摘要错位。
- **发布/回收协调锁**（store 上的 `publishMu`）：
  - `Registry.Publish / Stage / PushManifest / PushIndex` 的发布临界区
    （写入层/配置/清单并使标签可见）持有读侧锁，多个发布可并行；
  - GC 的整段标记+清除持有写侧锁，与任何发布临界区互斥；
  - 纯读取不参与该锁，不影响并发读。
  因此 GC 无法观察到“对象已写、标签未见”的中间态，也就不会把发布进行
  中的层判为垃圾。
- 枚举标签期间某个标签被并发删除：跳过该消失的根（原子写入保证不会
  读到“写了一半”的标签文件）。
- 保障范围是单进程 + 本库 API 的并发；绕过本库直接改文件不在保证之列。

---

## 5. 本地验证

### 5.1 运行测试

```bash
go vet ./...
go test -race -count=1 ./...
```

测试以题目要求的五类场景为核心，且每个用例都会打印**输入内容**与
**判定依据**（摘要、错误归类、候选集合），失败原因可区分、结论可复现：

- 重复组装：`assembler`、`integration` 验证相同上下文重复构建摘要逐字节
  一致、层不重复写入、不同内容稳定区分；
- 摘要损坏：`store` / `registry` / `gc` 分别验证缺失、截断、同长度篡改
  被归类为 `ErrNotFound` / `ErrSizeMismatch` / `ErrDigestMismatch`；
- 格式不兼容：`manifest` 验证未知版本/混用 mediaType 拒绝、v0 只读、
  缺省字段稳定；
- 多架构歧义：`manifest` / `registry` / `integration` 验证缺失、重复、
  歧义三种拒绝与精确/变体裁决；
- 并发回收：`gc` 与 `integration` 在 `-race` 下并发组装、发布、取消发布、
  回收，验证已发布镜像与其共享层始终完整。

### 5.2 CLI 端到端

```bash
bash scripts/demo.sh                 # 使用临时工作目录
# 或手动：
go build -o bin/oci-pipeline ./cmd/oci-pipeline
bin/oci-pipeline init --root ./.ociwork
bin/oci-pipeline build --root ./.ociwork --context ./ctx --name app:v1 \
    --os linux --arch amd64
bin/oci-pipeline index --root ./.ociwork --name app:multi \
    --entry linux/amd64:sha256:<manifest-digest> \
    --entry linux/arm64:sha256:<manifest-digest>
bin/oci-pipeline resolve --root ./.ociwork --name app:multi --os linux --arch arm64
bin/oci-pipeline gc --root ./.ociwork --verbose
```

子命令：

| 命令 | 作用 |
|---|---|
| `init` | 初始化 CAS 与仓库目录 |
| `build` | 组装上下文为镜像并打标签（固定 created 口径，默认可重复） |
| `index` | 用多个 `os/arch[/variant]:sha256:...` 条目构建多架构索引并打标签 |
| `resolve` | 解析引用；带 `--os/--arch` 时执行唯一平台选择 |
| `gc` | 标记-清除并输出扫描/保留/删除数量与释放字节数 |

---

## 6. 设计取舍说明

- **层格式**：仅支持未压缩 tar（单一 mediaType）。引入压缩会产生
  “压缩字节摘要 vs diff id”两套口径，本实现将二者显式分字段存放，
  即便未来支持压缩也不混用标识。
- **确定性优先于镜像完全等价**：tar 中固定 mtime/uid/gid、不写扩展头，
  换取跨机器、跨时间的可重复摘要。
- **保守失败**：任何“无法确认完整性/可达性”的情形（对象读不全、枚举
  出错）都按“保留/中止”处理，宁可少删，不可错删或返回损坏内容。
