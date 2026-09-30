# oci-image-pipeline-go

一套**完全本地运行**的容器镜像组装与分发流程，用本地目录模拟镜像仓库
（registry）与构建上下文（build context），不依赖任何外部服务或守护进程。

目标：让镜像内容在**重复构建、格式演进与并发访问**下得到一致、可解释、
可复现的结论，并对每一类失败给出**可区分、可判定**的原因。

## 目录布局

```
<root>/
├── blobs/sha256/<hex>   内容寻址对象（层、config、清单、索引），不可变
├── tmp/                 写入中的临时文件（原子 rename 提交）
└── refs/<name...>/<tag> 命名引用文件（name:tag），原子替换
```

包结构：

| 包 | 职责 |
| --- | --- |
| `internal/digest` | 摘要口径：解析、计算、校验 |
| `internal/cas` | 本地内容寻址存储：原子写入、去重、完整性三分类、pin 锁表 |
| `internal/tarball` | 确定性（可复现）tar 归档 |
| `internal/manifest` | 清单/索引规范序列化、格式探测与兼容边界 |
| `internal/assemble` | 层、config、清单的可重复组装 |
| `internal/registry` | 命名引用、多架构索引、平台选择、可达闭包、提交门 |
| `internal/commit` | 「组装并挂到引用」的单次原子提交编排 |
| `internal/verify` | 全库逐对象完整性校验（三分类报告） |
| `internal/gc` | 不可达对象回收与陈旧临时文件清理 |
| `cmd/pipeline` | 本地验证用 CLI |

## 一、内容寻址与摘要口径

- 算法**固定 sha256**，摘要字符串规范形态为 `sha256:<64 个小写十六进制>`。
  其他算法、错误长度、非十六进制、大写十六进制一律
  `digest.ErrInvalid`，不做猜测或映射。
- 摘要一律对**对象原始字节**计算：
  - 层：确定性 tar 字节（见下）；
  - config：规范 JSON（map 键按字典序、无空白、忽略易变的 `created`）；
  - 镜像摘要：清单的**规范 JSON 字节**（`manifest.CanonicalJSON`）。
- 确定性 tar：条目按相对路径字典序；所有时间戳固定 Unix 纪元 0；
  uid/gid 固定 0、无 uname/gname；只写目录与常规文件；权限只保留低 9 位。
  因此同一构建上下文，无论输入顺序如何，**层字节恒定**。
- 去重：对象由摘要寻址。`Put` 时若相同摘要的完整对象已存在，**不重复写入**
  （进程内同摘要写串行化，先到者落地，后到者直接返回）。
- 完整性口径只有一个判定入口：`cas.Store.Inspect(ctx, digest, declaredSize)`。
  读取、回收、全库校验、提交前检查全部经由它，结论在整条链路上完全一致。
  对每个对象给出**三类互斥结论**，可用 `errors.Is` 精确区分：
  1. **对象根本不存在**：`cas.ErrNotFound`；
  2. **对象在，但长度/大小对不上**（疑似被截断或追加）：
     `cas.ErrCorrupt` 且 `cas.ErrSizeMismatch`——只要文件长度与描述符
     声明的 `size` 不一致即归此类，不再读取内容；
  3. **对象在、长度也对，但内容被改过**：`cas.ErrCorrupt` 且
     `cas.ErrDigestMismatch`（实测摘要与文件名摘要不符）。
  - `cas.ErrCorrupt` 是第 2、3 类的总类；损坏**绝不静默退化为未命中**，
    任何一处发现问题都必须对上其中一类，不允许含糊地报一句“坏了”。
  - `declaredSize < 0` 表示此处没有大小声明（如无引用的孤立对象），
    此时只能按摘要校验，损坏统一归为 `ErrDigestMismatch`。
- 完整性校验在四处发生，且共用上述口径：
  1. **写入**：先落临时文件、边写边算，摘要与声明不符返回
     `cas.ErrDigestMismatch`，对象**绝不落地**；同名对象若已损坏，
     `Put` 直接报 `ErrCorrupt`，**不静默覆盖**（修复需先 `Delete`
     再重写），防止掩盖被引用对象的完整性问题；
  2. **读取**（`Get/Stat/Exists`）：打开前经 `Inspect` 全量校验；
     流式读到 EOF 时二次复核；
  3. **回收**：删除前求可达闭包，闭包中每个对象带描述符声明大小经
     `Inspect` 校验，缺任一对象或任一类损坏都**整轮中止**，绝不误删；
  4. **提交前检查**（`registry.VerifyClosure`，见第三节）：引用发布前
     整棵对象树（索引→每个清单→config/每个层）逐对象过 `Inspect`。
- 全库校验由 `internal/verify` 提供：`verify.Run` 枚举全部对象，对**每个
  对象**给出 `ok / missing / size-mismatch / digest-mismatch` 结论
  （含声明大小、实测长度与原始错误），不会在第一个坏对象处中止，便于
  整库导出排查；被清单/索引声明但不存在的对象记为 `missing`。
- 原子性：写入走“同卷临时文件 → fsync → 原子 rename”，读者只会看到
  “没有对象”或“完整对象”，不存在半写入可见。

## 二、清单与索引的兼容范围

只支持两种 OCI v1 媒体类型，**超出范围明确拒绝**：

- `application/vnd.oci.image.manifest.v1+json`（单镜像清单）
- `application/vnd.oci.image.index.v1+json`（多架构索引）

对象类型：

- config：`application/vnd.oci.image.config.v1+json`
- 层：`application/vnd.oci.image.layer.v1.tar`

规则：

- 其他 mediaType（Docker v2 schema1/schema2、未知 OCI 版本等）或未知
  `schemaVersion` → `manifest.ErrUnsupportedFormat`；JSON 无法解析或结构
  自相矛盾（如声明清单却含 `manifests` 字段）→ `manifest.ErrMalformed`。
- 稳定缺省语义：`schemaVersion` 缺失按 **2** 解释；`mediaType` 缺失按
  文档结构推断（有 `manifests` 即索引，有 `config/layers` 即清单），
  声明与结构冲突则拒绝。
- 前向新增字段：读入时**忽略未知字段**，写出只输出已知字段——新旧版本
  对“相同语义内容”始终得到相同摘要。
- 描述符的 digest/size/mediaType 三要素按位置强校验（config 位必须是
  config 类型，layers 位必须是层类型……），**不同格式标识绝不混用**。

### 引用（name:tag）的格式版本

引用文件含 `formatVersion`：

- 字段缺失：按 **1** 解释（兼容最早、无版本字段的旧引用）；
- `1`：当前结构；
- `> 1`：来自更新格式，返回
  `registry.ErrUnsupportedReferenceVersion` **明确拒绝**，绝不猜测字段。
- 引用文件损坏 → `registry.ErrCorruptRef`，不伪装成“引用不存在”。

### 多架构选择（不任选）

- 同一引用关联一个索引，每个平台（os/arch/variant）至多一个清单；
  追加重复平台 → `registry.ErrDuplicatePlatform`。
- `SelectPlatform` 的结论只有三种，且可区分：
  - 恰好 1 个匹配：返回该清单；
  - 0 个：`registry.ErrNoMatch`；
  - 多于 1 个：`registry.ErrAmbiguous`（例如只请求 `linux/arm` 而索引中
    同时存在 v7/v8），**禁止任选**；需要精确结果时必须显式给出 variant。
- 对清单（而非索引）引用做平台选择 → `registry.ErrWrongKind`。

## 三、提交语义（把组装好的镜像挂到引用上）

“组装一个新镜像并把它挂到引用上”由 `internal/commit` 编排，对外表现为
**一次提交**：要么引用指向一棵从头到尾都完整可用的对象树，要么这次动作
整体失败、仓库看起来跟没动过一样。不存在指向不存在对象或坏数据的引用，
外部也读不到挂了一半的中间状态。

### 什么时候算成功

一次提交（`commit.Committer.Commit` / `Publish`，底层即
`registry.Tag` / `AttachPlatform`）在**同一个提交区间**
（`registry.BeginCommit`）内完成三步：

1. **组装**：写层、config、清单（内容寻址 + 去重，相同 spec 恒得相同摘要）；
2. **提交前完整性闸门**：`registry.VerifyClosure` 沿
   索引→每个清单→config/每个层逐对象走 `cas.Inspect`——
   索引里选中的每个清单、清单里的 config 和每个层都必须**存在、
   长度与描述符声明一致、摘要通过**；
3. **原子发布**：引用文件走同目录临时文件 + 原子 rename，引用要么仍是
   旧目标，要么已是通过闸门的新目标，没有中间态。

只有第 3 步完成才算成功；成功后任何读者解析该引用并沿闭包读取，都拿得到
完整对象。

### 失败会发生什么

- 任一步失败，引用**保持提交前状态**（不存在就仍不存在，存在就仍是旧目标），
  错误统一链 `commit.ErrCommitFailed`，并保留**可区分的具体原因**，
  可用 `errors.Is` 判定，绝不只给一句笼统的“提交失败”：
  - `cas.ErrNotFound`：闭包中某对象根本不存在；
  - `cas.ErrSizeMismatch`（同时链 `ErrCorrupt`）：某对象长度与声明不符；
  - `cas.ErrDigestMismatch`（同时链 `ErrCorrupt`）：某对象长度对但内容被改；
  - `registry.ErrDuplicatePlatform`：向索引追加了完全重复的平台；
  - `registry.ErrWrongKind` / `manifest.ErrUnsupportedFormat` 等：
    目标种类/格式不对。
- **失败可直接重来，且重试结果与上一次失败无关**：组装是确定性的
  （相同输入恒得相同摘要），写对象走 CAS 去重，失败提交不会把引用改到
  半成品上；失败期间产生的“无引用对象”不属任何闭包，下一轮回收即清掉，
  **不会越攒越多**。配合回收长期反复跑，磁盘占用只跟“当前还被引用着的
  镜像”有关，不随历史提交/失败次数增长。
- 修复路径明确：若失败原因是某个同名对象已损坏，`Put` 不会静默覆盖，
  需先 `Delete` 损坏对象再以正确内容重写，然后原样重试提交。

### 提交与回收为什么不会互相打穿

“对象已写完但引用尚未发布”是经典危险窗口：若此时回收，可能把新对象删掉，
随后引用挂上去就指向了缺失对象。为此提交区间与整轮回收**互斥**：

- `BeginCommit` 同时取得“引用变更互斥锁”与针对 `gc.Run` 的读锁；
  一次提交覆盖“组装 → 闸门 → 发布”的全过程；
- `gc.Run` 经 `registry.BeginGC/EndGC` 持写锁，因此：
  - 回收进行中，新的提交会等本轮结束；
  - 提交进行中，回收无法开始。

由此，回收求可达闭包时看到的引用状态要么是提交前、要么是提交后，永远不会
在“对象在写、引用没挂”的中间状态上做删除决策。纯读取（`Resolve` /
`SelectPlatform` / `ReferencedBlobs`）不需要该锁，对象一经引用即由可达
闭包长期保护。

## 四、回收与并发语义

### pin 锁表（进程内）

`cas.Locker` 登记每个对象的进行中读/写，以及每个临时文件的写入登记：

- 写入：`BeginWrite(digest)` … `EndWrite`；临时文件名**先 pin 再创建**；
- 读取：`BeginRead(digest)` … `EndRead`（覆盖完整读取周期）。

### 回收判定（每对象可解释）

一轮 `gc.Run`：

0. 经 `registry.BeginGC()` 取得回收锁：整轮回收与提交区间互斥（见第三节），
   保证闭包看到的引用状态不会在“写对象/挂引用”中途被观察到；
1. 枚举全部引用，求**可达闭包**（索引→清单→config/层），闭包内每个对象
   带描述符声明大小走 `cas.Inspect`；任一对象缺失（`ErrNotFound`）、
   长度不符（`ErrSizeMismatch`）或摘要不符（`ErrDigestMismatch`）
   即中止本轮；
2. 列出存储中全部对象；
3. 取 pin 快照，并在删除前对当前 pin 状态**双检**：
   - 可达 → `Kept(referenced)`；
   - 不可达但被 pin → `Kept(pinned)`；
   - 两者皆无 → `Deleted`；
4. 清理 `tmp/` 无主临时文件。

因此：

- **仍被引用或正在读写的对象不会被删除**；多轮并发回收删除同一对象时，
  后到者得到的“已不存在”被视为自然消失而非错误；
- **占用有界**：通过提交门（`commit` / `Tag` / `AttachPlatform`）发布的
  对象，在“组装→挂引用”全程受提交门保护，不会在引用尚未可见时被回收；
  提交失败/从未发布的新对象不属任何闭包，下一轮即回收。长期反复提交、
  失败、重试之后，占用只随“当前可达对象集”变化，不随历史写入/提交次数
  无界增长。

### 临时文件清理的正确性（不依赖 mtime）

清理顺序刻意为 **先 `ReadDir`，后取 pin 快照，再删除**，依据的不变量是
“写入方先 pin 临时文件名、再创建文件”：

- 列表之后才创建的文件不在候选集合内；
- 列表中已存在的活跃文件，其 pin 必然已登记、必能被之后的快照观察到。

> 实现中曾使用“mtime 早于本轮开始”作为陈旧判据，但在某些文件系统上
> mtime 可能与实际创建时间不一致，会误删活跃临时文件。现口径**完全不
> 依赖 mtime**，仅依据 pin 集合判定。并发压测（`-race -count=50`）稳定
> 通过。

## 五、本地复现与验证方法

需要 Go 1.25+。

```bash
# 全部单元测试（含日志中的输入内容与判定依据）
go test -v ./...

# 并发压力 + 竞态检测（提交 / 回收 / 校验 一起跑；回收/写入/索引追加）
go test -race -count=50 ./internal/gc/ ./internal/commit/ ./internal/registry/
go test -race ./...
```

CLI 手工验证（`build`/`attach` 现在都是一次原子提交）：

```bash
go build -o bin/pipeline ./cmd/pipeline

mkdir -p /tmp/ctx-amd/bin /tmp/ctx-arm/bin
echo APP-AMD64 > /tmp/ctx-amd/bin/app
echo APP-ARM64 > /tmp/ctx-arm/bin/app

pipeline init   /tmp/repo
pipeline build  /tmp/repo ns/base:1.0 /tmp/ctx-amd  --os linux --arch amd64
pipeline attach /tmp/repo ns/app:1.0  /tmp/ctx-amd  --os linux --arch amd64
pipeline attach /tmp/repo ns/app:1.0  /tmp/ctx-arm  --os linux --arch arm64
pipeline select  /tmp/repo ns/app:1.0 --os linux --arch arm64
pipeline resolve /tmp/repo ns/app:1.0

# 全库逐对象完整性校验：每个对象一行 OK/MISSING/TRUNC(长度不符)/TAMPER(摘要不符)
pipeline verify /tmp/repo
pipeline verify --json /tmp/repo   # 结构化导出，便于转出去排查

pipeline gc /tmp/repo             # 打印 visited/kept/deleted/staleTmp 判定明细
```

### 复现“三类完整性结论”

```bash
# 1) 正常提交两个平台后做基线校验（全部 OK）
pipeline verify /tmp/repo

# 2) 截断某个被引用层（把某个 blob 文件尾部删掉若干字节）：
#    verify 输出 TRUNC（size-mismatch）；gc 整轮中止；提交/读取报
#    ErrCorrupt+ErrSizeMismatch。
BLOB=$(ls /tmp/repo/blobs/sha256 | head -1)
cp /tmp/repo/blobs/sha256/$BLOB /tmp/$BLOB.bak
head -c $(( $(stat -f%z /tmp/repo/blobs/sha256/$BLOB 2>/dev/null || stat -c%s /tmp/repo/blobs/sha256/$BLOB) - 5 )) \
  /tmp/$BLOB.bak > /tmp/repo/blobs/sha256/$BLOB
pipeline verify /tmp/repo || true
pipeline gc /tmp/repo || true

# 3) 等长篡改（翻转一个字节，长度不变）：verify 输出 TAMPER（digest-mismatch）。
# 4) 修复（先删后写），再原样重试提交：引用恢复，verify 全部 OK。
rm /tmp/repo/blobs/sha256/$BLOB
cp /tmp/$BLOB.bak /tmp/repo/blobs/sha256/$BLOB
pipeline verify /tmp/repo
```

### 复现“失败可重试、不累积垃圾”

```bash
# 反复提交 + 反复回收，观察对象数只随当前引用变化：
for i in $(seq 1 50); do
  pipeline build  /tmp/repo roll:latest /tmp/ctx-amd --os linux --arch amd64
  pipeline gc /tmp/repo >/dev/null
done
pipeline verify /tmp/repo
ls /tmp/repo/blobs/sha256 | wc -l   # 只与当前仍被引用的镜像集合有关
```

## 六、测试重点与失败归类

| 场景 | 关键用例 | 预期结论 |
| --- | --- | --- |
| 重复组装 | `assemble.TestRepeatedAssemblyIsIdentical` | 层/config/镜像摘要逐字节一致，二次全部 `reused=true` |
| 内容差异 | `assemble.TestContentChangeChangesDigest` | 不同层稳定区分，相同层跨镜像共享去重 |
| 完整性三分类 | `cas.TestInspectThreeClassClassification` | 不存在 `ErrNotFound`、长度不符 `ErrSizeMismatch`、内容改 `ErrDigestMismatch`，后两者均链 `ErrCorrupt`，互斥可区分 |
| 损坏/缺失旧口径 | `cas.TestMissingVsCorruptAreDistinct` | 缺失与损坏可区分；损坏 `Put` 不静默覆盖，先删后写可修复 |
| 全库逐对象校验 | `verify.TestVerifyClassifiesEachKind` | 同一份报告里截断/篡改/缺失逐对象分类，可导出 |
| 提交原子性 | `commit.TestCommitFailureIsAtomicAndRetriable` | 失败引用不动且原因可分类；修复后重试成功且摘要不变 |
| 提交缺失/篡改 | `commit.TestCommitMissingBlobClassified` | 缺失 `ErrNotFound`、等长篡改 `ErrDigestMismatch`、`ErrCommitFailed` |
| 提交门互斥 | `registry.TestCommitGateExcludesGC` | 提交区间内 gc 阻塞；发布后回收闭包对象不丢 |
| 旧引用格式 | `registry.TestLegacyRefWorksThroughCommit` | 无 `formatVersion` 旧引用经提交链路照常可用；超版本明确拒绝；损坏不伪装缺失 |
| 并发提交/回收/校验 | `commit.TestConcurrentCommitGCVerify` | 无悬挂引用/误删/坏对象；占用回落到可达集 |
| 失败不累积 | `commit.TestFailedCommitsDoNotAccumulate` | 未发布对象一轮回收清空，复测删除 0 |
| 提交闭环 | `integration.TestCommitAtomicityAndRetryFlow` | 多平台提交→损坏拒绝→三分类校验→gc 中止→修复重试→占用收敛 |
| 格式不兼容 | `manifest.TestParseKindRejectsUnsupportedFormats` | 旧/未知格式 `ErrUnsupportedFormat`/`ErrMalformed` |
| 多架构歧义 | `registry.TestVariantAmbiguityIsRejected` | 0 个 `ErrNoMatch`、多个 `ErrAmbiguous`、重复 `ErrDuplicatePlatform` |
| 并发回收 | `gc.TestConcurrentGCAndWrites` | 无半写入/摘要错位；引用对象不丢；占用回落到可达集 |
| 全链路 | `integration.TestEndToEnd` | 组装→索引→选择→回收→损坏中止的完整闭环 |

所有测试日志均打印**输入内容（摘要/平台/格式串）与判定依据**，
便于复核结论的可复现性。
