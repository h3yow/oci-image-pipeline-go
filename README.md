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
| `internal/cas` | 本地内容寻址存储：原子写入、去重、完整性校验、pin 锁表 |
| `internal/tarball` | 确定性（可复现）tar 归档 |
| `internal/manifest` | 清单/索引规范序列化、格式探测与兼容边界 |
| `internal/assemble` | 层、config、清单的可重复组装 |
| `internal/registry` | 命名引用、多架构索引、平台选择、可达闭包 |
| `internal/gc` | 不可达对象回收与陈旧临时文件清理 |
| `internal/verify` | 全库逐对象完整性巡检（ok/missing/truncated/tampered） |
| `internal/registry`（提交部分） | 原子提交：plan 事务、基线乐观并发、提交失败分类 |
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
- 完整性校验在三处发生：
  1. **写入**：先落临时文件、边写边算，摘要与声明不符返回
     `cas.ErrDigestMismatch`，对象**绝不落地**；
  2. **读取**：打开时全量校验；流式读完（EOF）时二次复核；
  3. **回收**：删除前判定可达性，闭包中的每个对象都按描述符声明的
     digest+size 做校验（见下），闭包无法完整解析时**整轮中止**，绝不误删；
  4. **提交前**：挂引用前对整个新闭包做同一套校验，任一对象不过则整次
     提交失败、引用保持不动（见“四、提交语义”）。

### 完整性三类结论（读取 / 校验 / 回收 / 提交前检查同一口径）

凡是拿得到描述符声明 `size` 的路径，都按**先比大小、再比摘要**给出三类
互斥结论，可由 `errors.Is` 精确区分，损坏绝不静默退化为未命中：

| 结论 | 哨兵 | 含义 |
| --- | --- | --- |
| 对象根本不存在 | `cas.ErrNotFound` | 引用/枚举期望它在，但没有该文件 |
| 大小对不上（截断/追加） | `cas.ErrSizeMismatch`（同时链 `cas.ErrCorrupt`） | 文件在，但实际长度 ≠ 声明 size，疑似被截断，不再猜测内容 |
| 大小一致但内容被改过 | `cas.ErrDigestMismatch`（同时链 `cas.ErrCorrupt`） | 长度相同，但算出的摘要 ≠ 声明摘要 |

- 统一入口是 `store.Check(ctx, digest, declaredSize)`；`wantSize < 0`
  表示调用方只有摘要（按摘要直读、全库裸枚举），此时只给“缺失/损坏”，
  不臆测是截断还是篡改。
- 引用闭包校验走 `registry.VerifyClosure`（只读）与
  `registry.VerifyClosurePinned`（带读 pin），索引→清单→config/每个层
  逐对象套用上面三类，且按位置核对类型（config 位必须是 config、layer
  位必须是层）。
- 全库巡检 `verify.Run`（CLI `pipeline verify`）**逐对象**给出结论，
  不因第一个问题中止：既枚举所有裸对象，也展开每个引用的闭包，输出
  `ok / missing / truncated / tampered` 与所在引用、角色（root/manifest/
  config/layers[i]），便于整批转出去排查。
- `cas.ErrInvalidDigest`：摘要标识本身非法（算法/格式不受支持），与
  “对象坏了”也是两回事。
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

## 三、回收与并发语义

### pin 锁表（进程内）

`cas.Locker` 登记每个对象的进行中读/写，以及每个临时文件的写入登记：

- 写入：`BeginWrite(digest)` … `EndWrite`；临时文件名**先 pin 再创建**；
- 读取：`BeginRead(digest)` … `EndRead`（覆盖完整读取周期）。

### 回收判定（每对象可解释）

一轮 `gc.Run`：

1. 枚举全部引用，求**可达闭包**（索引→清单→config/层），闭包内任一
   对象缺失/损坏即中止本轮；
2. 列出存储中全部对象；
3. 取 pin 快照，并在删除前对当前 pin 状态**双检**：
   - 可达 → `Kept(referenced)`；
   - 不可达但被 pin → `Kept(pinned)`；
   - 两者皆无 → `Deleted`；
4. 清理 `tmp/` 无主临时文件。

因此：

- **仍被引用或正在读写的对象不会被删除**；多轮并发回收删除同一对象时，
  后到者得到的“已不存在”被视为自然消失而非错误；
- **占用有界**：未发布引用的新对象在写入期间由 pin 保护，提交后若无人
  引用，下一轮即回收。长期写入后，占用只随“当前可达对象集”变化，不随
  历史写入总量无界增长。

### 临时文件清理的正确性（不依赖 mtime）

清理顺序刻意为 **先 `ReadDir`，后取 pin 快照，再删除**，依据的不变量是
“写入方先 pin 临时文件名、再创建文件”：

- 列表之后才创建的文件不在候选集合内；
- 列表中已存在的活跃文件，其 pin 必然已登记、必能被之后的快照观察到。

> 实现中曾使用“mtime 早于本轮开始”作为陈旧判据，但在某些文件系统上
> mtime 可能与实际创建时间不一致，会误删活跃临时文件。现口径**完全不
> 依赖 mtime**，仅依据 pin 集合判定。并发压测（`-race -count=50`）稳定
> 通过。

## 四、提交语义：组装并挂引用是一次原子提交

“把一个新组装好的镜像挂到引用上”对外是**一次提交（事务）**，与回收、
校验、其他组装并发时仍成立：

- **成功**：引用一定指向一个“从头到尾完整可用”的镜像——索引里选出的
  清单、清单里的 config 与每个层都存在、大小与摘要全部通过校验。
- **失败**：整次动作作废，仓库引用状态与没动过一样（原来指向谁还指向
  谁；原来不存在则仍不存在）。外部任何时刻都读不到“做了一半”的引用，
  也不会出现指向不存在对象或坏数据的引用。
- **失败原因可区分**（`errors.Is`），不是一句笼统失败：

  | 哨兵 | 含义（可继续下钻） |
  | --- | --- |
  | `registry.ErrCommitIntegrity` | 提交前完整性不过；链上再细分 `cas.ErrNotFound` / `ErrSizeMismatch` / `ErrDigestMismatch` |
  | `registry.ErrCommitInvalid` | 提交内容非法（描述符/平台/格式） |
  | `registry.ErrCommitState` | 引用状态冲突：向清单引用追加平台(`ErrWrongKind`)、平台重复(`ErrDuplicatePlatform`)、旧引用损坏(`ErrCorruptRef`)、超版本(`ErrUnsupportedReferenceVersion`) |
  | `registry.ErrCommitConflict`（链 `ErrCommitState`） | 乐观并发冲突：读到的索引基线在发布前被别的提交改走，本次未生效，应重读-重组装-重试 |

### 推荐入口：提交计划（无并发回收窗口）

`registry.CommitImagePlan` / `registry.CommitPlatformPlan` 接受一份
`ImagePlan`（config 规范字节 + 各层确定性 tar 字节 + 可选平台）：

1. 由字节算出整个闭包摘要（层/config/清单），**在写任何对象之前**先
   pin 全部摘要；
2. 逐个写入（Put 对已存在且完整者去重、对损坏者原子修复）；
3. 追加平台时，先 pin 旧索引闭包、读取基线，构造新索引后**先 pin 新索引
   摘要再落地**；
4. 对整个新闭包做完整性校验，并检查引用基线未被并发改走；
5. 全部通过才原子 rename 引用，最后释放 pin。

因此对象从“落地”起就受 pin 保护，rename 之后由新引用提供可达性，
**不存在“写好了但还没被引用、被并发回收删掉”的窗口**。

#### 提交与回收的读写门（防止过期可达集误删）

仅有对象级 pin 仍不足以覆盖一种交错：回收器**先**按旧索引算完可达集，
提交**随后**发布含新对象的新索引并释放 pin，回收器再拿着过期可达集去
删除，就可能删掉新引用刚需要的对象。为此 `cas.Locker` 提供一把读写门：

- 提交事务（plan 提交、attach、tag）在“校验 → 写 → 发布”期间持
  **读门**，多个提交彼此并发；
- 一轮回收在“计算可达闭包 → 列出对象 → 删除”期间持**写门（独占）**。

于是回收删除任一对象时，相关提交要么尚未发布（被门挡住，其对象还受
pin 保护），要么已发布（对象已在最新可达闭包中）。回收只在“扫描+删除”
这一段令提交短暂等待，临时文件清理不受影响。

> `CommitImage` / `CommitPlatform` / `AttachPlatform` 面向“对象已提前
> 写好”的调用方，事务内同样做闭包校验、pin 与基线检查；但调用方若无法
> 保证“写入→提交”之间没有并发回收，应改用上面的 plan 入口。
> `pipeline build/attach` CLI 已统一走 plan 入口。

### 失败可直接重试，占用有界

- 失败提交不改动引用；本次新写但未发布的对象只是**不可达 blob**，下一轮
  `gc` 即清理，不会越攒越多。
- 重试就是把同一套输入再提交一遍：Put 幂等去重、plan 纯由字节决定摘要，
  结果与“第一次就成功”完全一致，不依赖上一次失败的残留。
- 乐观冲突（`ErrCommitConflict`）是正常并发结果而非损坏：调用方重新读取
  当前索引、重新并入平台后重试即可，系统绝不静默覆盖他人提交。

## 五、本地验证方法

需要 Go 1.25+。

```bash
# 全部单元测试（含日志中的输入内容与判定依据）
go test -v ./...

# 并发压力 + 竞态检测（回收/写入/原子提交/全库校验一起跑）
go test -race -count=50 ./internal/gc/
go test -race -count=50 ./internal/registry/ -run TestConcurrentCommitGCVerify
go test -race ./...
```

CLI 手工验证：

```bash
go build -o bin/pipeline ./cmd/pipeline

mkdir -p /tmp/ctx-amd/bin /tmp/ctx-arm/bin
echo APP-AMD64 > /tmp/ctx-amd/bin/app
echo APP-ARM64 > /tmp/ctx-arm/bin/app

pipeline init   /tmp/repo
pipeline attach /tmp/repo ns/app:1.0 /tmp/ctx-amd  --os linux --arch amd64
pipeline attach /tmp/repo ns/app:1.0 /tmp/ctx-arm  --os linux --arch arm64
pipeline select /tmp/repo ns/app:1.0 --os linux --arch arm64
pipeline verify /tmp/repo     # 逐对象打印 OK/MISSING/TRUNCATED/TAMPERED
pipeline gc     /tmp/repo     # 打印 visited/kept/deleted/staleTmp 判定明细
pipeline resolve /tmp/repo ns/app:1.0
```

复现三类完整性结论（以截断为例）：

```bash
# 1) 正常提交一个镜像并回收，确保引用闭包稳定
pipeline attach /tmp/repo demo:v1 /tmp/ctx-amd --os linux --arch amd64
pipeline gc /tmp/repo >/dev/null
# 2) 取被引用层摘要（verify 输出中 role=layers[0] 的那一行）
d=$(pipeline verify /tmp/repo | awk '/layers\[0\]/{print $NF}' | sed 's/.*sha256://')
# 3) 截断到 20 字节，再巡检：引用闭包报 TRUNCATED，裸对象按摘要报 TAMPERED
head -c 20 /tmp/repo/blobs/sha256/$d > /tmp/repo/blobs/sha256/$d.cut \
  && mv /tmp/repo/blobs/sha256/$d.cut /tmp/repo/blobs/sha256/$d
pipeline verify /tmp/repo   # 非 0 退出，逐对象给出 missing/truncated/tampered
# 删除某被引用对象即 MISSING；等长改字节（保持长度）即 TAMPERED。
```

## 六、测试重点与失败归类

| 场景 | 关键用例 | 预期结论 |
| --- | --- | --- |
| 重复组装 | `assemble.TestRepeatedAssemblyIsIdentical` | 层/config/镜像摘要逐字节一致，二次全部 `reused=true` |
| 内容差异 | `assemble.TestContentChangeChangesDigest` | 不同层稳定区分，相同层跨镜像共享去重 |
| 三类完整性 | `cas.TestCheckThreeWayClassification` / `cas.TestMissingVsCorruptAreDistinct` | 缺失 `ErrNotFound`、截断 `ErrSizeMismatch`、等长篡改 `ErrDigestMismatch`，互不串味 |
| 全库逐对象巡检 | `verify.TestVerifyClassifiesPerObject` | 缺失/截断/篡改逐对象列出，好镜像仍 `ok`，坏对象不被当缺失跳过 |
| 写入拒污 | `cas.TestPutRejectsDigestMismatch` | 声明摘要不符拒绝且无对象落地 |
| 格式不兼容 | `manifest.TestParseKindRejectsUnsupportedFormats` | 旧/未知格式 `ErrUnsupportedFormat`/`ErrMalformed` |
| 引用版本 | `registry.TestRefVersionCompatibility` | 无版本字段兼容；高版本拒绝；损坏不伪装成缺失 |
| 提交原子性 | `registry.TestCommitImageFailureIsAtomic` | 坏镜像提交失败、引用停在旧版本；分类 `ErrCommitIntegrity`+具体哨兵 |
| 失败重试/占用有界 | `registry.TestCommitRetryDoesNotAccumulate` / `TestCommitImagePlanAtomicAndRetry` | 反复失败不累积、重试结果确定、占用回落到当前可达集 |
| 旧引用继续提交 | `registry.TestCommitPlatformLegacyAndFutureRef` | 无版本字段旧引用可继续追加平台；超版本明确拒绝 |
| 多架构歧义 | `registry.TestVariantAmbiguityIsRejected` | 0 个 `ErrNoMatch`、多个 `ErrAmbiguous`、重复 `ErrDuplicatePlatform` |
| 并发提交+回收+校验 | `registry.TestConcurrentCommitGCVerify` | 已发布引用始终完整、无 lost-update（冲突报 `ErrCommitConflict`）、占用收敛 |
| 并发回收 | `gc.TestConcurrentGCAndWrites` | 无半写入/摘要错位；引用对象不丢；占用回落到可达集 |
| 全链路 | `integration.TestEndToEnd` | 组装→索引→选择→回收→损坏中止的完整闭环 |

所有测试日志均打印**输入内容（摘要/平台/格式串）与判定依据**，
便于复核结论的可复现性。
