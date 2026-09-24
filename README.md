# Python UDF JIT

Python UDF JIT 为 Daft/Ray 作业提供透明的 Python UDF 编译路径。用户保持原有 UDF 与 `where`、`select`、`with_columns` 调用方式：驱动节点生成可移植制品，工作节点完成严格校验、物理化、CinderX 编译、守卫式多变体执行和冻结治理。本仓库是首个正式实现，不读取穿刺期制品，也不承担旧格式或未来格式的兼容责任。

项目由三条相互衔接的内容线构成：

1. **标量主线**（RFC-001～RFC-008）：标量 UDF 的捕获、语义 IR、可移植制品、CinderX 编译与守卫执行、运行时治理，已完成正式实现与验收。
2. **列式与表达式降级线**：守卫式列式批执行与原生表达式降级，把支持的 UDF 形态下沉到列式/原生执行（FineWeb 200K 全管线 1.83×，whitespace 算子 7.71×），已合入 master。
3. **CinderX 运行时补丁系列**（`vendor/cinderx/`）：本仓库依赖的 CinderX 改动以确定性补丁系列携带，与上游固定提交对齐，构建时按运行时线二选一。

## 性能现状

对照口径：R 为关闭 UDF JIT 接入的基线；T=仅 ① typed region；N=仅 ② 原生表达式；B=批执行（含字典复用）；ALL=①+②+③ 全开。FineWeb 数值为 real-KenLM 语言过滤口径（当前代码，含 join-genexp translate 捕获），每臂同块 ABBA 两轮取均值。

### 优化点级（各自独立启用）

| 优化点 | 负载 | 对照 | 基线 → 优化后（秒） | 加速比 |
|---|---|---|---:|---:|
| ① Strict Typed JIT | FineWeb 200K | R→T | 371.98 → 336.72 | 1.10× |
| ② 原生表达式降级 | FineWeb 200K | R→N | 373.66 → 238.39 | 1.57× |
| ③ 守卫批执行（含字典复用） | AD 5M | R→B | 139.93 → 96.67 | 1.45× |

三项在不同负载上独立启用，不可相加；③ 在 AD 上测，FineWeb 的唯一值负载不满足字典复用门槛。

### 管线级（端到端）

| 管线 | 负载 | 对照 | 加速比 |
|---|---|---|---:|
| FineWeb 文本处理 | 200K 行 | R→ALL | 1.83× |
| AD 传感对齐 | 5M 行 | R→B | 1.45× |
| COCO 图像 | 5K | R→ALL | 1.00× |
| CommonVoice 音频 | 512 | R→ALL | 1.00× |
| Panda 视频 | 8 | R→ALL | 1.00× |
| PMC PDF | 4 | R→ALL | 1.00× |

四个控制管线无优化命中，≈1.00× 的微差不解释为收益或回归。AD 取批执行命中配置；未命中的直接参数配置见[纠偏报告](docs/reports/2026-09-07-blue53-ad-explicit-root-correction.md)。

### 算子级

FineWeb 200K（real-KenLM），三列分别为仅 ①、仅 ②、全开时该算子窗口的加速比：

| 算子 | ① typed | ② 原生 | 全开 |
|---|---:|---:|---:|
| clean_html_mapper | 0.94 | 2.96 | 2.94 |
| clean_links_mapper | 0.99 | 28.74 | 30.24 |
| clean_email_mapper | 0.99 | 1.00 | 0.99 |
| clean_copyright_mapper | 0.99 | 9.00 | 8.50 |
| fix_unicode_mapper | 0.76 | 0.88 | 1.21 |
| punctuation_normalization_mapper | 2.01 | 5.51 | 3.28 |
| whitespace_normalization_mapper | 0.92 | 7.71 | 6.82 |
| text_length_filter | 1.05 | 1.12 | 1.24 |
| alphanumeric_filter | 4.10 | 0.90 | 4.07 |
| language_id_score_filter | 1.00 | 0.94 | 0.99 |
| perplexity_filter | 0.96 | 1.08 | 1.05 |
| document_deduplicator | 1.05 | 0.91 | 1.31 |
| text_chunk_mapper | 0.97 | 0.86 | 0.89 |

AD 5M（批执行口径）：

| 算子 | 对照 | 加速比 |
|---|---|---:|
| ad_sensor_align_mapper | R→B | 17.02× |
| ad_sensor_align_mapper | B0→B | 3.64× |

ALL 中可下沉算子优先走 ②，① 与 ② 不叠加同一算子。whitespace 在 ② 列的 7.71× 即 join-genexp translate 捕获落地后的取值；对官方 Data-Juicer 实现的隔离对比（14.2×、输出零分歧）为另一口径，不在本表。

数据文件：[FineWeb 逐算子 CSV](docs/reports/evidence/2026-09-24-realkenlm-perop.csv)、[FineWeb 端到端 JSON](docs/reports/evidence/2026-09-24-realkenlm-endtoend.json)。方法说明、stand-in 口径历史消融与控制组明细见[三路径与逐算子性能实测](docs/reports/2026-09-07-blue53-three-path-and-operator-performance.md)。

## 当前状态

**标量主线。** RFC-001～RFC-008 全部实现并通过正式验收：单元、集成、实时系统测试全量通过、零跳过，8/8 标量契约通过，自然工作节点覆盖与数据面隔离达标。验收明细见[标量主线正式验收报告](docs/reports/2026-07-29-mainline-scalar-acceptance.md)。发布契约中仍缺真实多物理节点外部证据这一项，因此 `release_ready=false` 维持：单宿主多容器结果不冒充多物理节点结果。生产目标版本是 Python 3.11.6；当前解码器和 CinderX 补丁先在 Python 3.14.3 上开发、测试和验证，Python 3.11.6 适配与资格验证仍待 CinderX 支持就绪后完成。

**列式与表达式降级线。** 已合入 master，包含四个层次：调用布局驱动的标量执行、守卫式列式批执行、Arrow 字典值跨重复行复用、守卫式 UDF 到原生列式执行的降级。在此之上，表达式捕获器把可证明等价的字符串变换直接降级为 Daft 原生表达式（含 join-genexp 字符类 translate 形态），保持摘要与语义哈希一致门禁。

**JIT 回归门禁。** 携带全部 UDF-JIT 补丁的 CinderX 在 `PYTHONJITAUTO=auto:2` 口径下通过全量 pyperformance A/B：122/122 配对、零崩溃、几何平均 1.00× 持平。修复前曾出现的 deepcopy 21.84×、sympy_integrate 9.32×、sphinx 3.23×、sympy_sum 2.03× 回归已全部消除（见 [vendor/cinderx/udf-jit/README.md](vendor/cinderx/udf-jit/README.md)）。

**扩展边界。** RFC-009～RFC-012（混合执行提供者、列式执行、稀疏批侧退出、等价语义改写作为独立 RFC）保持关闭；向量、Arrow 和批处理执行只在降级线内按守卫边界提供。

## 标量主线能力

| RFC | 已实现能力 |
|---|---|
| RFC-001 | Daft 精确兼容检查、显式启动引导、候选登记、操作定稿、完整 UDF 选项保留和工作节点载体 |
| RFC-002 | 版本化字节码解码、控制流图、抽象解释、源码映射、图中断、依赖身份和有界捕获缓存 |
| RFC-003 | 语义核心 IR、类型/空值/效应/异常分析、区域划分、验证器、参考执行和精确 Python 区域 |
| RFC-004 | 首个正式制品格式 1.0、固定字段与分段、内容哈希、内联和 Ray `ObjectRef` 双载体、工作节点重新校验 |
| RFC-005 | `bool/int32/int64/float32/float64` 标量槽位、可空有效位、能力句柄、进程代际、所有权、原子发布和向量扩展拒绝点 |
| RFC-006 | 五种标量类型、可空值、算术、比较、分支、CinderX 数据内建函数、W^X 代码、标量执行与解释续体 |
| RFC-007 | 分层守卫、策略绑定的变体键、异步单次编译、多变体缓存、负缓存、熔断、精确侧退出、活动引用和硬资源预算 |
| RFC-008 | `off/observe/auto`、冻结策略和策略哈希、作业/租户隔离、解释信息、有限原因码、异步遥测、命令行工具和验收聚合 |

治理没有远程凭据分发或运行中控制通道，也没有紧急停用通道。模式和策略在作业提交时冻结；回滚通过新作业使用 `off` 或部署回滚完成，不中断已经进入执行的区域。

## 列式与表达式降级线

| 能力 | 说明 |
|---|---|
| 调用布局驱动执行 | 按调用布局选择标量执行路径，为后续形态扩展提供挂点 |
| 守卫式列式批执行 | `ColumnarBatchWrapper` 以守卫边界承载批执行，守卫失败精确回落标量路径 |
| Arrow 字典值复用 | 跨重复行复用字典值与解码结果，重复键负载下消除重复物化 |
| 原生表达式降级 | `vector_predicate` 捕获器把可证明等价的逐元素字符串变换降级为 Daft 原生表达式；语义哈希与输出摘要一致是硬门禁 |
| join-genexp translate 捕获 | `"".join(c if c not in SET else " " for c in text)` 形态识别为字符类 translate，降级为 `lstrip/rstrip + regexp_replace` 原生序列；whitespace 归一管线内 7.71×，FineWeb 200K 全管线 `off→原生` 1.57×、`off→原生+列式` 1.83×，8/8 摘要一致 |

逐算子口径与验证方法见 [FineWeb 三路降级验证报告](docs/reports/2026-08-27-fineweb-three-lowerings-validation.md)与[框架表达式降级模式](docs/solutions/architecture-patterns/framework-expression-lowering-for-columnar-udfs.md)。

## CinderX 运行时补丁系列

`vendor/cinderx/` 以确定性补丁系列携带本仓库对 CinderX 的全部改动，与固定上游提交对齐（`manifest.json` 记录逐补丁 sha256、按序拼接 sha256 与触及文件并集数）。两条系列内容不相交、不可相互叠加，构建时按运行时线二选一：

| 系列 | 目录 | 内容 |
|---|---|---|
| 标量主线 | `vendor/cinderx/patches/`（0001–0006） | 运行时候选、原始数据内建、续体去优化、W^X 双映射、泛型 typed-loop 特化、泛型序列模式 |
| UDF-JIT | `vendor/cinderx/udf-jit/`（0001–0007） | UDF 续体与数据流、generic HIR typed region、OSR 用例清理、守卫式精确值复用、autojit 空指针修复、瞬态闭包编译复用、闭包特化编译预算 |

UDF-JIT 系列的后三个补丁是 autojit 门控（`auto:2`）下的崩溃与回归修复：空指针崩溃、瞬态闭包逐实例重编（deepcopy 类回归）、闭包特化签名逐实例漂移导致的重编（异常格式化/闭包密集负载回归）。每个补丁的机制、证据与验证数字见 [vendor/cinderx/udf-jit/README.md](vendor/cinderx/udf-jit/README.md)；标量主线系列说明见 [vendor/cinderx/README.md](vendor/cinderx/README.md)。

## 性能口径

性能与功能状态分开管理。上方"性能现状"表维护当前已确立的结论性数字；每个优化变更继续执行同口径 A/B 并更新该表，逐算子明细与稳定性统计随对应变更写入验证报告。标量阶段的方向性观测不写成正式性能结论，只有另行声明性能资格时才启用累计目标。

## 仓库布局

```text
src/python_udf_jit/   协议(protocol)、编译器(compiler)、提供者(provider)、
                      运行时(runtime)、治理(governance)、集成(integration)、
                      基准(benchmarks)、诊断(diagnostics)
tests/                unit / integration / e2e / system / fuzz
vendor/cinderx/       标量主线系列(patches)与 UDF-JIT 系列(udf-jit)，各带 manifest
docs/                 design / rfcs / plans / reports / operations / solutions
benchmarks/           基准负载与驱动
```

## 文档入口

- [RFC 索引与实现状态](docs/rfcs/README.md)
- [架构设计](docs/design/2026-07-13-python-udf-jit-architecture.md)
- [标量主线功能设计](docs/design/2026-07-29-python-udf-jit-scalar-mainline-function-design.md)
- [主线完成计划](docs/plans/2026-07-26-001-feat-mainline-production-completion-plan.md)
- [标量主线正式验收报告](docs/reports/2026-07-29-mainline-scalar-acceptance.md)
- [FineWeb 三路降级验证报告](docs/reports/2026-08-27-fineweb-three-lowerings-validation.md)
- [三路径与逐算子性能实测](docs/reports/2026-09-07-blue53-three-path-and-operator-performance.md)
- [三路径性能收益归因](docs/reports/2026-09-07-three-path-performance-attribution.md)
- [部署、灰度与回滚手册](docs/operations/mainline-deployment-and-rollback.md)
- [标量主线垂直切片复盘](docs/solutions/architecture-patterns/scalar-mainline-vertical-slice.md)
- [框架表达式降级模式](docs/solutions/architecture-patterns/framework-expression-lowering-for-columnar-udfs.md)
