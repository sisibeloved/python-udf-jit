# blue-53：三路径与逐算子性能实测

> **AD 基线已纠偏：** 旧 AD 数据含未配置根目录时的每行 Path 拼接，已从正常配置收益表撤下。下文 AD 正收益采用两臂都设置 `VOLC_NUSC_ROOT` 的重测；直接 `params.nusc_root` 配置当前阻断目标准入，没有收益。详见 [完整纠偏报告](/opt/Codex/python-udf-jit/docs/reports/2026-09-07-blue53-ad-explicit-root-correction.md)。

**状态：全部计划已完成且资源/输出门禁通过。** 有效计时：96次流水线＋56次独立算子，共152次；另补32次AD显式根目录纠偏计时。功能验证、捕获输入和失败样本不计入。测试日期 2026-09-07；采样状态更新于 2026-09-07T11:04:27.729297+00:00。本页只使用这次在 blue-53 新跑的数据，没有混入历史倍数。

## 三条路径各自的收益

以下各项是相对于原 UDF 基线的独立启用结果。①、②测 FineWeb；③测具备可复用输入的 AD。它们是不同工作负载中的路径收益，不是一个流水线里可相加的贡献百分比。

| 独立路径 | 基线 → 优化后（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| ① Strict Typed JIT；FineWeb R→T | 288.692 → 225.017 | 1.2830× | 22.06% |
| ② 原生表达式；FineWeb R→N | 290.646 → 122.278 | 2.3769× | 57.93% |
| ③ Guarded 批执行含字典；AD R→B，显式 VOLC_NUSC_ROOT | 139.932 → 96.673 | 1.4475× | 30.91% |

直接 `params.nusc_root` 的完整 pipeline 对照 R→B 为 138.108→140.683s（0.9817×）；目标未进入批执行，不能用环境变量配置的大倍数代替这个结果。

时间口径为运行器 `metrics.elapsed_s`：包括 Ray 初始化、流水线执行和最终 Lance 写入。驱动构图、进程导入、文件 manifest 准备及计时后的内容哈希不在该指标里；外层 launcher wall 保存在原始记录中，不能拿它当优化收益。`pipeline_execute_s` 的独立结果保存在机器可读汇总中。

## 组合和增量对照

| 对照 | 基线 → 优化后（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| FineWeb：全部能力 R→ALL | 289.790 → 89.034 | 3.2548× | 69.28% |
| FineWeb：原生表达式后增加标量 JIT N→ALL | 122.225 → 89.882 | 1.3598× | 26.46% |

AD 直接参数配置 R→ALL：138.096→147.264s，0.9377×；align 未准入。旧默认目录的 AD 组合倍数仅保存在本页末尾的历史配置附录。

R 关闭本项目的 UDF JIT 接入；共同的 CinderX 运行时保持可用，但按调用次数的自动编译未配置。独立策略探针中普通函数调用十万次仍未进入 CinderX，显式 force_compile 成功。因此这些是相对原 UDF 的路径收益。T 只允许 typed region，显式关闭标量 invariant/value-cache 后备路径；S 允许这些后端，包括它们的显式编译。因此 AD 的 S 对照不会被算入 strict Typed。B 的 batch loop 自身使用 CinderX 编译与其 value-cache 能力，这是路径③内部实现。ALL0 与 ALL 的差别只有适用字典复用。

## 每个观察到可归因提升的算子

原流水线 `per_op/p0` 的 `collect()` 窗口包括保留列的物化、框架调度和计算；实测部分步骤CV超过40%，不足以给出稳定的算子倍数。因此主表使用补充的**固定真实阶段输入的独立算子对照**，原窗口数据完整留在附录CSV。

独立测试先用原算子和原参数顺序生成完整 FineWeb200K / AD5M 的真实阶段输入与期望输出，保存单列检查点；没有缩小或合成输入。每次读取检查点、加载Arrow、构造计划后，计时只围绕单个公共 `to_arrow()` 终端执行，完整取回该算子输出。窗口包含框架传输、算子执行、filter和输出物化，排除检查点读取、Ray初始化、构图、内容哈希；不是纯机器码内核耗时，也不能累加为惰性end模式的贡献。

所有独立组保持同一检查点、同一源码/参数/运行时和1个分区、4核预算，使用新进程避免跨阶段缓存保留。初始ABBA各臂两轮，样本CV超过5%时追加一组ABBA并保留全部样本；不删除异常值或放宽判读门槛。主表要求内容与资源合同、独立目标命中证明、双向正收益和稳定性门禁通过；没有声称统计置信区间。

| 算子 | 对照 | 基线 → 优化后（秒） | 加速比 / 耗时下降 | 对应优化点 |
|---|---|---:|---:|---|
| FineWeb 200K / `clean_html_mapper` | R→N | 3.726 → 1.467 | 2.539× / 60.61% | 等价 re.sub 变成 Daft regexp_replace，移除逐行 Python UDF 边界；保留非空和依赖 guard。 |
| FineWeb 200K / `clean_links_mapper` | R→N | 23.223 → 1.488 | 15.607× / 93.59% | 等价 re.sub 变成 Daft regexp_replace，移除逐行 Python UDF 边界；保留非空和依赖 guard。 |
| FineWeb 200K / `clean_copyright_mapper` | R→N | 28.411 → 1.834 | 15.493× / 93.55% | 等价 re.sub 变成 Daft regexp_replace，移除逐行 Python UDF 边界；保留非空和依赖 guard。 |
| FineWeb 200K / `punctuation_normalization_mapper` | R→T | 20.273 → 10.185 | 1.991× / 49.76% | 常量标点转换表成为 immutable.lookup 与 Unicode builder 循环，避免逐行重建 maketrans 字典及通用 translate 调用。 |
| FineWeb 200K / `punctuation_normalization_mapper` | R→N | 20.487 → 3.382 | 6.058× / 83.49% | 六项固定标点替换变成 Daft replace 表达式链；不再构造 Python 翻译表。 |
| FineWeb 200K / `whitespace_normalization_mapper` | R→T | 32.078 → 15.626 | 2.053× / 51.29% | 将已证明的 whitespace regex+strip 捕获为 Unicode 空白分类、有限状态转换与结果 builder。 |
| FineWeb 200K / `whitespace_normalization_mapper` | R→N | 31.891 → 9.069 | 3.516× / 71.56% | Daft regexp_replace + lstrip/rstrip；匹配 Python Unicode 空白集合。 |
| FineWeb 200K / `text_length_filter` | R→N | 2.022 → 1.230 | 1.644× / 39.18% | Daft length 加闭区间比较，移除逐行 Python 长度谓词调用。 |
| FineWeb 200K / `alphanumeric_filter` | R→T | 44.443 → 5.836 | 7.615× / 86.87% | 将逐字符 isalnum 生成器归约捕获为 typed 循环；Unicode 读取/分类、累加、除法和阈值比较由编译区域执行。 |
| FineWeb 200K / `language_id_score_filter` | R→N | 69.343 → 29.654 | 2.338× / 57.24% | 六组语言字符的 regexp_count、最高计数 tie-break 和英文十分之一阈值表达式，省去 Python findall 列表/字典/评分循环。仅用于启发式 stand-in，不是 fastText 模型下沉。 |
| AD 5M / `ad_sensor_align_mapper`，显式 env root | R→B | 52.728 → 3.099 | 17.015× / 94.12% | Arrow批入口、CinderX批循环、guarded结果复用，以及批内字典去重/take重建；已消除默认Path拼接 |
| AD 5M / `ad_sensor_align_mapper`，显式 env root | B0→B | 11.221 → 3.085 | 3.637× / 72.50% | 保持编译批循环和值缓存，仅开启Arrow字典去重与take重建；两臂均显式VOLC_NUSC_ROOT |

AD直接参数配置与环境配置的全部结果（包括未命中和退步）见[纠偏表](/opt/Codex/python-udf-jit/docs/reports/2026-09-07-blue53-ad-explicit-root-correction.md)。


同一算子可能有 typed 与 native 两种替代实现，表中分别列出独立测试；ALL 中可下沉的算子优先走 native，不同时叠加两种内核。原独立算子数据（AD旧配置已标识）：[2026-09-07-blue53-isolated-operator-performance.csv](/opt/Codex/python-udf-jit/docs/reports/evidence/2026-09-07-blue53-isolated-operator-performance.csv)；原流水线collect窗口（包含高波动样本）：[2026-09-07-blue53-operator-performance.csv](/opt/Codex/python-udf-jit/docs/reports/evidence/2026-09-07-blue53-operator-performance.csv)。

## 未发现专门路径收益的控制流水线

| 流水线 | R → ALL（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| COCO 5K 图像 | 66.913 → 67.019 | 0.9984× | -0.16% |
| CommonVoice 512 音频 | 780.187 → 778.890 | 1.0017× | 0.17% |
| Panda 8 视频 | 91.757 → 91.652 | 1.0012× | 0.11% |
| PMC 4 PDF | 447.071 → 448.784 | 0.9962× | -0.38% |

AD index 独立控制：R 76.655s → ALL 81.414s，0.9416×，耗时增加 6.21%。该函数没有目标优化命中。控制组完整重跑于16GiB；旧10GiB OOM不计入。这个单算子观察不等于已经证明宏观B/ALL差值的全部原因。

这些算子以媒体库、OCR、模型或不被当前前端接受的调用为主；没有专门路径命中时，微小的正差值不会被解释为新优化。音频预期会隔离四个无有效静音处理产物的输入，完整输出应为 508；日志中的这种行级处理不是 Ray worker 重试。

## 实际命中与代码优化点

- ①：FineWeb `dj_alphanumeric_ok`、`dj_punctuation_normalize`、`dj_whitespace_normalization` 在独立 T 诊断中进入 generic typed HIR。归约使用 UnicodeData/Kind/Read/Classify 和整数操作；查表构造与 FSM 构造使用 typed table/builder。正式计时不启用这些诊断 wrapper。
- ②：FineWeb 七项 native plan，HTML/links/copyright 三项 regexp_replace，punctuation translation，whitespace，length，language。独立目标诊断证实七项 guard checks、零 miss；ALL 同时保留 alnum typed。语言是限定 en/0.1 的字符集启发式，不能宣称 fastText 获得加速。
- ③：下述命中只适用于原函数调用形状；显式 `VOLC_NUSC_ROOT` 的完整5M纠偏诊断也已复验。直接 `params.nusc_root` 的参数绑定包装当前未准入。AD align 实际进入 native batch。独立 500K 重复输入诊断中，四个 Arrow 批次覆盖 500000 行，只物化 40000 个批内唯一 Python 输入和输出，减少 92%；无全量 Python 物化、fallback、precommit failure 或 replay。计数取每进程最终累计值，不能累加并发调用重叠的快照差值。补充的完整5M独立证明有39个native批次：38个字典批次覆盖4980736行、380000个批内唯一输入，尾批19264行走普通批执行；这些诊断时间均不进入性能比值。
- AD `ad_index_mapper` 在当前源码结构下未获得 typed/value-cache 准入，target 诊断 compile/hits 均为 0，不能沿用旧报告的 index 缓存收益。
- 不变 helper/value cache、Arrow dictionary 与框架原生模型 batch 有不同归因；已有 BGE/SentenceTransformer 模型批处理属于共享基线，不算成新路径③。

源码与机制引用见 [operator-mechanisms.md](/opt/Codex/python-udf-jit/.context/blue53-three-path-20260907/operator-mechanisms.md)；该映射连同目标运行证据保存在 JSON 中。

## 固定环境和输入

- 主机 blue-53（node02），aarch64；Python 3.14.3，SOABI `cpython-314-aarch64-linux-gnu`，header patchlevel 已验证。
- UDF 源码 `f67ff8309da2b752c13cfb5c35e3508ae5d19dad`；业务源码为 clean `85d84b92406f472dbefe7ff1955bd1d563dfe81b` 的快照。用户本地未提交 PDF/media 修改未混入实验。
- 所有臂使用镜像 `f27a67a8ebb356b019297485c8d936bff0bb16f902c1538aa7c4c8a0b242c765`，固定生产 `_cinderx.so` SHA256 `b3b1cf5b79563138008bd735ca1a90f023f2c10d3ba5f9f0a99c41831b595985`；本次没有重建 CinderX。
- Daft 0.7.2、Ray 2.55.0、PyArrow 22.0.0、Lance 7.0.0；CinderX lightweight frame/HIR inliner 配置在两臂相同；所有业务和 JIT 正式诊断关闭。
- 每条流水线分配不同 L3 的四物理核，Ray budget=4，库线程=1，fusion 与业务框架手工 native shortcut 均关闭。实际分区数按 runner 记录，不能把四核预算当成四个并发 worker。
- CPU NUMA3 无本地内存。主流水线普通组使用 memory node2/10GiB，AD 使用 node0/28GiB；独立算子使用空闲槽位和 memory node2，普通10GiB，AD index控制16GiB。每个对照块内部配置相同。AD 的旧10GiB OOM轮被排除，全部 AD 正式组在新配置下重启。其他用户作业未被停止；不同槽位仍可能共享内存带宽，成对重复和 CV 用于观察噪声。
- FineWeb200K 为真实冻结文本，困惑度采用 stand-in；现有镜像缺 KenLM/SentencePiece，未安装新依赖。AD5M 为固定10K唯一值的重复规模集。配套冻结metadata只有一条CAM_FRONT传感器记录，文件名覆盖六相机方向；本测量属于文件路径/metadata查表，不代表完整多传感器时序对齐。图像含原有 GPU/embedding 替代路径；PDF真实 parse/OCR以及现有 all-MiniLM-L6-v2 CPU句向量执行不被 fake-GPU 标志短路。
- native 三项新增 FineWeb 开关均显式开启；这些开关在产品代码中默认关闭。这里测量路径能力，不能作为完全默认环境的性能声明。

## 正确性与异常处理

所有用于比值的块检查相同源码、二进制、任务、CPU/内存配置，以及输入/输出行数、schema和 sink 列值 multiset。普通组内存上限从实际 docker 命令的 --memory=10g 提取核对。FineWeb 运行器未报告输入 fingerprint，使用冻结任务中声明的指纹及相同任务 SHA 作为输入身份依据，并明确标为声明值；没有把四个缺失值当成运行时指纹校验，也没有声称每轮重新计算了输入内容哈希。视频输出含随机临时目录，额外保留最终帧并比较完整文件名、大小、SHA256；只规范化已识别的随机根目录，保留确定性路径尾部和内容，原始 URI 摘要也保留。

本次环境 smoke 的无内存 NUMA3配置、AD10GiB OOM、缺少最终帧内容证明的视频旧 smoke均未计入正式结果。独立输入捕获中的长驻Ray进程OOM改为每阶段新进程；旧collect后再to_arrow的N验证触发第二次lineage解析，已排除，改为一次公共to_arrow并验证guard/miss/rebuild为1/0/0。没有修改生产源码，也没有通过删除步骤、允许重放或放宽内容比较获得收益。

## 复现与原始证据

远端专属目录：`/home/lxy/udfjit-three-path-20260907-01a07ad8`。各次 `status/*.json` 保存真实 docker 命令、环境变量、输入任务/源码/驱动/二进制身份、时间、exit status；对应 `logs/`、`results/*/runner/`、sink-check和独立probes保留原始证据。

本地同步入口：[/opt/Codex/python-udf-jit/.context/blue53-three-path-20260907/evidence](/opt/Codex/python-udf-jit/.context/blue53-three-path-20260907/evidence)。复算脚本只读这些新样本，不混历史：

```bash
cd /opt/Codex/python-udf-jit
python3 .context/blue53-three-path-20260907/collect_progress.py
python3 .context/blue53-three-path-20260907/summarize_new.py
python3 .context/blue53-three-path-20260907/render_report.py --final
python3 .context/blue53-ad-root-correction-20260907/update_prior_reports.py
```

正式重跑应为每次 arm提供新的label，禁止覆盖既有结果。测试驱动在 `.context/blue53-three-path-20260907/harness`，视频和AD使用记录在各自phase里的专用launcher。控制开关只在实验启动层操作，不改 UDF 业务函数体或生产源码。

## AD 旧回退配置历史附录（不作为正常配置收益）

以下均是本次纠偏前未设置 `params.nusc_root` / `VOLC_NUSC_ROOT` 的测量，包含默认目录解析；只保留审计记录。不同计时范围分开列出，不能与新值拼接。

| 原对照 | 基线 → 优化后（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| ③ Guarded 批执行含字典；AD R→B | 174.515 → 93.936 | 1.8578× | 46.17% |
| AD：标量 guarded-cache 后端 R→S（含显式编译） | 174.934 → 148.942 | 1.1745× | 14.86% |
| AD：全部能力 R→ALL | 174.026 → 98.770 | 1.7619× | 43.24% |
| AD：已有标量缓存后增加批执行 S→ALL | 150.899 → 100.160 | 1.5066× | 33.62% |
| AD：已有批执行后增加字典 ALL0→ALL | 108.154 → 99.254 | 1.0897× | 8.23% |

| 原算子 | 对照 | 基线 → 优化后（秒） | 加速比 / 耗时下降 | 当时机制 |
|---|---|---:|---:|---|
| AD 5M / `ad_sensor_align_mapper` | R→S | 88.766 → 52.843 | 1.680× / 40.47% | guarded value-cache 后端，含显式编译；helper缓存未单独拆分贡献 |
| AD 5M / `ad_sensor_align_mapper` | R→B | 88.839 → 2.624 | 33.853× / 97.05% | Arrow批入口、CinderX批循环和值复用；只执行批内唯一值并用take重建，减少逐行包装和Python对象物化 |
| AD 5M / `ad_sensor_align_mapper` | B0→B | 10.900 → 2.639 | 4.130× / 75.79% | 保持编译批循环和值缓存，仅开启Arrow字典去重与take重建 |
