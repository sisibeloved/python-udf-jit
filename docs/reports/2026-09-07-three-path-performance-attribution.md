# Python UDF 三路径性能收益：已有实测复算与归因

> **历史记录：** 本页未新跑实验。当前同机实测见[三路径报告](/opt/Codex/python-udf-jit/docs/reports/2026-09-07-blue53-three-path-and-operator-performance.md)；AD 已发现默认目录回退开销，正常根目录配置应引用[纠偏实测](/opt/Codex/python-udf-jit/docs/reports/2026-09-07-blue53-ad-explicit-root-correction.md)，本页旧 AD 数据不作为当前正常配置收益。

日期：2026-09-07。检查代码：`f67ff8309da2b752c13cfb5c35e3508ae5d19dad`。

本次读取并复算已有计时样本，没有新跑 benchmark。最新原生表达式实验受测提交 `dcd38360b025fb67a816b264643beeac2777f778` 与当前 HEAD 的 `src/`、`tests/` 无差异；这不等于本次复验了测试机器的 CinderX 二进制、数据或环境。旧 JIT 和批执行记录仍是历史版本证据。

## 可以引用的收益

| 路径与测量范围 | 基线 → 候选（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| ① Typed JIT；FineWeb 200K；2026-08-04，执行阶段 | 522.088 → 426.247 | 1.2248× | 18.36% |
| ② 既有四项原生下沉；FineWeb 200K；2026-08-14，执行阶段 | 303.328 → 283.664 | 1.0693× | 6.48% |
| ② 新增三项下沉的增量；FineWeb 200K stand-in；2026-08-26/27，总耗时 | 294.443 → 157.008 | 1.8753× | 46.68% |
| ② 新增三项下沉的增量；FineWeb 200K real-KenLM；2026-08-26/27，总耗时 | 604.228 → 435.075 | 1.3888× | 27.99% |
| ③ Guarded 批执行含字典；AD 5M；2026-08-14，执行阶段 | 197.216 → 124.532 | 1.5837× | 36.86% |
| ③ 字典复用的额外增量；AD 5M；2026-08-13，执行阶段 | 141.221 → 129.249 | 1.0926× | 8.48% |

①的流水线数字只有单次 A→B；②既有四项、②新增三项、③批执行均是各臂两轮的 ABBA 均值；③字典增量采用各臂四轮的中位数。不同日期、镜像、计时范围和基线的数字不能拼成共同基线，更不能相加或相乘。

## 每项究竟对比什么

1. **① Typed JIT**：2026-08-04 的 FineWeb 200K，`UDFJIT_MODE=off` 对 `auto`；当时尚无后续框架表达式和 Arrow 批次路径。执行阶段不含最终 sink，记录为 522.088468s 对 426.246999s。微基准则明确比较“原 UDF 的普通 CinderX JIT”与“typed region + generic HIR”，并非无 JIT 的 CPython。
2. **② 既有原生表达式**：iteration 19 的 FineWeb，固定 `MODE=auto`，只切换 `COLUMNAR=0/1`。覆盖 HTML、标点、空白、长度四类原生表达式；保留其他标量执行。归因来自该工作负载的实际准入和计划证据；`COLUMNAR` 本身并非全局只控制路径②。功能诊断在 blue-98、正式计时在 blue-53，不将诊断轮计入时间。
3. **② 新增原生表达式**：最新同提交 B→F 只打开 links、language、copyright 三个独立开关。B 已包含四项原生表达式与 typed JIT。故 1.8753×/1.3888×只归于三项新增下沉，不能代表路径②全部收益。总耗时含 Ray 初始化及最终 Lance 写入。stand-in 为替代算子；real-KenLM 为真实困惑度模型，语言过滤仍是正则启发式。
4. **③ Guarded 批执行**：iteration 19 的 AD 5M，固定标量/CinderX 能力，`COLUMNAR=0/1`。结果 1.5837×是批次入口及适用字典复用的整体增量，基线已经具备 CinderX 标量及 value-cache 能力。5M 行中的结果唯一值为 10K，四臂记录的 schema、行数、multiset 摘要一致。
5. **③ 字典增量**：已开启 native batch 后，单独关闭/开启字典；四轮中位数 141.2206325s 对 129.249186s。只允许无动态逐行 entry guard 的字符串计划折叠重复行。该实验与 iteration 19 非同块，不能用 1.5837/1.0926 推算“纯批次”收益。

## 路径①的内核收益

7 轮、每轮 400 次调用、4,275,200 个字符，诊断关闭；原普通 CinderX JIT 对 typed generic HIR。下面是内核耗时，不是流水线收益。

| 模式 | 基线 → typed（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| 字母数字比例归约 | 0.475 → 0.043 | 11.0073× | 90.92% |
| 标点转换 / 不可变查表构造 | 0.133 → 0.043 | 3.1090× | 67.83% |
| 空白归一化 / FSM 构造 | 0.234 → 0.095 | 2.4733× | 59.57% |

2026-08-03 的 10.81×旧 whole-loop helper 路线已被后续 generic HIR 替代，本报告采用 2026-08-04 的后续证据。

## 当前版本尚缺的统一消融

当前能引用三类历史实测；不能给出当前版本同机、同输入、同计时范围下三条路径各自的完整独立收益或贡献百分比。最新 A→B 混合 typed JIT 与既有原生表达式；A→F 的跨块总收益也不能标成路径②。

现有开关可以形成以下**待运行**的嵌套矩阵，所有行必须固定代码、CinderX、输入、CPU、线程、模型模式和计时范围，所有 FineWeb 新增开关的值也要固定并记录：

| 组 | MODE | COLUMNAR | DICTIONARY | 用途 |
|---|---|---|---|---|
| P | off | 0 | 0 | 无 UDF JIT 接入参照；保持同一 CinderX 运行时 |
| S | auto | 0 | 0 | 标量 provider：包含 typed 和可能的 guarded cache |
| E | auto | native-expr | 0 | S + 原生表达式；明确不启用批次提升 |
| B | auto | 1 | 0 | E + 适用的 native batch，字典关闭 |
| D | auto | 1 | 1 | B + 适用字典复用 |

可测 S→E 为②增量、E→D 为③增量、E→B 为纯批次增量、B→D 为字典增量。P→S 只有确认目标命中 typed 且其他标量加速受控时才能标为①；否则它是标量 provider 整体收益。三者的收益依赖该加入顺序，不能称作顺序无关的占比。若要求①纯 typed，应对已确认 typed 命中的 UDF做受控 direct A/B，或增加仅控制 typed region 的实验开关。

按 workload 各做相邻 ABBA，至少分别记录 `pipeline_execute_s` 和含 sink 的 `elapsed_s`；另跑诊断确认 typed hit、native expression plans、native batch rows、dictionary unique rows 与 fallback。未命中路径的运行记为不适用，不把噪声当成该路径收益。旧数据已经能回答已测范围，本次没有启动新的远端全矩阵。

## 证据、排除项与复算

- Typed 原始样本：[2026-08-04-generic-sequence-patterns-ab.json](evidence/2026-08-04-generic-sequence-patterns-ab.json)。
- 最新 22 个 ABBA 块：[2026-08-27-fineweb-three-lowerings-ab.json](evidence/2026-08-27-fineweb-three-lowerings-ab.json)。
- 批执行/四项下沉：`.context/compound-engineering/ce-optimize/columnar-batch-piercing/iteration-19-formal-summary.json`。
- 字典八轮：同目录 `experiment-log.yaml` 的 iteration 13。
- 复算输出：[2026-09-07-three-path-performance-attribution.json](evidence/2026-09-07-three-path-performance-attribution.json)，内嵌所用时间样本、原文件 SHA256、环境记录和范围说明；没有伪造新的 run.json。
- 明确排除缺少 null 证明、改变 Daft 进程策略、worker crash/retry、规划副作用或输入 schema 准入有误而作废的历史轮次；采用后续 qualified iteration 19。
- 本次检查了归档摘要一致性，未重读远端完整 sink；样本量不支持当前版本三路径占比的置信区间。
- 运行时代码无修改；无需重新构建 CinderX 或执行全量 RuntimeTests。

复算命令（读取历史文件，不运行业务）：

```bash
cd /opt/Codex/python-udf-jit
python3 .context/three-path-performance-20260907/recompute.py
```

已重算 33 个比较、164 个归档计时样本，另核验 22 个组合总收益派生值；它们不是 33 个新实验。iteration 19 性能/语义文件 SHA256 与实验日志记录一致，最新源码与受测实现一致。
