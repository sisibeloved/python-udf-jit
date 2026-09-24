# blue-53：AD 数据集根目录配置纠偏实测

用户指出的配置问题已确认。此前 AD 对照未设置 `params.nusc_root` 或 `VOLC_NUSC_ROOT`，基线包含每行默认 Path 拼接；原来的 AD 倍数只能描述该回退配置，不能作为正常配置下的优化收益。本报告使用相同源码、数据和二进制，在两臂都明确指定同一根目录后重新运行；没有从旧时间中扣减估算值。

## 配置与执行位置

[ad_ops.py:69](/opt/Codex/volc_operator_sim/ops/ad_ops.py:69) 的解析优先级为 `params.nusc_root` → `VOLC_NUSC_ROOT` → `Path(VOLC_DE_BENCH_ROOT) / "raw" / "extracted" / "nuscenes"`。仅设置 `VOLC_DE_BENCH_ROOT` 仍会触发最后一步。[align 算子:198](/opt/Codex/volc_operator_sim/ops/ad_ops.py:198) 每行调用此 helper。

直接在 pipeline 的 align 步骤配置：

```json
{
  "dj_ops": "ad_sensor_align_mapper",
  "category": "mapper",
  "params": {
    "nusc_root": "/home/lxy/de_bench_full/raw/extracted/nuscenes"
  }
}
```

也可保持 `params={}`，在执行环境设置 `VOLC_NUSC_ROOT=/home/lxy/de_bench_full/raw/extracted/nuscenes`。两种方式均消除默认 Path 拼接，直接参数还省去每行读取环境变量。

[_load_nusc_index:86](/opt/Codex/volc_operator_sim/ops/ad_ops.py:86) 已有 `lru_cache(maxsize=2)`；目录递归查找、metadata JSON 加载是每进程首次加载成本，不能说成每行重复。按输入取 basename、查索引、构造业务 JSON 和后续文件存在校验仍保留在对照中。

## 主口径：显式 params.nusc_root

| 范围 / 对照 | 基线 → 开关开启后（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| 完整 pipeline R→B | 138.108 → 140.683 | 0.9817× | -1.86% |
| 完整 pipeline R→ALL | 138.096 → 147.264 | 0.9377× | -6.64% |
| 独立 align R→S | 51.066 → 56.313 | 0.9068× | -10.28% |
| 独立 align R→B | 50.970 → 52.342 | 0.9738× | -2.69% |

负的“耗时下降”表示变慢。这里 S/B/ALL 均未让 align 真正进入目标优化，不能称为 Typed JIT 或批执行获得加速。R 关闭本项目 UDF 优化接入；S 开启标量能力（包含 invariant/value-cache 与其显式编译）；B 只开启 guarded batch 与字典；ALL 开启所有能力。共同 CinderX 运行时保持启用，但未配置按调用次数自动编译，R 不是已验证进入普通 CinderX JIT 的 UDF 基线。

原因是 [registry._bind_params:586](/opt/Codex/volc_operator_sim/registry.py:586) 产生 `return _base(s, **_kw)` 包装，而当前 [typed_loop_worker.py:449](/opt/Codex/python-udf-jit/src/python_udf_jit/integration/daft_ray/typed_loop_worker.py:449) 遇到展开关键字参数直接拒绝解包。实际解析停留在 `_bind_params.<locals>._fn`，wrapper depth 为 0；S 诊断 `compile_successes=0 / hits=0`，原因 `source_code_identity_mismatch`；B 没有 native batch 构建或执行证据。这个配置兼容性缺口没有在本次测量中修改。

## 补充口径：显式 VOLC_NUSC_ROOT，保留原函数调用形状

| 范围 / 对照 | 基线 → 开关开启后（秒） | 加速比 | 耗时下降 |
|---|---:|---:|---:|
| 完整 pipeline R→B | 139.932 → 96.673 | 1.4475× | 30.91% |
| 独立 align R→S | 52.756 → 53.759 | 0.9814× | -1.90% |
| 独立 align R→B | 52.728 → 3.099 | 17.0152× | 94.12% |
| 独立 align B0→B（仅字典增量） | 11.221 → 3.085 | 3.6367× | 72.50% |

这些是已经消除默认 Path 拼接、但仍保留每行环境查询的配置下的成对实测。它们不能替代上面的直接参数结果，也没有拿两种配置互作基线。S 的变化属于 guarded scalar cache 后端；本次实测没有净收益，旧配置下的标量缓存正收益不再成立，也不归入 strict Typed。B0 与 B 都保留编译批循环和值缓存，唯一性能因素是字典去重与 take 重建；这项增量已经包含在 R→B 中，不能再叠加。

当前 AD 的主要可用优化点仍落在 `ad_sensor_align_mapper`：Arrow 批入口减少逐行调度和对象转换，CinderX 编译批循环，guarded value-cache 复用结果，适用批次只计算字典唯一值再用 take 恢复整列。环境配置下的解析/编译探针确认函数为 `ops.ad_ops.dj_ad_sensor_align_real`；对同一个实际输入保留返回引用连续调用，验证了 immutable JSON 结果对象复用。该独立探针只证明实际目标能够复用返回对象，不提供完整 5M 标量缓存命中率；S adapter 的 hits 也不是 cache lookup 命中计数。

完整 5M 诊断中有 39 个 native batch：38 个字典批覆盖 4,980,736 行、380,000 个批内唯一值；末尾 19,264 行走普通批执行。无标量 fallback/replay。B0 的对应诊断证实字典关闭。输入是固定 10K 唯一值的 5M 重复规模集，因此倍数依赖重复度，不能外推到 5M 个唯一输入。

基础10K清单的文件名覆盖六个相机方向，但当前配套冻结 sensor.json 只有一条 CAM_FRONT 记录，calibrated_sensor.json 也只有一条；这份测试数据用于文件路径到冻结元数据的索引处理，不能据此评价完整多传感器时序对齐。数据清单及各阶段实际样本见 [数据形态核对](/opt/Codex/python-udf-jit/.context/blue53-ad-root-correction-20260907/data-shape.md)。

## 默认目录解析本身有多少成本

仅 helper 的独立微测（每样本 500,000 次调用，六个样本/模式）：

| 根目录模式 | 平均每次调用 |
|---|---:|
| 未指定，默认 Path 拼接 | 6.33260 μs |
| VOLC_NUSC_ROOT | 0.37310 μs |
| 直接 nusc_root 参数 | 0.08692 μs |

helper 未进入 JIT，也未 force_compile；包含 Python 调用和微测循环开销，不包含 metadata 加载或完整算子。这个微测只验证配置开销确实存在，不能乘以 5M 后从 pipeline 总时间硬减。

## 旧数据的处理

原 AD pipeline R→B 的 1.8578×、独立 align R→S 的 1.6798×、R→B 的 33.8527×、B0→B 的 4.1301×，全部退回“未显式配置根目录”的历史测量上下文。原始样本保留，正常配置收益以本页为准。旧 AD 其他组合没有与本次相同的显式根目录对照，不继续作为现行配置收益引用。FineWeb 以及图像、音频、视频、PDF 的既有测试不受该 AD helper 影响。

`ad_index_mapper` 的独立控制曾测得 0.9416×且无目标命中；本次没有将其包装成新优化收益。当前直接参数配置的入口缺口也不能用环境变量配置的大倍数掩盖。

## 测量和校验

纠偏共 12 次完整 pipeline 与 20 次独立算子有效计时，均在 blue-53 新跑。每块 ABBA，各臂初始两次；样本 CV 超 5% 才追加完整块并保留所有样本。本次有效块最大样本 CV 为 1.35%；不据两次样本声称置信区间。功能验证与 helper 微测不混入性能比值。20 份有效算子日志的审计通过，无 crash/retry/OOM；完整 pipeline 日志也未发现对应失败信号。

- pipeline 口径为 `metrics.elapsed_s`：Ray 初始化、完整 align→index→Lance sink；排除驱动导入、构图、manifest 准备和计时后的内容哈希。独立 `pipeline_execute_s` 也保留在 JSON。
- 单算子使用预加载的完整 5M 真实阶段检查点，在单次公共 `to_arrow()` 周围计时；包括算子、框架传输和输出物化，排除输入读取、Ray 初始化、构图、哈希。不是纯机器码内核时间，不能累加为完整 pipeline 贡献。
- 固定 UDF `f67ff8309da2b752c13cfb5c35e3508ae5d19dad`、业务 clean snapshot `85d84b92406f472dbefe7ff1955bd1d563dfe81b`；Python 3.14.3、Daft 0.7.2、Ray 2.55.0、PyArrow 22.0.0、Lance 7.0.0。
- 固定镜像 `f27a67a8ebb356b019297485c8d936bff0bb16f902c1538aa7c4c8a0b242c765`；`_cinderx.so` SHA256 `b3b1cf5b79563138008bd735ca1a90f023f2c10d3ba5f9f0a99c41831b595985`。未重编运行时或修改生产源码。
- pipeline 使用 CPU 304,306,308,310、memory node 0、28 GiB；独立 align 使用 CPU 368,370,372,374、memory node 2、10 GiB、1 分区。每块配置相同，线程预算固定；四核预算不能当成四个并发 UDF worker。
- 检查成对任务与实际参数/环境、源码/二进制/驱动 SHA、Docker CPU/内存配置、完整行数、输入身份、输出 schema 和内容摘要。pipeline 的 5M 输出 multiset 为 `d4e45fc351ac9b80e5fe8ea02ecd3b8099e3617be6bab199dc2c28246a65b892`，与旧语义结果相同。没有删除阶段或放宽正确性比较。

## 证据与复算

[纠偏 JSON](/opt/Codex/python-udf-jit/docs/reports/evidence/2026-09-07-blue53-ad-explicit-root-correction.json) · [纠偏 CSV](/opt/Codex/python-udf-jit/docs/reports/evidence/2026-09-07-blue53-ad-explicit-root-correction.csv) · [原三路径报告](/opt/Codex/python-udf-jit/docs/reports/2026-09-07-blue53-three-path-and-operator-performance.md)

原始记录：[/opt/Codex/python-udf-jit/.context/blue53-ad-root-correction-20260907](/opt/Codex/python-udf-jit/.context/blue53-ad-root-correction-20260907)。远端专属目录 `/home/lxy/udfjit-three-path-20260907-01a07ad8/root-correction`，包含任务配置、真实命令、stdout/stderr、exit status、sink、独立输入/输出证明和不参与计时的路径诊断。

```bash
python3 .context/blue53-ad-root-correction-20260907/collect_macro.py
python3 .context/blue53-ad-root-correction-20260907/collect_macro_evidence.py
python3 .context/blue53-ad-root-correction-20260907/summarize_correction.py
python3 .context/blue53-ad-root-correction-20260907/render_correction.py
```
