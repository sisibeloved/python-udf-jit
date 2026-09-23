# CinderX UDF-JIT 运行时补丁系列

本目录以确定性补丁系列的形式携带 UDF-JIT 线的 CinderX 改动，与
`vendor/cinderx/patches/`（标量主线系列）平行：两者基于同一上游提交
`ac09c68527153b43cc8b4f16f36d9245cb861d12`，但内容线不同且不可相互
叠加，构建时按运行时线二选一。blue-204 的 real-KenLM 基准与
pyperformance 验证所用运行时（`_cinderx.so` sha256 前缀 `f9a2916f`，
修复版 `b2bc369d`、瞬态闭包修复版 `85a8eeac`）来自本系列。

按 `manifest.json` 顺序应用补丁后，源码树等价于 cinderx 仓
`feat-udf-jit` 分支（`7214f89d`）叠加第 5、6、7 三个补丁的修复；该分支本身
不作为集成线启用，内容以本系列为准。

## 补丁内容

1. **0001-udf-continuation-dataflow**：UDF 续体与数据流基础（对应
   feat-udf-jit `8286d87c`）。
2. **0002-typed-udf-regions-generic-hir**：typed UDF region 经 generic
   HIR 降低（对应 `118dfa38`），含 `makeUdfInvariantState` /
   `makeUdfValueState` 的 HIR 构建接缝。
3. **0003-osr-test-cleanup**：移除重复的 OSR 异利用例（对应
   `a88455be`）。
4. **0004-guarded-exact-value-reuse**：守卫式精确值复用（对应
   `7214f89d`）。
5. **0005-udf-cache-descriptor-nullcheck**：修复
   `makeUdfInvariantState`/`makeUdfValueState` 对 `func_dict` 中无
   UDF cache 属性的普通函数直接把 `PyDict_GetItemString` 的 NULL
   返回值传入 `PyDict_CheckExact` 的空指针解引用。autojit 强制编译
   路径（`PYTHONJITAUTO=auto:2`：`forcedJitVectorcall ->
   compilePreloader -> buildHIRImpl`，`frame_state == nullptr`）会把
   任意单参普通函数送进该门控，pyperformance 全套 96/96 SIGSEGV；
   默认 tier-2 自然升级路径带 frame_state 不经过该门控。gdb 栈与
   修复验证（nbody 55.2ms ± 2.6ms 对齐基线 54.5ms）见 blue-204
   `/root/pyperf-results/gdb-nbody.log`。
6. **0006-udf-transient-closure-compile-reuse**：修复 autojit 门控下
   瞬态闭包（如 `copy._reconstruct` 内 `(_deepcopy(arg, memo) for arg
   in args)` 的 genexpr 工厂，每次实例深拷贝新建、单参调用一次、用后
   即弃）被逐实例重复编译的问题。每个新实例都会重入
   `compile_func`，而编译缓存查询与确定性失败缓存检查位于
   `compilePreloader` 内部、在昂贵的 `preloadFuncAndDeps`（递归
   preload 整个依赖图，实测约 187us）之后，且编译产物仅锚定在垂死
   实例上（无外层函数锚），实例死亡即被拆除。三处协同修复：
   `Context` 新增按 code 生命周期的 `anchored_compiled_funcs_`
   强引用锚表（`makeCompiledFunction` 在无外层锚时锚定，
   `codeDestroyed`/`forgetCode`/`~Context` 对应回收）；
   `compile_func` 在 preload 之前先查确定性失败缓存与
   `lookupCode`（命中即 `finalizeFunc` 直接挂载，处理多线程编译的
   deferred finalization 分支）；`scheduleJitCompile` 对已记录确
   定性失败的 code 不再调度新实例。效果：实例深拷贝 191us -> 5.1us
   （基线 4.8us），pyperformance `deepcopy` 子项 5.82ms -> 275us
   （基线 264ms 口径下 21.84x 回归消除）。验证证据见 blue-204
   `/root/pyperf-results/dbg1/`（探针与 gdb 计数）与本仓
   `staging` 同步副本。
7. **0007-udf-closure-spec-compile-budget**：修复捕获兄弟嵌套函数的
   闭包被逐实例重复编译的问题。`closureFunctionTargetSignature` 将
   闭包中函数值的实例身份编入编译缓存键（服务 frozen-closure 内联
   的精确目标语义），而 `traceback._extract_caret_anchors_from_line_segment`
   内的 `increment_until` 等闭包每次外层调用都捕获新建的兄弟函数
   （`nextline`/`increment`），签名逐次不同（调试计数实测 101 次调用
   101 个签名、缓存零命中、逐次完整重编约 2.4ms），异常格式化密集
   的负载（如 sphinx 每次构建吞异常打 traceback）因此成倍变慢。
   修复：`Context` 新增按 code 的非零 specialization 编译计数，
   超过预算（4 次）即冻结为解释执行——`compile_func` 早查拒编、
   `scheduleJitCompile` 停止调度新实例（`closure_spec_budget_skip`
   计数）、`codeDestroyed` 回收计数；spec=0 的瞬态闭包（deepcopy
   0006 场景）不受影响。效果：caret 锚点计算稳态 2414us -> 27.1us
   （base 26.7us），pyperformance `sphinx` 7.21ms 对齐 base 7.15ms
   （3.23x 回归消除）。

## 与标量主线系列的关系

| 系列 | 目录 | 内容 | 状态 |
|------|------|------|------|
| 标量主线 | `vendor/cinderx/patches/`（0001–0006） | typed-loop 特化、序列内建、续体去优化、WX 双映射 | 既有 |
| UDF-JIT | `vendor/cinderx/udf-jit/`（0001–0007） | UDF 续体/数据流、generic HIR typed region、守卫缓存、空指针修复、瞬态闭包编译复用、闭包特化编译预算 | 本系列 |

`manifest.json` 沿用与标量主线相同的字段语义（逐补丁 sha256、
`patch_series_sha256` 为按序拼接补丁字节的 sha256、`changed_file_count`
为系列触及文件的并集计数）；本系列不携带 runtime tree 指纹字段，
运行时构建产物指纹由构建线另行记录。
