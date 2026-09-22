# CinderX UDF-JIT 运行时补丁系列

本目录以确定性补丁系列的形式携带 UDF-JIT 线的 CinderX 改动，与
`vendor/cinderx/patches/`（标量主线系列）平行：两者基于同一上游提交
`ac09c68527153b43cc8b4f16f36d9245cb861d12`，但内容线不同且不可相互
叠加，构建时按运行时线二选一。blue-204 的 real-KenLM 基准与
pyperformance 验证所用运行时（`_cinderx.so` sha256 前缀 `f9a2916f`，
修复版 `b2bc369d`）来自本系列。

按 `manifest.json` 顺序应用补丁后，源码树等价于 cinderx 仓
`feat-udf-jit` 分支（`7214f89d`）叠加第 5 个补丁的修复；该分支本身
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

## 与标量主线系列的关系

| 系列 | 目录 | 内容 | 状态 |
|------|------|------|------|
| 标量主线 | `vendor/cinderx/patches/`（0001–0006） | typed-loop 特化、序列内建、续体去优化、WX 双映射 | 既有 |
| UDF-JIT | `vendor/cinderx/udf-jit/`（0001–0005） | UDF 续体/数据流、generic HIR typed region、守卫缓存、空指针修复 | 本系列 |

`manifest.json` 沿用与标量主线相同的字段语义（逐补丁 sha256、
`patch_series_sha256` 为按序拼接补丁字节的 sha256、`changed_file_count`
为系列触及文件的并集计数）；本系列不携带 runtime tree 指纹字段，
运行时构建产物指纹由构建线另行记录。
