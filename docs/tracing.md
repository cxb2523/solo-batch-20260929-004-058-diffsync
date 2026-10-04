# 常驻同步追踪（Sync Tracing）

`diffsync/tracing.py` 在 **Store 协议** 之外包了一层装饰器 `TracedStore`，把
`LocalStore` 与 `RedisStore` 每一次 sync 的三件事固化成结构化事件 `SyncEvent`：

1. **diff 序列**：整棵 diff 的快照 `diff_snapshot` 与动作计数 `diff_summary`；
2. **父子应用顺序**：按“父 → 子（删除时尊重 `NATURAL_DELETION_ORDER` 的子 → 父）”
   推导出的 `expected_order`，以及每次落盘时仍打开的祖先帧；
3. **落盘结果**：逐条 `writes`（`add`/`update`/`remove` 是否被后端确认）、可回放前缀
   `replay_prefix`、以及 sync 后对目标端的**回读对照** `readback`。

事件保存在进程内、线程安全的 `SyncTracer` 里（常驻，跨请求保留），由
`server/trace_app.py` 通过 FastAPI 暴露。

启动：

```bash
uvicorn server.trace_app:app
```

接口：

- `POST /sync`：对 Local 与 Redis 两个后端各跑一轮 sync（同 `sync_id`），返回两个事件 id。
  可选 JSON：`{"round": 1}`；`round: 2` 且 `redis_fail_step: 2` 会让 Redis 的第 2 次增量写失败。
- `GET /trace`：全部事件（新→旧），支持 `?backend=local|redis` 与 `?sync_id=round-1`。
- `GET /trace/{event_id}`：单条事件（diff 快照、写序列、partial 标记、回读）。
- `GET /`：可回看的对照页面，两栏分别展示两个后端；点开单条事件可见 diff 快照、
  父子顺序、可回放前缀与 partial/fallback 标记。

事件状态字段：

- `status`：`success` / `partial` / `running`；
- `partial`：只要出现未确认的写，或回读与源不一致，即为 `true`；
- `fallback`：Redis 不可用、退化到 fakeredis 时为 `true`，并给出 `fallback_reason`。

下面三处取舍是**定死**的实现契约。

## 取舍一：增量写在第二步失败时算不算提交？

**结论：不算提交。事件标 `partial`，并给出“可回放前缀” `replay_prefix`。**

一次 sync 是一串按父子顺序下发的增量写（`add`/`update`/`remove`）。当第 N 步失败时：

- 只有在后端**已经确认**（`acknowledged=true`）的第 1…N-1 步算真正落盘；
- 第 N 步本身以及其后所有步骤都是未提交的；
- 事件 `status="partial"`、`partial=true`，`error` 记录失败原因；
- `replay_prefix` 只含已确认的写，按应用顺序排列，是安全的重放起点——重放它要么幂等
  （同值 `add/update`），要么直接把已落盘部分重建出来；**绝不能**从失败那步开始重放，
  因为那步没有 ACK，状态未知。

Redis 侧尤其如此：`SET`/`DEL` 是逐条独立命令，没有跨 key 事务。第 2 条命令失败时，
第 1 条已经在 Redis 里生效，这正是“前缀已提交、整体未完成”的部分提交。失败可由后端
真实错误产生，也可通过 `TracedStore.fail_on_write = N`（1 起）注入，演示与测试用它
稳定复现。失败后事件还会做一次回读，`readback.divergent` 明确列出与源状态的偏差
（`missing-in-destination` / `stale-in-destination` / `attrs-mismatch`）。

## 取舍二：父子 diff 顺序被打乱，错该由包装层还是 store 抛？

**结论：由包装层（`diffsync.tracing`）抛 `TraceOrderError`，store 保持“哑后端”。**

顺序是 sync 编排层的不变量，不是任何一个后端的职责：

- `LocalStore`/`RedisStore` 只回答“这个 key 在不在、写什么”，它们无法知道一条 child
  diff 是否落在正确的父窗口里；
- 因此在 `TracedStore` 拦截写操作时，用一个“仍在应用中的祖先帧栈”做校验：对
  `create`/`update`（以及非级联 `delete`），每一个声明了祖先的写，其祖先帧必须仍然打开，
  即子写必须严格发生在父的应用窗口之内；违反即 `TraceOrderError`。
- 正常的 diffsync DFS 同步天然满足该约束；一个会重排子树的自定义 `Diff`、或乱序回放，
  才会触发它。

删除是唯一的合法反序：当模型带 `NATURAL_DELETION_ORDER` 时，子先于父删除。
`expected_order` 对这种情况按“子 → 父”推导，帧上带 `natural_delete` 标记，删除写不做
“父必须打开”的检查，避免误报。

## 取舍三：同一 path 的 create→update→delete，折叠终态还是逐条留痕？

**结论：逐条留痕，绝不折叠。**

追踪层的职责是“可回看”，折叠成终态会丢失中间过程，也无法回答“第几步失败、前缀到哪”。
因此同一个 `(model, uid)` 在一次或多次 sync 里的 `create`、`update`、`delete` 都各自产生
一条 `WriteRecord`，顺序、祖先、ACK 与否全部保留。

这与“回读对照是否相等”的判定方式直接相关：正因为**不折叠**，我们不拿“想象中的终态”做
比较，而是在 sync 结束后**独立回读**目标端，与源端快照逐对象比对：

- 整串写都确认 → 回读与源相等（`readback.equal=true`），事件成功；
  中途失败 → 已确认前缀落盘、其余缺失，回读必然与源不等，`partial=true`。

也就是说：“终态相等”是**逐条落盘 + 回读**这一事实的结果，而不是靠折叠 diff 假设出来的。

### 为什么 UPDATE 也要显式落盘

diffsync 默认 syncer 的 UPDATE 只在内存里改对象、依赖“内存对象即存储对象”的身份语义
（LocalStore 成立）。对 pickle 型的 RedisStore，内存里改完不会回写，跨轮 update 会丢。
追踪层因此在 sync 时把每个成功的 UPDATE 额外走一遍 `store.update`：LocalStore 对同一对象
是 no-op，RedisStore 则真正持久化，并同样留下一条 `update` 写记录。`RedisStore` 也相应
维护了一个按 uid 的进程内身份缓存，使反序列化出的对象在同一进程内复用同一实例，避免
sync 把已存在于 Redis 的对象误判为“需要新建”。

## Redis 不可用：退化 fakeredis，绝不静默

`RedisStore` 建连 `ping` 失败（连接拒绝、超时、`OSError` 等）时，不抛
`ObjectStoreException` 终止，而是退化到进程内的 `fakeredis.FakeStrictRedis`：

- 实例上 `fallback=true`、`fallback_reason=<原始错误>`；
- 每个经过 `TracedStore` 的事件都带同样的 `fallback` / `fallback_reason`，页面显示
  `FALLBACK` 标记；
- 探测用 1 秒超时、最多 1 次重试，使退化快速发生；
- 若环境里没有安装 `fakeredis`，才抛 `ObjectStoreException`（明确提示安装）。

这样在没有真实 Redis 的机器上，`POST /sync` 仍能完整跑通 Redis 链路，且任何人都能从
事件和页面上一眼看出“这不是真 Redis”，不会被静默替换。
