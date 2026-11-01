# 数据库开发计划 (从 0 到 1 演进路线)

语言: Go。模块名 `mysqlfake`。每完成一个 Phase 提交一次代码。
若实现中发现计划与代码不一致，先修改本计划，再改代码。

## 总体约定 (设计决策)
- 页大小 4096 字节；页 0 为文件头页，`PageID=0` 同时作为"无效页"哨兵。
- 页头 16 字节: `type(1) pad(3) pageID(4) lsn(8)`，其余为 Payload。
- 所有上层模块通过 `storage.PageIO` 接口读写页 (Phase 1 由 DiskManager 实现，Phase 3 改由 BufferPool 实现)。
- 表 = 以主键为 key 的聚簇 B+ 树；每张表必须有且仅有一个单列 PRIMARY KEY (INT 或 VARCHAR)。
- 支持的数据类型: `INT` (int64)、`VARCHAR(n)`；支持 NULL。
- B+ 树 value 上限 1024 字节、key 上限 256 字节 (保证每个节点至少容纳 3 条记录)；Phase 4.3 起每行带 17 字节版本头，因此行数据本身不超过 1007 字节。
- 目录结构: `storage/` `tuple/` `btree/` `sql/` `catalog/` `executor/` `wal/` `buffer/` `txn/` `db/` `cmd/mysqlfake/`；Phase 5 起新增 `binlog/` `raft/` `cluster/`，Phase 7 `stats/` `optimizer/`，Phase 8 `server/` `vector/`，Phase 9 `backup/` `metrics/`。
- 工程约定: 严格按 Phase 顺序实现，每个 Phase 完成后单独提交；实现中发现计划与代码不一致，先改计划再改代码；每个 Phase 都带测试 (含 `-race`)。

## Phase 1: 存储与索引层 (单机文件 KV 引擎)
- [x] **1.1 磁盘与 Page 管理**
  - [x] 定义数据库文件二进制格式及 Magic Number (页 0 文件头: magic/version/pageSize/numPages/freeHead/catalogRoot/nextTxnID)
  - [x] 实现 Page 结构 (定义大小，如 4KB/8KB，划分 Header 与 Payload)
  - [x] 实现 DiskManager (封装按 Page 读写磁盘的 API)
  - [x] 实现空闲页分配器 (Free List/Bitmap) —— 采用 Free List: 空闲页的 Payload 前 4 字节存下一个空闲页
- [x] **1.2 记录与序列化**
  - [x] 定义 Tuple/Record 数据结构 (Schema/Column/Value)
  - [x] 实现 Record 序列化与反序列化逻辑 (内存对象与字节流转换: null 位图 + 定长/变长字段；含保序的主键编码)
- [x] **1.3 B+ 树索引基础**
  - [x] 定义 B+ 树节点结构 (Internal Node, Leaf Node；变长 key/value，按字节数而非条数分裂)
  - [x] 实现节点内二分查找
  - [x] 实现 B+ 树 Insert (包含叶子节点满时的 Split 逻辑；根页号固定不变)
  - [x] 实现 B+ 树 Search (点查与范围查询，范围查询使用叶子链表 + 迭代器)
  - [x] (可选) 实现 B+ 树 Delete (采用物理删除，**不做 Merge/再平衡**，叶子可为空)

## Phase 2: SQL 与执行层 (包上关系型外壳)
- [x] **2.1 解析与元数据**
  - [x] 接入/编写 SQL 语法解析器 (自写词法+递归下降；Phase 2 支持 CREATE TABLE / INSERT / SELECT(WHERE 含 AND/OR/比较) ；UPDATE/DELETE/事务语句在 Phase 4 扩展)
  - [x] 实现 Catalog 管理器 (定义系统表，持久化表名、列名、数据类型；系统表是根页记录在文件头 catalogRoot 的 B+ 树)
  - [x] 实现 Binder (校验 AST 中的表与列是否存在，字面量类型)
- [x] **2.2 基础执行器 (火山模型)**
  - [x] 定义基础 Executor 接口 (`Init()`, `Next()`)
  - [x] 实现 SeqScan Executor (全表扫描)
  - [x] 实现 IndexScan Executor (走 B+ 树查询；WHERE 含主键比较时由 Planner 选用)
  - [x] 实现 Insert Executor
  - [x] (配套) Filter / Projection Executor
- [x] **2.3 交互层**
  - [x] 开发 REPL 命令行终端
  - [x] 格式化输出查询结果集 (类似 MySQL 的 ASCII 表格)
- 已知限制: 多行 INSERT 在 Phase 4 事务回滚实现之前不保证语句原子性；WHERE 仅支持比较/IS NULL/AND/OR/NOT，无算术表达式。

## Phase 3: 内存与持久化 (Buffer Pool 与 WAL)
- [x] **3.1 缓冲池管理器**
  - [x] 实现 BufferPoolManager (内存页帧 Frame 管理)
  - [x] 实现 LRU 或 Clock 淘汰算法
  - [x] 为 Page 增加读写锁 (Latch)，保障内存多线程并发安全 (锁挂在页帧 Frame 上，因为 `storage.Page` 被按值拷贝使用)
  - [x] 改造引擎：所有读写不再直连 DiskManager，全部通过 Buffer Pool 拿 Page
- [x] **3.2 Redo Log (持久性)**
  - [x] 定义 WAL 日志格式 (LSN, TxnID, PageID, Offset, Before/After Image；物理日志，记录页内变化的字节区间)
  - [x] 实现 LogManager (追加写日志文件，实现 `fsync` 刷盘)
  - [x] 修改写入逻辑：遵循 Write-Ahead Logging 原则，先写日志再改内存页 (刷脏页前必须先刷日志到 pageLSN)
- [x] **3.3 崩溃恢复**
  - [x] 编写系统启动时的 Recovery 逻辑
  - [x] 实现正向回放 Redo Log，恢复宕机前未刷盘的数据页 (按页 LSN 保证幂等)
  - [x] (配套) Checkpoint: 刷全部脏页后截断日志
- 设计: BufferPool 位于 `buffer/` 包 (实现 `storage.PageIO`，读写均拷贝页内容)；WAL 与恢复位于 `wal/` 包；日志 LSN 为连续序号，日志文件头记录 baseLSN，Checkpoint 后截断日志但 LSN 不回退。
- 设计: 页写入时与帧内旧内容做字节 diff，生成若干 Update 日志 (Before/After)；新分配/释放的页首次写入记整页镜像，使重放不依赖页的旧内容。
- 设计: B+ 树持有固定的 PageIO，因此页更新日志的 TxnID 统一记 0 (系统)；事务归属由 Commit 日志记录 (TxnID 为语句/事务 ID)。
- 已知限制: 不保证 B+ 树多页修改 (分裂) 的原子性；崩溃时分配但尚未写入的页会泄漏；崩溃时未提交事务的回滚 (Undo 阶段) 不在范围内。

## Phase 4: 并发与事务层 (高并发与 ACID)
- 设计: 新增 `txn/` 包 (事务管理、锁、Undo、Read View)，`db.Session` 代表一个连接/会话；`Engine.Exec` 使用内置默认会话。
- 设计: 事务 ID 在 Begin 时分配，文件头 nextTxnID 以批量预留方式持久化；重启后此前的所有版本视为已提交且可见。
- 设计: 锁资源 = 表名 + 主键编码；锁在事务提交/回滚时统一释放 (严格 2PL)。
- 设计: 隔离级别 REPEATABLE READ —— 显式事务内首次快照读时建立 Read View 并沿用到事务结束，自动提交的语句每条新建 Read View。
  INSERT/UPDATE/DELETE 以及 `SELECT ... FOR SHARE / FOR UPDATE` 是"当前读": 加 X/S 锁后读取最新版本；普通 SELECT 是无锁快照读。
- 设计: 语句出错只回滚该语句 (按 Undo 保存点)；死锁/锁超时则回滚整个事务。DDL (CREATE TABLE 等) 会先隐式提交当前事务 (与 MySQL 一致)，自身不可回滚。
- [x] **4.1 锁与并发控制**
  - [x] 实现 LockManager (管理行级锁，共享锁 S / 排他锁 X，支持 S→X 升级，FIFO 排队)
  - [x] 在执行器层面集成两阶段锁协议 (2PL，严格 2PL: 提交/回滚时统一释放)
  - [x] 实现死锁检测 (超时中断或 Wait-for Graph 环路检测，采用 Wait-for Graph，并保留超时兜底)
  - [x] (配套) 扩展 SQL: UPDATE / DELETE / BEGIN(START TRANSACTION) / COMMIT / ROLLBACK、`SELECT ... FOR SHARE|FOR UPDATE`、表达式中的 `+ - *` 算术，及对应执行器
- [x] **4.2 Undo Log 与事务回滚**
  - [x] 定义并实现 Undo Log 格式 (内存中的逻辑 undo: Insert/Update/Delete + 修改前的整行存储值)
  - [x] 在执行 Update/Delete 前记录反向操作
  - [x] 实现 `ROLLBACK` 逻辑 (逆序执行 Undo Log 恢复数据；同时支持语句级回滚)
- [x] **4.3 多版本并发控制 (MVCC)**
  - [x] 重构 Tuple 格式：增加隐藏列 `Txn_ID` 与 `Rollback_Pointer` (及删除标记；DELETE 改为打删除标记，不再物理删除)
  - [x] 实现 Read View 生成逻辑 (获取当前活跃事务快照)
  - [x] 实现可见性判断逻辑：基于 Read View 决定读取哪个历史版本，实现无锁读
- 已知限制: Undo Log 仅存于内存 (包括已回滚事务的记录，并发读者可能仍在引用)，仅在没有活跃事务时整体清理 (无细粒度 purge)；重启后之前的版本一律视为已提交可见，崩溃时未提交事务的修改不会被回滚；
  无间隙锁/谓词锁，当前读不防幻读 (快照读由 MVCC 保证)；打了删除标记的行不会被物理清除。

---
*(以下为企业级特性扩展阶段)*

## Phase 5: 高可用与分布式复制 (HA & Replication)
- 设计: 复制单位是**行级逻辑日志 (Binlog)**: 事务提交时带上它产生的行事件 `Put(table,key,row) / Delete(table,key) / DDL(sql)`。事件幂等 (按序重放结果一致)，所以可被复制、重启重放、PITR 复用。
  事务在 `txn` 中随 undo 记录同步收集事件 (一条 undo 对应一个事件，语句回滚时一并截断)。
- 设计: 提交顺序 = Raft/复制 → Binlog 落盘 → WAL 提交记录 → 释放锁。复制失败则用 Undo 回滚本地事务，因此领导者不会留下"未复制却已提交"的数据 (同步模式)。
- 设计: `raft/` 实现完整 Raft (选主、日志复制、提交、持久化 term/vote/log)，传输层是接口 (进程内网络用于测试和分区模拟，另有 TCP/`net/rpc` 实现)；不做日志快照/压缩 (已知限制)。
- 设计: 租约 (Lease): 领导者只有在最近一个租约周期内收到过多数派心跳确认时才算"有效"，否则拒绝写入并降级；跟随者在租约期内收到过领导者心跳就不投票、不发起选举 (防止被隔离节点打断/脑裂)。
- 设计: 复制模式 `sync` (等多数派提交，默认)、`semisync` (至少 N 个副本确认，超时退化为异步)、`async` (不等待)。
  异步/半同步下旧领导者可能提交过被新领导者覆盖的事务 —— 检测到后将本节点标记为 `Diverged` 并拒绝写入，需要重新同步 (已知限制)。
- 设计: 跟随者按 Raft 日志顺序应用事件 (事务内原子、MVCC 可见)；跟随者是只读副本，写语句返回 `ErrNotLeader` (带领导者提示)，读可选择等待追平到指定日志位置。
- [x] **5.1 复制协议集成**
  - [x] 实现 Binlog (逻辑日志)：事件格式、文件追加 + fsync、按序号读取/订阅
  - [x] 引入 Raft 一致性协议模块 (选主/复制/提交/持久化)，将单机日志转为分布式日志
  - [x] 集群节点 `cluster.Node`: 领导者经 Raft 提交事务，跟随者应用已提交日志
- [x] **5.2 故障转移与集群管理**
  - [x] 实现基于租约 (Lease) 的自动选主逻辑，防止脑裂
  - [x] 支持异步/半同步复制机制配置
  - [x] 实现只读节点 (Read Replica) 逻辑，分担主库查询压力
- 已知限制: Raft 无日志快照，新节点只能从头追日志；节点重启后未提交事务的页修改仍按 Phase 3 的限制处理；异步模式可能丢数据。

## Phase 6: 存储引擎边界防护 (Engine Hardening)
- 设计: 页头扩展为 20 字节，新增 4 字节 CRC32 (文件格式版本升到 2，不兼容旧文件)；校验和在落盘时写入、读盘时校验，全零页视为未写过。
- 设计: Doublewrite: 刷脏页时先把整批页顺序写入 `<db>.dw` 并 fsync，再写数据文件；启动时在 WAL 重放之前扫描 `.dw`，数据页校验失败 (撕裂) 则用 `.dw` 中完好的副本覆盖。
- 设计: Fuzzy Checkpoint: 页帧记录 `recLSN` (首次弄脏时的 LSN)；后台协程在不阻塞写入的前提下分批刷脏页 (持页读锁拷贝)，Checkpoint LSN = 仍脏页的最小 `recLSN`，WAL 只丢弃 LSN 小于它的前缀。原有"全停顿"Checkpoint 仅用于关闭/打开。
- 设计: B+ 树改为页级锁存器 (Latch) + 螃蟹步 (Crab Walking)：读操作逐级加 R 锁；写操作先乐观 (只对叶子加 X 锁)，叶子可能分裂才退回悲观 (沿路径加 X 锁，遇到"安全"节点释放祖先)；范围扫描沿叶子链手递手加锁。去掉树级读写锁。
- [x] **6.1 物理层安全**
  - [x] 实现 Doublewrite Buffer (双写缓冲)，防止 Linux 4KB 落盘导致的页撕裂
  - [x] 为所有 Page 加入 Checksum 校验机制
- [x] **6.2 异步落盘优化**
  - [x] 实现 Fuzzy Checkpoint (模糊检查点)，根据 LSN 后台异步刷盘，不阻塞写操作
- [x] **6.3 索引并发优化**
  - [x] 升级 B+ 树为 B-link 树或引入 Crab Walking (螃蟹步) 加锁协议，缓解根节点锁争用 (采用 Crab Walking + 乐观叶子加锁)

## Phase 7: 智能查询优化 (Query Optimizer)
- 设计: 为支撑本阶段扩展 SQL: `FROM a [AS x] JOIN b ON ... JOIN c ON ...` / `FROM a, b WHERE ...`、限定列名 `x.col`、`ORDER BY col [DESC]`、`LIMIT n`、`EXPLAIN SELECT ...`、`ANALYZE TABLE t`；仅支持 INNER JOIN 与单表之外的只读查询 (FOR UPDATE 仍只限单表)。
- 设计: 统计信息: 对表做蓄水池采样 (默认最多 10000 行) 后得到行数、每列 NDV/空值比例/最小最大值、**MCV (高频值) 列表**和直方图。直方图支持等高与等宽两种，分析器检测数据倾斜后自动选择 (倾斜用等高)。
  统计信息放内存；后台守护协程按表的修改计数 (超过阈值或从未分析) 周期性重新采样。
- 设计: 代价模型 `cost = IO代价×页数 + CPU代价×行数`；选择率由 MCV + 直方图插值估算 (无统计时用默认值)；访问路径在 SeqScan / 主键 IndexScan 之间按代价选择。
- 设计: Join 重排用动态规划 (Selinger，按表集合位掩码)，每个子集保留"每种有序性下的最优计划"做剪枝，优先枚举有连接条件的子集，只有图不连通才允许笛卡尔积；
  物理 Join: NestedLoop (兜底、非等值)、HashJoin (小表建哈希，内存内)、SortMergeJoin (输入已按主键有序则免排序)。
- [x] **7.1 统计信息搜集**
  - [x] 开发后台守护进程，定期采样表数据
  - [x] 构建列数据的等高/等宽直方图 (Histogram) 以处理数据倾斜
- [x] **7.2 基于成本的优化 (CBO)**
  - [x] 构建代价估算模型 (计算 CPU Cost 与 I/O Cost)
  - [x] 集成 Cascades 或动态规划优化框架 (采用动态规划)
- [x] **7.3 复杂算子与 Join 重排**
  - [x] 实现 Hash Join 和 Sort-Merge Join 算子
  - [x] 在优化器中实现多表 Join 的最优路径剪枝与重排
  - [x] (配套) SQL 扩展 (JOIN/别名/限定列/ORDER BY/LIMIT/EXPLAIN/ANALYZE) 及 Sort/Limit/NestedLoop 算子
- 已知限制: 仅 INNER JOIN；Hash Join / Sort 全内存不落盘；统计信息不持久化；无聚合与 GROUP BY。

## Phase 8: 极致计算与网络模型 (High-Performance Execution)
- 设计: 服务端实现 MySQL 协议子集 (握手 v10、`COM_QUERY`/`COM_PING`/`COM_QUIT`/`COM_INIT_DB`、文本结果集、OK/ERR 包)，认证不校验口令。
- 设计: Linux 用 `epoll`、macOS/BSD 用 `kqueue` (均基于标准库 `syscall`，非阻塞 socket + 单事件循环 + 每连接状态机和读写缓冲)；其他平台退化为 goroutine-per-connection。
  事件循环只做 I/O 和协议拆包，完整请求投递给动态工作池执行，结果写回缓冲并通过 `EPOLLOUT` 冲刷。同一连接的请求串行。
- 设计: 动态工作池 (min/max 工作协程，积压时扩容，空闲超时缩容)，限制并发执行的语句数；"线程"在 Go 里即 goroutine，运行时再复用到 OS 线程。
- 设计: 向量化: 新增批 (Batch) 接口 `NextBatch()`，列式存放 (INT/VARCHAR 向量 + 空值位图 + 选择向量)，批大小 1024；
  向量化算子: Scan、Filter (按列核函数，AND/OR/NOT/比较/IS NULL，三值逻辑；不支持的形状如算术整体退回逐行求值)、Projection、HashJoin 探测、Limit；其余算子 (NestedLoop/MergeJoin/Sort) 经行/批适配器接入。
  向量化是 SELECT 的默认执行模式，`optimizer.Options.RowMode` 可回退到行式；两种模式结果必须一致 (有对拍测试)。
- [x] **8.1 网络连接池与多路复用**
  - [x] 废弃“一连接一线程”，底层接入 `epoll`/`kqueue` 事件驱动模型
  - [x] 实现动态线程池处理客户端并发请求
  - [x] (配套) MySQL 协议子集编解码与测试客户端
- [x] **8.2 现代计算引擎**
  - [x] 重构执行器，实现向量化执行 (Vectorized Execution)，按列块(Batch)而非单行处理数据
  - [ ] (可选) 引入 LLVM JIT 编译执行计划，消除火山模型的虚函数开销 —— **不实现**: Go 没有可用的 LLVM 绑定，改由向量化核函数消除逐行虚调用。
- 已知限制: 不实现 TLS 与口令认证；预处理语句协议 (`COM_STMT_*`) 不支持。

## Phase 9: Day-2 运维与可观测性 (Operations & Observability)
- 设计: 二级索引: `CREATE INDEX name ON t(col)` (非唯一，单列，NULL 不入索引)。索引树的 key = 列值保序编码 + 主键，value 为空；
  索引项只增不删 (回滚时仅撤销本次新增的项)，读取时回表取可见版本并校验列值仍匹配，因此对 MVCC 安全。
  索引记录创建时的事务号 `ValidFrom`: 只有在索引创建之后建立的 Read View 才能用它做快照读 (旧视图可能需要未被索引的旧值)，当前读 (UPDATE/DELETE/FOR UPDATE) 不受限。
  优化器的访问路径加入二级索引 (代价模型: 索引范围 I/O + 每行回表)；UPDATE/DELETE/加锁读也可用索引定位候选行。
- 设计: Online DDL (影子拷贝 + 增量回放): `ALTER TABLE t ADD COLUMN c type [DEFAULT lit]` 与 `CREATE INDEX`。
  ① 订阅提交事件 (复用 Phase 5 的行事件) 开始捕获 → ② 用快照分批拷贝到影子表/影子索引 → ③ 回放捕获的增量 (多轮至队列很小) → ④ 对表加元数据锁 X (等待已触及该表的事务结束，期间新事务短暂阻塞) → 回放最后增量 → ⑤ 在 Catalog 中原子切换 → ⑥ 写 DDL 事件 (经 Raft 复制，失败则在持有 X 锁时撤销切换) → 释放。
  元数据锁 (MDL): 写事务在首次触及表时 (先于读取 TableInfo) 对 `mdl:<表>` 加 S 锁直到事务结束；读语句不加。同一时刻只允许一个在线 DDL。集群中 DDL 以 DDL 事件复制，跟随者同步执行。
- 设计: 热备份 (物理，模糊但可重放): 先做 Fuzzy Checkpoint 并暂停 WAL 截断 → 拷贝数据页 (校验和失败则重读) → **短暂静默写事务** (等已经写过数据的事务结束，新的写入排队；因为恢复只做 redo、不回滚未提交事务，WAL 拷贝里不能有未结束的事务)，在静默期间记录 binlog 位置、刷日志并拷贝 WAL 与 binlog，**最后**拷贝文件头页 → 清单 `MANIFEST.json`。
  增量备份只拷贝 `页 LSN > 上次备份 endLSN` 的页。还原 = 基础备份 + 依次覆盖增量页 + 把链上所有备份的 WAL 合并 (按 LSN 去重)，打开数据库时由崩溃恢复完成重放。
  包划分: `backup/` 放备份格式与文件级操作 (清单、页覆盖、WAL 合并)，与引擎交互的部分 (`Engine.Backup`、`RestoreTo`/PITR) 在 `db/`。
- 设计: PITR: 在还原后的库上重放 Binlog (按序号/提交时间戳) 直到目标时间点；目标时间必须不早于最后一份备份的完成时间。SQL 入口: `BACKUP TO 'dir' [INCREMENTAL FROM 'basedir']`；还原由离线命令行 `mysqlfake restore` 完成。
- 设计: 观测: `metrics/` 提供计数器/仪表/直方图并以 Prometheus 文本格式在 HTTP `/metrics` 暴露 (语句数/QPS、缓冲池命中、锁等待、事务提交/回滚/死锁、WAL 字节与 fsync、连接数、Raft 角色/任期/提交位置等)；
  慢查询日志按阈值写文件 (耗时、扫描行数、返回行数、阶段耗时)；Profiling: 每条语句的阶段耗时 (parse/plan/execute/锁等待) 和扫描/返回行数 (`Session.LastProfile`)，`EXPLAIN ANALYZE` 给出逐算子的实际行数与耗时。
- [x] **9.1 在线变更 (Online DDL)**
  - [x] 实现二级索引 (存储、DML 维护、回滚、优化器访问路径)
  - [x] 实现无锁加列、建索引机制 (影子表拷贝 + 增量日志回放) 及元数据锁
- [x] **9.2 备份与恢复**
  - [x] 实现基于 LSN 的物理热备份 (Hot Backup) 工具，支持全量与增量备份
  - [x] 支持 PITR (Point-in-Time Recovery)，按时间点恢复数据
- [x] **9.3 系统观测**
  - [x] 接入 Prometheus 兼容的 Metrics 暴露接口 (QPS, 缓冲池命中率, 锁等待)
  - [x] 实现 Slow Query Log (慢查询日志) 与执行耗时细粒度 Profiling
- 已知限制: `ADD COLUMN ... DEFAULT` 只用于回填已有行，之后的 INSERT 未指定该列时为 NULL；二级索引项不清理 (无 purge)；热备份结束阶段会短暂静默写事务 (等待已写入的事务结束，最长 30 秒，否则备份失败)；Profile 的 Plan 耗时只统计 SELECT/EXPLAIN；无 DROP INDEX/DROP COLUMN；备份期间 WAL 不截断，磁盘占用上升；PITR 目标点之前的备份必须存在；
  Online DDL 切换时刻之前开启的快照事务，切换后会读到切换时刻的数据 (不保持其快照)；集群里 DDL 复制失败时只能在 X 锁内撤销。

## Phase 10: 崩溃恢复完善 —— 回滚未提交事务 (ARIES 式 Analysis / Redo / Undo)
- 背景: Phase 3 的恢复只做 redo，且无条件重放全部 Update 记录 (页更新日志的 TxnID 记 0，缓冲池不知道事务)，Phase 4 的 Undo Log 只在内存里。
  结果: 崩溃时已经写出日志、但没有提交的事务，其修改会留在数据里 (见 `db/undo_recovery_test.go`)。本阶段补上 Undo 阶段，消除这条已知限制。
- 设计: 恢复分三个阶段。Analysis: 顺序扫描日志找出"失败者"；Redo: 沿用现有逻辑，重放**全部**历史 (含失败者的修改，页 LSN 保证幂等)，使页回到崩溃瞬间的样子；
  Undo: 按 LSN 倒序撤销失败者的修改。先重做再撤销，是为了让 Undo 面对的永远是一个一致的现场。
- 设计: Undo 信息持久化。新增日志类型 `RecUndo`，由 `Txn.AddUndo` 在修改数据**之前**写入日志，内容为 `{TxnID, Kind, 主键树根页号, Key, OldValue, IdxAdds[(索引树根页号, Key)]}`。
  按 WAL 规则，任何已落盘的页修改，其 `RecUndo` 一定先于它落盘 (页刷盘前必须先把日志刷到页 LSN，而 `RecUndo` 的 LSN 更小)。
  `txn.Logger` 增加写 Undo 记录的方法；内存 `UndoLog` 保留 (MVCC 版本链与运行期回滚仍然使用它)。树用**根页号**标识 (Phase 1 起根页号固定不变)，恢复时 `btree.Open(pool, root)` 即可，不依赖 Catalog。
- 设计: 必须是**逻辑** Undo，不能用 Update 记录里的 Before 镜像做物理撤销: Before 是页内字节区间，同一页上别的事务之后可能插入键、导致字节整体移动或页分裂，
  把旧字节写回原偏移会破坏页。行锁保护的是行，不是页内字节位置。因此撤销 = 对 B+ 树执行 `Put(Key, OldValue)` / `Delete(Key)`，并删除 `IdxAdds` 中本次新增的索引项。
- 设计: Analysis 规则: 出现 `RecUndo` 的事务进入活跃表；遇到该事务的 `RecCommit` 或 `RecAbort` 则移出；扫描结束仍在表中的就是失败者。
  已有 `RecAbort` 的事务不再撤销: 回滚过程对 B+ 树的修改也走缓冲池并记了日志，重做历史即得到回滚后的状态。`Abort` 记录不强制刷盘；若它丢失而回滚的页修改已落盘，该事务会被当作失败者再撤销一遍，结果相同 (见下条)。
- 设计: 撤销操作都是"绝对值"写入 (写回旧值 / 删除键)，按倒序把整个撤销列表完整执行一遍，每个键的最后一次写入来自它最早的 undo 记录，因此重复执行是幂等的；失败者持有的行锁直到崩溃都未释放，没有其他事务能改同一个键，所以不会与别的事务的修改交错。
  恢复可重启: Undo 期间再次崩溃，失败者仍没有 `RecAbort`，下次恢复从头再撤销。全部撤销完成并落盘后才为每个失败者写 `RecAbort`。
- 设计: 恢复入口调整。`wal.Recover` 只做 Analysis + Redo，**不再截断日志**，返回失败者及其 undo 记录 (倒序)；`Engine.Open` 在 `catalog.Open` 之后执行 Undo (撤销期间的页修改同样记日志)，写 `RecAbort`、`FlushAll`，
  最后沿用 `Open` 里已有的 Checkpoint 截断日志。
- 设计: 日志截断下界。Fuzzy 与 Sharp Checkpoint 当前都会丢弃日志前缀，若此时有活跃的写事务，它的 `RecUndo` 会被丢掉，而它修改过的脏页已刷盘，崩溃后就无法撤销。
  `txn.Manager` 记录每个写事务的第一条 `RecUndo` 的 LSN，缓冲池的截断点取 `min(原截断点, 所有活跃写事务的最小 firstUndoLSN)`；保留下来的旧日志重放时由页 LSN 跳过。
  优雅关闭 (`Engine.Close`) 时仍有打开的事务: 与崩溃走同一条路径，Checkpoint 保留这些事务的日志，下次 `Open` 撤销。
- 设计: 范围与不变项。热备份的写入静默 (`Quiesce`) 保持不变 (不再是正确性所必需，但仍使备份内容更干净)；DDL 不产生 undo 记录，崩溃发生在 DDL 期间的行为不变；
  从节点应用复制日志同样经过 `txn.Manager`，自动获得此能力；Raft 日志与 Binlog 只在提交时生成，失败者不会出现在其中。
- 测试: `db/undo_recovery_test.go`，验收场景见各测试注释。未实现的步骤用 `pending()` 标记，默认跳过，`MYFAKE_PENDING=1` 强制运行 (用于确认它们现在失败)；完成对应步骤时删除标记。
- [ ] **10.1 Undo 日志持久化与 Analysis**
  - [ ] 新增 `RecUndo` 日志记录 (编码/解码/恢复读取)，`Txn.AddUndo` 先写日志
  - [ ] Analysis: 从日志找出失败者及其 undo 记录
- [ ] **10.2 Undo 阶段**
  - [ ] `wal.Recover` 只做 Analysis + Redo 且不截断；`Engine.Open` 在 Catalog 就绪后逻辑撤销失败者 (含二级索引项)，写 `RecAbort` 后 Checkpoint
- [ ] **10.3 检查点与关闭的联动**
  - [ ] 记录 firstUndoLSN，Fuzzy/Sharp Checkpoint 的截断点不越过活跃写事务的 undo 记录；优雅关闭保留未结束事务的日志
- [ ] **10.4 可重启与压力测试，并更新旧限制**
  - [ ] Undo 期间崩溃可重启的测试 (需要在撤销过程中注入崩溃的钩子)；多事务并发 + 随机崩溃的不变量测试 (转账总额)
  - [ ] 更新 Phase 3 / 4 / 5 / 8 / 9 中"崩溃时未提交事务不回滚"的已知限制措辞
- 已知限制 (完成后仍然存在): 没有 CLR (补偿日志记录)，撤销期间再崩溃要从头再撤销一遍而不是接着上次的进度；Undo 信息是逻辑记录，每次修改多写一条日志 (写放大)；
  DDL 期间崩溃仍可能泄漏页或留下半成品；`Commit` 时 Binlog 已追加而 WAL 提交记录尚未落盘就崩溃，该事务在 Binlog 里存在、数据却被撤销 (与 MySQL 的 binlog/redo 两阶段提交问题同类，不处理)；
  不实现 Savepoint 嵌套和 Nested Top Action，B+ 树分裂等结构修改的原子性仍不保证 (Phase 3 已知限制)。
