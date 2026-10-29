# 数据库开发计划 (从 0 到 1 演进路线)

语言: Go。模块名 `mysqlfake`。每完成一个 Phase 提交一次代码。
若实现中发现计划与代码不一致，先修改本计划，再改代码。

## 总体约定 (设计决策)
- 页大小 4096 字节；页 0 为文件头页，`PageID=0` 同时作为"无效页"哨兵。
- 页头 16 字节: `type(1) pad(3) pageID(4) lsn(8)`，其余为 Payload。
- 所有上层模块通过 `storage.PageIO` 接口读写页 (Phase 1 由 DiskManager 实现，Phase 3 改由 BufferPool 实现)。
- 表 = 以主键为 key 的聚簇 B+ 树；每张表必须有且仅有一个单列 PRIMARY KEY (INT 或 VARCHAR)。
- 支持的数据类型: `INT` (int64)、`VARCHAR(n)`；支持 NULL。
- 单行序列化后不超过 1024 字节，主键不超过 256 字节 (保证每个 B+ 树节点至少容纳 3 条记录)。
- 目录结构: `storage/` `tuple/` `btree/` `sql/` `catalog/` `executor/` `wal/` `txn/` `db/` `cmd/mysqlfake/`。

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
- [ ] **3.1 缓冲池管理器**
  - [ ] 实现 BufferPoolManager (内存页帧 Frame 管理)
  - [ ] 实现 LRU 或 Clock 淘汰算法
  - [ ] 为 Page 增加读写锁 (Latch)，保障内存多线程并发安全
  - [ ] 改造引擎：所有读写不再直连 DiskManager，全部通过 Buffer Pool 拿 Page
- [ ] **3.2 Redo Log (持久性)**
  - [ ] 定义 WAL 日志格式 (LSN, TxnID, PageID, Offset, Before/After Image；物理日志，记录页内变化的字节区间)
  - [ ] 实现 LogManager (追加写日志文件，实现 `fsync` 刷盘)
  - [ ] 修改写入逻辑：遵循 Write-Ahead Logging 原则，先写日志再改内存页 (刷脏页前必须先刷日志到 pageLSN)
- [ ] **3.3 崩溃恢复**
  - [ ] 编写系统启动时的 Recovery 逻辑
  - [ ] 实现正向回放 Redo Log，恢复宕机前未刷盘的数据页 (按页 LSN 保证幂等)
  - [ ] (配套) Checkpoint: 刷全部脏页后截断日志
- 已知限制: 不保证 B+ 树多页修改 (分裂) 的原子性；崩溃时未提交事务的回滚 (Undo 阶段) 不在范围内。

## Phase 4: 并发与事务层 (高并发与 ACID)
- [ ] **4.1 锁与并发控制**
  - [ ] 实现 LockManager (管理行级锁，共享锁 S / 排他锁 X，支持 S→X 升级)
  - [ ] 在执行器层面集成两阶段锁协议 (2PL，严格 2PL: 提交/回滚时统一释放)
  - [ ] 实现死锁检测 (超时中断或 Wait-for Graph 环路检测，采用 Wait-for Graph)
  - [ ] (配套) 扩展 SQL: UPDATE / DELETE / BEGIN / COMMIT / ROLLBACK 及对应执行器
- [ ] **4.2 Undo Log 与事务回滚**
  - [ ] 定义并实现 Undo Log 格式 (内存中的逻辑 undo: Insert/Update/Delete + 旧值)
  - [ ] 在执行 Update/Delete 前记录反向操作
  - [ ] 实现 `ROLLBACK` 逻辑 (逆序执行 Undo Log 恢复数据)
- [ ] **4.3 多版本并发控制 (MVCC)**
  - [ ] 重构 Tuple 格式：增加隐藏列 `Txn_ID` 与 `Rollback_Pointer` (及删除标记；DELETE 改为打删除标记)
  - [ ] 实现 Read View 生成逻辑 (获取当前活跃事务快照)
  - [ ] 实现可见性判断逻辑：基于 Read View 决定读取哪个历史版本，实现无锁读
- 已知限制: Undo Log 仅存于内存、不做 purge；重启后之前的版本一律视为已提交可见；无间隙锁，不防幻读 (快照读由 MVCC 保证)。
