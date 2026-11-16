# 测序中心归档回收 API（Archive Reclamation Service）

按批次号管理测序 ZIP 归档的 FastAPI + SQLite 服务。两个实例共享同一数据卷，
内容按 SHA-256 去重，批次撤下后只回收**已无任何有效批次引用**的原件，并支持
进程崩溃后重启续清。

## 功能与接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches/{batch_id}/archive` | 上传 ZIP（≤ 2 MiB），multipart 字段 `file` |
| GET  | `/batches/{batch_id}/archive` | 下载该批次的原始字节 |
| POST | `/batches/{batch_id}/retire` | 撤下批次（幂等），随后内联执行回收 |
| POST | `/gc/run` | 手动触发标记-清扫式垃圾回收 |
| GET  | `/batches?status=active|retired` | 批次摘要列表 |
| GET  | `/batches/{batch_id}` | 单个批次摘要 |
| GET  | `/storage/summary` | 台账核对：批次数、blob 行数、磁盘原件数等 |
| GET  | `/healthz` | 健康检查 |
| GET  | `/docs` | Swagger UI（OpenAPI 文档） |

### 业务规则

- 上传仅接受合法 ZIP，单批次上限 **2 MiB**（超限返回 413，非 ZIP 返回 400）。
- 相同 SHA-256 内容在共享卷上**只存一份**（`blobs/<sha256>`），多批次共用。
- 同号同内容重传：返回 200 且 `reused_existing_content=true`。
- 同号异内容：拒绝并返回 **409**，原内容不受影响。
- 撤下的批次号**永久不可复用**：再传返回 409，新下载返回 **404**。
- 回收只删除无 active 批次引用的原件；仍被其他批次共用的原件保留。
- 下载一旦开始即持有独立文件描述符流式传输，**不持有数据库事务/锁**；
  即使期间原件被移入隔离区或删除（POSIX 语义），慢速下载仍可完整结束，
  且不会阻塞其他批次的撤下与回收。

## 并发与崩溃安全设计

- SQLite 启用 **WAL** 与 `busy_timeout`；所有写操作在单个
  `BEGIN IMMEDIATE` 事务内完成判定与写入，两个实例靠写锁串行化，
  不会互相读到半成品状态。
- 上传顺序：先写 `tmp/` 临时分片 → 事务内插入/认领 blob 行与批次行并提交
  → 再原子 `rename` 到 `blobs/`。行先于文件存在，回收扫描不会误判新上传。
- 回收分两阶段：
  1. **标记**：把无 active 引用的 ready blob 写入 `gc_queue`；
  2. **清扫**：事务内复查引用后置 `deleting`，文件移入 `quarantine/`，
     再在第二个事务内删行并删除隔离文件。每一步都幂等，两个实例可并发回收。
- 若清扫中内容被新批次重新引用（同哈希上传），事务内复查会把行恢复为
  `ready` 并把隔离文件移回，内容得以保留。
- 进程在任何阶段退出后，重启时 lifespan 调用 `reconcile()`：
  清理临时分片、续清 `deleting` 残留、重新被引用的内容恢复、
  清理无主隔离文件，然后继续常规标记-清扫。
- `gc_queue` 与 `deleting` 状态持久化在 SQLite 中，回收进度跨重启可续。

## 数据卷布局

```
/data
├── archives.db          # SQLite（WAL 模式，含 -wal/-shm 旁车文件）
├── blobs/<sha256>       # 内容寻址的原件，每个哈希一份
├── quarantine/<sha256>  # 回收中的原件（崩溃可恢复）
└── tmp/part-<uuid>      # 上传临时分片
```

`/storage/summary` 同时返回 `blob_rows`（台账行数）与
`blob_files_on_disk`（实际原件数），两者一致即说明回收已收敛。

## 启动

### Docker Compose（推荐，两个实例 + 共享命名卷）

```bash
docker compose up --build
# 实例 A: http://localhost:8001/docs
# 实例 B: http://localhost:8002/docs
# 共享卷: archive-data -> /data
```

### 本地直接运行

```bash
pip install -r requirements.txt
ARCHIVE_DATA_DIR=./data uvicorn app.api:app --port 8000
# 第二个实例换端口、同一 ARCHIVE_DATA_DIR 即可
```

### 快速冒烟

```bash
curl -F "file=@batch.zip" http://localhost:8001/batches/B2026-001/archive
curl http://localhost:8002/batches/B2026-001/archive -o roundtrip.zip
curl -X POST http://localhost:8001/batches/B2026-001/retire
curl http://localhost:8001/storage/summary
```

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

测试覆盖：

- 上传/下载往返、2 MiB 限制、ZIP 校验、去重与单原件存储；
- 同号同内容复用、同号异内容 409、撤下号不可复用、撤下后 404；
- 共用原件只在最后一个引用批次撤下后才回收；
- **两个独立 OS 进程**共享同一数据目录的并发：相同内容并发上传、
  并发撤下、上传/撤下/回收/下载混合压力（校验 active 批次不缺文件、
  台账行数与磁盘原件数一致）；
- 慢速下载不阻塞其他批次撤下，且开始的下载可完整结束；
- 回收两阶段中 `os._exit` 硬崩溃后的重启续清，以及崩溃后内容被
  重新引用时的恢复；临时分片与隔离区孤儿清理。
