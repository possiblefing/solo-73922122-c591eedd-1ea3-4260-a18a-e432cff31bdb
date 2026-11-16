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
- 下载**先打开原件文件描述符、后发送响应头**，且全程只从该 fd 读取：
  一旦客户端看到 `200` 与 `Content-Length`，正文必然完整；即使期间原件
  被本实例或另一实例移入隔离区/删除（POSIX 语义），传输也不会被截断。
- 慢速下载不持有数据库事务/锁，也不持有 `blobs/` 上的任何锁，
  因此不会拖住其他批次的撤下、回收或下载。
- 客户端中途断连时下载立即停止并关闭 fd，不会长期占用已被回收的原件。
- `Range` 行为（单段、后缀、多段、`If-Range`、`416`、`400`）与原先一致，
  正文严格匹配响应声明的长度与范围（多段响应的 `Content-Length` 按实际
  字节精确计算）。

## 并发与崩溃安全设计

- **下载与回收的竞态（本次修复的核心）**：旧实现使用 Starlette
  `FileResponse`，它先发送 `200` 响应头、**之后才**打开文件。若撤下批次
  的回收恰在这个窗口把原件移走，服务器 `open()` 会抛 `FileNotFoundError`，
  客户端收到空正文。现在 `GET /batches/{id}/archive` 先校验批次仍为
  active，再 `os.open()` 固定 inode（必要时读取隔离区中的同一份内容），
  然后才构造响应。之后无论哪个实例执行 rename/unlink，已打开的 fd 都能
  读完整份原件（`app/download.py`）。
- SQLite 启用 **WAL** 与 `busy_timeout`；所有写操作在单个
  `BEGIN IMMEDIATE` 事务内完成判定与写入，两个实例靠写锁串行化，
  不会互相读到半成品状态。
- 上传顺序：先写 `tmp/` 临时分片 → 事务内插入/认领 blob 行与批次行并提交
  → 再原子 `rename` 到 `blobs/`。行先于文件存在，回收扫描不会误判新上传。
- 回收分两阶段：
  1. **标记**：在写锁内读取台账并清理无主文件（避免把并发上传刚发布的
     原件误删），然后把无 active 引用的 ready blob 写入 `gc_queue`；
  2. **清扫**：事务内复查引用后置 `deleting`，文件移入 `quarantine/`，
     再在持锁事务内复查引用并删除行与残留文件。每一步都幂等，两个实例可
     并发回收；无主的 ready 行会被删除而**不会**被重新插入，回收因此收敛。
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
`instance` 字段显示服务该请求的副本名（Compose 中的 `INSTANCE_NAME`），
可用于确认两个实例读到的是同一共享卷。

## 启动

### Docker Compose（推荐，两个实例 + 共享命名卷）

```bash
docker compose up --build
# 实例 A: http://localhost:8001/docs
# 实例 B: http://localhost:8002/docs
# 共享卷: archive-data -> /data
```

镜像只构建一次（服务 A 拥有 `build:`，服务 B 复用同一镜像），
避免 `up --build` 时两个服务并发构建同名标签。两个实例都映射容器的
8000 端口到宿主机 8001/8002，`/docs` 可直接完成上传、下载、撤下与查询。

### 本地直接运行

```bash
pip install -r requirements.txt
ARCHIVE_DATA_DIR=./data python -m app.main        # 端口 8000
ARCHIVE_DATA_DIR=./data PORT=8001 python -m app.main
# 或: ARCHIVE_DATA_DIR=./data uvicorn app.api:app --port 8000
```

### 快速冒烟

```bash
curl -F "file=@batch.zip" http://localhost:8001/batches/B2026-001/archive
curl http://localhost:8002/batches/B2026-001/archive -o roundtrip.zip
curl -X POST http://localhost:8001/batches/B2026-001/retire
curl http://localhost:8001/storage/summary
```

### 双实例竞态压力脚本

```bash
python3 scripts/race_hammer.py 80 8   # 80 轮、8 并发
```

每轮在 A 发起下载的同时在 B 撤下并回收（带随机抖动命中 stat/open
窗口），校验：已发出的 `200` 正文长度与哈希完全一致；撤下先完成时新
下载为 404；从不出现半截正文或 500。脚本对旧实现会报
`FileNotFoundError`，可用于回归对比。

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
- **下载/撤下竞态（确定性交错）**：ASGI 层在“已发 200 头、正文未发”时
  暂停，另一侧完成撤下与回收，正文仍完整；回收先于 fd 打开时返回 404
  而非残缺 200；原件短暂位于隔离区时仍可下载；台账长度不符时 404；
- **真实 HTTP 层**：裸 socket 只读响应头后由另一实例撤下回收，再读正文，
  校验长度与哈希；客户端中途断连后服务端 fd 被释放且停止读取；
- `Range`：单段、后缀、开区间、多段（正文长度与声明一致）、`416`、
  `400`、`If-Range` 命中/不命中、HEAD；
- 回收两阶段中 `os._exit` 硬崩溃后的重启续清，以及崩溃后内容被
  重新引用时的恢复；临时分片与隔离区孤儿清理；无主 ready 行收敛，
  标记阶段持写锁不误删并发上传的原件。
