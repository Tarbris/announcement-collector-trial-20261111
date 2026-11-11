# 公告采集服务：不可靠分页源到可靠业务状态

这是一个只访问本机模拟数据源的增量采集开发任务。初始仓库仅提供源服务与固定数据，没有采集器；无需账号、第三方依赖或外网。适用 Python 3.9+、Windows/Linux。

## 启动源

```powershell
python fixture_server.py --port 8765
```

源仅绑定 `127.0.0.1`。本任务不需要抓取任何真实网站。

## 固定 HTTP 协议

* `GET /health`：返回 `ok` 和分区名 `north`、`south`。
* `GET /feed?partition=north&cursor=start`：返回 `partition`、`cursor`、`records`、`next_cursor`。初始游标为字符串 `start`；也可省略 `cursor`。非空 `next_cursor` 是下一页游标；`null` 表示分区完成。不要把完成态当成初始态。
* 每区有三页：`north: start -> n2 -> n3 -> null`，`south: start -> s2 -> s3 -> null`。单区必须顺序拉取；不同分区可并发。
* `north:n2` 首次请求返回 429，响应头 `Retry-After: 1`；第二次起成功。
* `south:s2` 前两次请求返回 503，第三次起成功。
* 每个合法分页请求固定有约 80 ms 延迟。无效分区或游标返回 400。
* `GET /metrics`：包含各 `partition:cursor` 的尝试数、请求时间与状态、当前 `in_flight`、历史 `max_parallel`。时间是服务器 `time.monotonic()`，只比较间隔。
* `POST /reset`：重置故障计划及请求指标，不动客户端数据库；存在进行中的分页请求时返回 409，否则 200。

## 记录规则

合法记录要求 `id` 为非空字符串、`revision` 为正整数（布尔值不算整数）、`op` 为 `upsert` 或 `delete`。`upsert` 另要求 `title` 为非空字符串。`delete` 不需要标题，持久化后标题为 `null`，`deleted` 为 1。同一 `(partition,id)` 只保留最高版本；相同版本重复与较低版本不覆盖已有状态。墓碑也参与版本比较，不能物理删除后让旧版复活。

坏记录按 `(partition,cursor,record_index)` 隔离，`record_index` 从 0 开始，保存原始 JSON 与明确原因。遇坏记录不终止整页。有效记录、隔离记录、分区游标必须在同一 SQLite 事务提交。

## 精确最终状态

`announcements` 最终 7 行，5 条活跃、2 条墓碑：

| partition | id | revision | title | deleted |
| --- | --- | --- | --- | --- |
| north | N001 | 2 | 北区道路养护公告（时间更正） | 0 |
| north | N002 | 2 | null | 1 |
| north | N003 | 1 | 北区公共设施检修公告 | 0 |
| south | S001 | 2 | 南区管线维护公告（更正） | 0 |
| south | S002 | 2 | null | 1 |
| south | S003 | 2 | 南区场馆开放公告（更正） | 0 |
| south | S004 | 1 | 南区河道巡检公告 | 0 |

`quarantine` 恰好 2 行：`(north,n2,1)` 缺少 title；`(south,s3,2)` revision 非整数。`checkpoints` 两行均 `done=1`、`next_cursor=null`。

完整无中断首跑共成功 6 页、HTTP 请求 9 次（含 1 次 429、2 次 503），源 `max_parallel >= 2`。从 429 响应记录到同页下一请求响应的时间差应不小于 1 秒。完成后再次 run 不应再访问 `/feed`，业务和隔离行数不变。

## 采集器的固定验收接口（待实现）

```powershell
python collector.py run --base-url http://127.0.0.1:8765 --db runtime/demo.db --workers 2
python collector.py status --db runtime/demo.db
python collector.py reset --db runtime/demo.db
python collector.py run --base-url http://127.0.0.1:8765 --db runtime/demo.db --workers 2 --crash-before-commit north:n2
```

`status` 向 stdout 输出合法 JSON，包含 `total`、`active`、`deleted`、`quarantined`、`partitions`；最终四个计数分别为 7、5、2、2。`partitions` 是以分区名为键的对象，值至少包括 `next_cursor`、`done`。命令正常退出 0，失败非 0。

`reset` 只重置指定采集库自身表的数据，不能访问源 `/reset`，不能删整个目录。正常 `run` 不清库。

故障注入参数在匹配页的业务、隔离、游标 SQL 已执行而事务尚未提交时，打印并 flush `FAULT_INJECTION north:n2 before_commit`，然后必须调用 `os._exit(86)` 立刻终止整个进程，不能用会正常清理事务的异常代替。不带此参数重启，应从最后成功提交的游标恢复；最终状态与干净首跑一致。不要在源服务里实现故障注入。

采集表至少提供这些列：

* `announcements(partition,id,revision,title,deleted)`，主键 `(partition,id)`。
* `quarantine(partition,cursor,record_index,raw_json,reason)`，唯一键 `(partition,cursor,record_index)`。
* `checkpoints(partition,next_cursor,done)`，主键 `partition`，初始值 `start,0`。

允许额外列和辅助表；不得硬编码最终行数或直接读 fixtures 来替代 HTTP 采集。

## 不可修改范围

不得修改 `fixture_server.py` 与 `fixtures/pages.json`。可以新增采集器、测试、演示脚本和使用文档。整个业务实现与测试只允许 Python 标准库。`runtime/` 是临时产物目录，已忽略。
