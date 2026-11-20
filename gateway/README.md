# 冷库网关：采集进程热换版，记录仪 TCP 长连接不断

冷库温度记录仪通过 **一条 TCP 长连接** 持续上发逐行 JSON 读数。网关侧的
采集进程需要换版（v1 → v2），要求：

- 记录仪只连接一次，换版期间 TCP 连接不中断、不重连；
- 旧进程处理完手头已收到的完整读数后，把连接交给新进程；
- 已发出但尚未处理的读数沿原连接按发送顺序继续收，每条只确认一次；
- 新版启动失败时自动回退，旧进程继续收数；
- FastAPI 操作页展示每笔确认的读数编号与处理进程版本。

## 工作原理

```
                ┌──────────────────── gateway 容器 / 单 supervisor 进程 ───────────────────┐
  记录仪(客户端) │  TCP:9000 ── accept ──┐                                                  │
  只 connect 一次 │                        │ SCM_RIGHTS 传连接 fd（SOCK_SEQPACKET 控制通道）  │
  逐行 JSON 连发  │                        ▼                                                  │
                │              ┌──────────────────────┐        ┌──────────────────────┐     │
                │              │ 采集进程 v1（子进程）  │ 换版   │ 采集进程 v2（子进程）  │     │
                │              │ 持有记录仪 TCP fd      │ ─────▶ │ 接手同一个 TCP fd      │     │
                │              └──────────────────────┘        └──────────────────────┘     │
                │                        ▲  启动失败：v2 退出，v1 继续收数（自动回退）           │
                │              FastAPI 操作页 :8000（启动记录仪 / 换版 / 模拟失败 / 校验）     │
                └──────────────────────────────────────────────────────────────────────────┘
```

关键机制：

1. **监听 socket 归 supervisor 所有**，采集进程是独立子进程（独立版本号、独立 pid），
   自身不 accept 连接。supervisor accept 后通过 Unix 域套接字 + `SCM_RIGHTS`
   把 TCP 连接 fd 发给当前采集进程。
2. **换版即 fd 交接**，不是断连重连。内核里同一个 `struct file`（同一四元组、
   同一接收缓冲区）被复制到新进程，记录仪完全无感知。
3. 控制通道选用 **`SOCK_SEQPACKET`**：既面向连接、可靠，又保留消息边界，
   保证一次 `recvmsg` 同时取到整帧和附属 fd（STREAM 上先读长度头会吞掉
   SCM_RIGHTS 辅助数据，这是本项目实现中实测踩过的坑）。
4. **半行不丢不重**：旧进程换版时先把缓冲区与内核中已到达的**完整行**全部处理，
   剩余的不完整半行（leftover）随 fd 一起交给新进程；新进程先拼完半行再继续读。
5. **逐条确认、仅一次**：每个读数 id 的确认事件按连接记录，服务端实时检测重复确认；
   `/api/verify` 独立校验编号 1..N 连续、无重复、版本分界为干净的前缀/后缀。
6. **启动失败回退**：换版先拉起新版本并等待 `ready`；失败标记文件存在时新版本
   启动即失败，supervisor 不发起交接，旧进程与原连接原封不动继续工作。

读数行格式（`\n` 结尾的 JSON）：

```json
{"id": 7, "sensor": "freezer-1", "temp_c": -18.4, "ts": 1731000000.123}
```

## 目录结构

```
gateway/
├── app/
│   ├── main.py         # 入口：supervisor + uvicorn/FastAPI
│   ├── supervisor.py   # 监听、进程编排、换版/回退、fd 中继、事件与校验
│   ├── collector.py    # 采集进程：读数解析、逐条确认、交接逻辑
│   ├── recorder.py     # 模拟记录仪：只连一次，连发编号 JSON
│   ├── frame.py        # SEQPACKET 长度前缀帧 + SCM_RIGHTS
│   ├── webapp.py       # FastAPI 路由
│   └── web/            # 操作页（index.html / app.js）
├── tests/
│   ├── e2e_test.py     # 33 项端到端断言（连发/换版/失败回退/半行/多连接）
│   └── smoke_http.py   # 纯 HTTP API 冒烟测试
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## 用 Docker Compose 启动

```bash
cd gateway
docker compose up --build
# 操作页：  http://localhost:8000/
# 记录仪 TCP： localhost:9000
```

页面操作（也全部有对应 REST API）：

1. **① 模拟记录仪**：设置总数/间隔/暂停点，点击“启动记录仪（仅连接一次）”。
   建议把“在第几笔前暂停”设为例如 20、暂停 5 秒，制造换版窗口。
2. **② 换版**：暂停窗口内输入新版本号 `v2`，点“执行换版 / 交接”。
   勾选“模拟新版启动失败”可演练回退；也可用“放置失败标记”按钮手动制造失败。
3. **④ 逐笔确认** 表格实时显示每笔读数编号、处理进程版本、温度、连接、确认序号。
4. **⑤ 校验结果** 显示编号连续性、唯一性、各版本处理的编号区间，以及重复确认计数。

也可以用 compose 里的独立记录仪容器（同一镜像，recorder profile）：

```bash
docker compose --profile recorder run --rm recorder \
  python -m app.recorder --host gateway --port 9000 --count 60 --gap-at 20
```

## REST API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/api/status` | 当前版本、各代进程状态、连接计数、失败标记、重复确认 |
| GET  | `/api/events?cursor=N` | 增量事件流（确认、换版、日志…） |
| GET  | `/api/verify` | 每连接编号连续性/唯一性/版本分界的独立校验 |
| POST | `/api/upgrade` | `{"new_version":"v2","simulate_fail":false}` |
| POST | `/api/recorder/start` | `{"count":60,"interval":0.2,"gap_at":20,"gap_seconds":5}` |
| POST | `/api/recorder/stop` | 停止模拟记录仪 |
| POST | `/api/fail-marker` | `{"present":true}` 手动放置/清除启动失败标记 |
| POST | `/api/events/reset` | 清空事件与确认记录 |

## 本地验证（无需 Docker）

```bash
pip install -r requirements.txt
python tests/e2e_test.py    # 33 项：热换版、失败回退、半行交接、双连接、版本号复用
python tests/smoke_http.py  # 13 项：真实 HTTP 端点全流程
python tests/stress_test.py # 200 笔连发 + 3 次换版 + 1 次失败回退混合压力
```

## 设计取舍与边界

- 采集进程与 supervisor 放在**同一容器**内：fd 交接依赖同一内核网络命名空间
  与 Unix 套接字，跨容器需要共享 pid/network namespace，复杂度更高且无收益。
- 换版进行中到达的新 TCP 连接会在监听 backlog 中短暂等待，继任者就绪后再投递，
  不会交给即将退出的旧进程。
- 若**活动采集进程异常崩溃**（非换版场景），其持有的连接 fd 随进程释放，
  记录仪会收到 EOF；supervisor 会自动重启同版本进程。换版路径本身不会断连。
- 失败标记是演示“新版启动失败”的模拟手段（文件存在即启动失败），真实系统中
  对应健康检查/版本初始化失败。
