# Flink Studio — DBX 插件

通用 Flink 客户端：连接 JobManager REST API，查看集群概览、作业、TaskManager、配置，并从 print-sink 日志**查看消费内容**。零外部 Go 依赖，单文件 UI，纯只读。

## 功能

| Tab | 数据源 | 说明 |
|---|---|---|
| 概览 | `/overview` | 版本 / TM 数 / slots / 运行中作业数 |
| 作业 | `/jobs` + `/jobs/:id` | 作业列表（状态徽标）+ 详情（状态/启动时间/算子顶点） |
| **消费内容** | `/taskmanagers/:id/logs/*.out` | 解析 print-sink 的 `+I[...]` / `-U[...]` 行成表格；支持按 TM 过滤、条数限制 |
| TaskManager | `/taskmanagers` + `/taskmanagers/:id/logs` | TM 指标 + 日志文件在线查看 |
| 配置 | `/jobmanager/config` | Flink 配置项表格 + 过滤 |

截图风格参考 Kafka Studio：顶部工具栏（连接选择 + 地址 + 只读徽标）+ 功能 Tab + 左侧对象树 + 右侧详情。主题桥接宿主 `--color-*` 令牌，亮/暗自适应。

## 目录

```
manifest.json          # 插件身份 + 连接表单 + workbench/menus
dbx-plugin.toml        # CLI 打包配置
backend/
  main.go              # sidecar 入口，pluginVersion 常量（须等于 manifest.version）
  internal/
    dbxpluginsdk/      # 官方 JSONL SDK（vendored，零外部依赖）
    connection/        # lifecycle 解析 + HTTP 客户端（凭据 json:"-"）
    transport/         # Handle 路由 + Flink REST 只读代理 + 日志解析
ui/index.html          # 单文件 UI（内联 CSS+JS，宿主桥注入）
scripts/
  check-manifest.py    # 身份/版本/路径一致性校验
  smoke_test.py        # JSONL 端到端（连真实 Flink）
assets/                # plugin.svg / connection.svg
```

## 连接配置

从 DBX 连接树新建 **Flink Studio** 连接（database_type=flink）：

| 字段 | 默认 | 说明 |
|---|---|---|
| host | localhost | JobManager 地址 |
| port | 8081 | REST 端口 |
| scheme | http | http / https |
| auth_type | none | none / basic / bearer |
| username / password | — | basic 认证（password 存 secret） |
| bearer_token | — | bearer 认证（存 secret） |
| tls_verify | true | https 时校验证书 |
| timeout_secs | 12 | 请求超时 |
| max_body_mb | 16 | 日志读取上限 |
| read_only | true | 插件全链路只读 |

## "消费内容"工作原理

Flink 本身没有"消息存储"API。插件读取 TaskManager 的 stdout 日志（`.out`），其中 SQL print connector 会把每条结果按 changelog 格式打印：

```
+I[2249, 成都, 显示器, 680.36]
-I[2250, 北京, 键盘, 120.00]     #  retract / update-before
+U[2250, 北京, 鼠标, 130.00]     #  update-after
```

插件扫描所有 TM 的 `.out` 文件，用正则提取 `[+-]I|+-U|D [...]`，按逗号（跳过引号/括号内）拆成列，取最新 N 行展示。

**前置条件**：作业需使用 print sink：

```sql
CREATE TABLE sink_print (id INT, city STRING, product STRING, amount DOUBLE)
WITH ('connector' = 'print');
INSERT INTO sink_print SELECT id, city, product, amount FROM kafka_events;
```

注意：日志滚动/清理后旧记录不可见；非 print sink 的作业（如写 DB）无 changelog 行可解析。

## 构建

Portable Go（本项目归档工具链）：

```bash
export PATH="/c/asdm-tools/_tools/go/bin:$PATH"
export GOROOT="C:\asdm-tools\_tools\go"
cd backend && go build -o ../bin/dbx-plugin-flink-studio.exe .
cd .. && python scripts/check-manifest.py
```

打包（需 dbx-plugin CLI，仅当前主机 target）：

```bash
dbx-plugin package . --target windows-x64
```

**多平台打包 + 签名**（CLI 只构建本机，故用交叉编译 + 手工组装官方包格式）：

```bash
bash scripts/build_cross.sh          # 交叉编译 5 平台：linux/darwin × x64/arm64 + windows-x64
python scripts/pack_all.py           # 组装 .dbxp + 用 leansoftx.release-2026 key 签名
```

关键：`pack_all.py` 会把 manifest 的 `entrypoints.backend.executable` 改写为 `bin/<target>/<binary>`（对齐 ads-arm 已验证格式），否则装到 dbx 找不到 sidecar。签名 key 复用 `C:\asdm-tools\dbx-ads-arm\.signing`（193 已信任 `leansoftx.release-2026`）。产物在 `dist/*.signed.dbxp`。

**部署到 193 dbx**（`python scripts/install_to_dbx.py [包路径]`）：走 `/api/auth/login` → cookie → `/api/plugins/install` multipart 上传。默认装 `linux-x64.signed.dbxp`。安装后 `/api/plugins` 应见 `id=local.flink.studio`、签名 key 正确。已验证（2026-11-19）：安装 HTTP 200，24→25 插件，`flink/ping|test|messages` 在 dbx 上真实调用成功。

## 验证

```bash
# 后端端到端：连真实 Flink，验证 8 个方法 + 路径穿越门禁
python scripts/smoke_test.py --bin <sidecar> --host <jobmanager-host> --port 8081

# UI 渲染：启动真实 sidecar 子进程 + Playwright，截图到 _tmp/shots/
```

已验证：Flink 1.14.1 上 `flink/test` 返回版本/TM/作业数，`flink/messages` 可解析 print-sink 日志中的 `+I[...]` 记录，UI 五个 Tab 全部渲染无 JS 错误。

## 方法契约

| 方法 | 参数 | 说明 |
|---|---|---|
| `flink/ping` | — | 探活 |
| `flink/test` / `connection/test` | lifecycle | 连通性 + 版本 |
| `connection/connect` / `disconnect` / `list` | lifecycle / connection_id | 会话管理 |
| `flink/overview` | connection_id | 集群概览 |
| `flink/jobs` / `flink/job` | connection_id, jobId | 作业列表 / 详情 |
| `flink/taskmanagers` / `flink/taskmanager-logs` | connection_id, taskManager | TM 列表 / 日志文件列表 |
| `flink/logfile` | connection_id, taskManager, fileName | 日志文件正文 |
| `flink/messages` | connection_id, taskManager?, limit | 消费记录（解析 print-sink 日志） |
| `flink/config` | connection_id | 集群配置 |
| `flink/request` | connection_id, path, query? | 通用只读 GET 代理（路径禁 `..`） |
| `flink/batch` | connection_id, requests[] | 批量只读 GET |

## 版本同步

`manifest.json.version` == `backend/main.go` 的 `pluginVersion`（当前 0.1.1）。`check-manifest.py` 强制断言。升级时两处同改。

## 安全

- 所有请求为 GET；`validatePath` 拒绝非绝对路径和 `..` 穿越。
- 凭据只存于 `Auth` 的未导出字段（不参与 JSON 序列化），不进响应/日志。
- `read_only` 在连接表单默认 true；后端无任何写/取消作业方法。
- 日志正文上限 `max_body_mb`（默认 16MB），HTTP 客户端超时保护。
