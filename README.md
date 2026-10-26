# WorkBuddy Local Gateway

把 WorkBuddy 账号（国内站 `codebuddy.cn` / 国际站 `workbuddy.ai`）封装成本地 OpenAI / Anthropic 兼容 API 的
单二进制反向代理网关。纯 Go 实现、零 CGO 依赖，内置网页控制台，配置与运行状态全部落在本地 JSON 文件，不需要数据库。

一句话概括：**一个账号池 + 三套协议入口 + 一套可观测的控制台**，让任意支持 OpenAI 或 Anthropic 协议的客户端
（OpenAI SDK、Claude Code、Cherry Studio、各类 IDE 插件等）都能直接使用 WorkBuddy 的模型能力。

## 目录

- [它解决什么问题](#它解决什么问题)
- [核心特性](#核心特性)
- [请求处理流程](#请求处理流程)
- [快速开始](#快速开始)
- [客户端接入](#客户端接入)
- [网页控制台](#网页控制台)
- [后台定时任务](#后台定时任务)
- [数据文件](#数据文件)
- [项目结构](#项目结构)
- [构建与测试](#构建与测试)
- [部署](#部署)
- [安全与隐私](#安全与隐私)
- [已知边界](#已知边界)
- [文档](#文档)

## 它解决什么问题

WorkBuddy 官方提供的是网页/客户端形态，没有面向第三方客户端的稳定 API；单账号又有额度、并发与限流约束。
本项目在本地做一层网关，解决四件事：

1. **协议适配**：对外暴露 OpenAI Chat Completions、OpenAI Responses、Anthropic Messages 三套协议，
   对内统一转发到上游，必要时做请求/响应与流式事件的互转。
2. **多账号聚合**：把多个 `workbuddy*.json` 凭据聚合成账号池，按轮询与健康状态调度，
   单个账号限流或额度耗尽时自动切换，不拖垮整体可用性。
3. **状态可视化**：额度、套餐、模型倍率、免费/收费判定、请求耗时、TTFT、失败原因全部汇总到网页控制台。
4. **长期无人值守**：凭据热加载、Token 到期前自动续期、每日自动签到、失效自动禁用与恢复，服务常驻即可。

## 核心特性

### 协议与转发

- **三套入口**：`/v1/chat/completions`、`/v1/responses`、`/v1/messages`，并兼容不带 `/v1` 前缀的旧路径。
- **模型完全透传**：客户端传什么 `model` 就原样中继，不做白名单改写；`/v1/models` 只用于客户端自动补全。
- **流式规范化**：把上游分片里空的 `finish_reason` 归一化为 `null`，避免 Anthropic 翻译层误判 `stop_reason`
  导致工具调用不执行；缺失 `finish_reason` 的流不会被误判为成功。
- **工具调用自愈**：出站前按 `tool_call_id` 修复并行调用中夹入的消息、补齐缺失的工具结果，
  并保留 DeepSeek 等模型的 reasoning 历史，避免多轮工具对话被上游拒绝。
- **多模态兼容**：Responses 协议的图片/文本工具结果会正确配对与提升，不丢失并行图片结果。
- **网络重试**：仅对连接建立阶段的瞬时错误重试（默认 2 次）；请求已发出或流已开始后不再重试，避免重复计费。
- **超时可控**：响应头超时、空闲读超时均可配置；慢速大响应不会被误杀。

### 账号池与调度

- **多账号轮询**：`-auth` 逗号分隔或 `-auth-dir` 目录，国内站与国际站账号可混挂。
- **模型级隔离**：`6004` 只冷却触发它的账号 + 模型，`14018` 只阻断该账号的当前收费模型，
  不会因为一个模型拖垮整个账号。
- **额度感知**：额度耗尽的账号自动跳过；免费模型优先使用仍可用的账号。
- **免费优先**：同一模型若「一个站点免费、另一个收费」，优先走免费站点；两边都收费时正常轮询。
- **站点能力证据**：根据历史成功请求、模型目录与探测结论判断某账号能否服务某模型，避免盲试。
- **凭据热加载**：默认每 5 秒扫描凭据来源，新增 / 更新 / 删除凭据免重启生效。
- **失效自愈**：401/403 / `invalid token` / 登录过期时禁用账号并写失效标记；重新登录后自动恢复。

### 模型与计费

- **双来源模型目录**：实时接口 + npm 静态目录，按 ID 去重、接口优先；失败回退本地缓存。
- **倍率展示**：促销生效时展示 `credits × factor`，过期促销自动回退基础价格。
- **免费/收费学习**：按「账号 + 模型」从响应 `usage.credit` 学习；`credit=0` 且样本足够才判定免费。
- **价格探测默认关闭**：只有显式传入 `-allow-price-probes` 才开启后台实测（真实生成请求会消耗积分）；
  关闭时仍会从正常请求中被动学习。

### 额度与套餐

- **额度扫描**：每 5 分钟刷新，单账号 10 秒超时，超时保留旧值。
- **套餐识别**：解析套餐页与资源包，区分个人 / 企业 / 试用，展示套餐名与到期信息。
- **企业额度**：企业成员只统计被分配的额度，不混入个人资源包。

### 运维与可观测性

- **状态快照**：每 3 秒写 `workbuddy-status.json`，`monitor` 命令与网页都读它。
- **日志脱敏**：Token、Cookie、API Key、账号标识在写盘与展示前统一脱敏；调试日志也不落原始请求体。
- **任务视图**：每个后台任务（签到、额度、模型目录、价格探测、续期）都有计划时间、上次结果与 traceId。
- **统计面板**：按模型/账号统计请求数、Token、耗时、TTFT、免费与收费占比。
- **网页配置**：风险确认后可完整编辑 `config.json`；端口类修改提示重启生效。

## 请求处理流程

```text
客户端 (OpenAI / Anthropic SDK)
        │
        ▼
  协议入口  /v1/chat/completions | /v1/responses | /v1/messages
        │  鉴权 → 限流检查 → 模型过滤 → 消息与工具链修复
        ▼
  账号调度  轮询 → 跳过禁用/冷却/额度耗尽 → 站点能力匹配 → 免费优先
        │  Token 临期自动续期（不足 15 分钟）
        ▼
  上游转发  连接阶段瞬时错误重试（默认 2 次）
        │
        ▼
  响应处理  流式规范化 → 协议互转 → 用量/免费状态学习 → 统计与日志脱敏
```

调度失败会按顺序尝试下一个可用账号；全部不可用时返回明确的中文错误，而不是空响应。

## 快速开始

需要 Go 1.26.5 或更新工具链。在项目根目录执行：

```bash
go build -o workbuddy-gateway .
./workbuddy-gateway login          # 扫码登录，生成 workbuddy1.json
./workbuddy-gateway serve -webui   # 启动网关 + 网页控制台
```

Windows：

```powershell
.\workbuddy-gateway.exe login
.\workbuddy-gateway.exe serve -webui
```

默认监听：

| 用途 | 地址 |
|---|---|
| 模型 API | `http://127.0.0.1:8317/v1` |
| 网页控制台 | `http://127.0.0.1:8316/ui/` |
| 健康检查 | `http://127.0.0.1:8317/health` |

首次使用可复制 `config.example.json` 为 `config.json`，按需设置管理 Key 与模型 API 鉴权。
更多账号直接重复 `login`，或在网页控制台扫码添加。

常用参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `-addr` | `127.0.0.1` | 监听地址 |
| `-port` | `8317` | 模型 API 端口 |
| `-webui` | 关闭 | 启用网页控制台（`8316`） |
| `-proxy` | 空 | 上游代理，如 `http://127.0.0.1:7890` |
| `-checkin-delay` | `60` | 开机已过 08:00 时，补签前等待秒数 |
| `-keepalive-hours` | `22` | 主动续期时刻（本地小时，逗号分隔；留空关闭） |
| `-allow-price-probes` | 关闭 | 允许后台价格探测（会消耗积分） |

## 客户端接入

OpenAI SDK：

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8317/v1", api_key="none")
print(client.chat.completions.create(
    model="hy3",
    messages=[{"role": "user", "content": "你好"}],
).choices[0].message.content)
```

Anthropic 协议：

```bash
export ANTHROPIC_BASE_URL=http://127.0.0.1:8317
export ANTHROPIC_AUTH_TOKEN=none
```

```bash
curl -N http://127.0.0.1:8317/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"hy3","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

模型 API 开启 Key 校验后，把 `api_key` 换成 `config.json` 里配置的 Key 即可。

## 网页控制台

`http://127.0.0.1:8316/ui/` 提供：

- 账号总览：额度、套餐、到期时间、冷却与失效原因、按站点汇总。
- 模型列表：倍率、免费/收费、站点支持情况、上下文长度、可服务账号数。
- 请求统计：模型排行、Token 曲线、耗时与 TTFT。
- 日志查看：按文件、级别、traceId 过滤，敏感字段已脱敏。
- 任务视图：每个定时任务的计划时间、上次运行结果。
- 配置编辑：完整 JSON 编辑（需风险确认）、端口修改、API Key 多把管理。
- 账号操作：扫码新增、重新登录、启用/禁用、删除凭据。

管理 Key 为空时，只允许在服务器本机（回环地址）首次生成，避免远程被抢注。

## 后台定时任务

| 任务 | 周期 | 说明 |
|---|---|---|
| Token 续期检查 | 5 分钟 | 距过期不足 15 分钟自动刷新 |
| 额度扫描 | 5 分钟 | 单账号 10 秒超时，超时保留旧值 |
| 模型目录刷新 | 60 分钟 | 实时接口 + npm 目录合并去重 |
| 模型价格探测 | 默认关闭 | 显式 `-allow-price-probes` 才开启 |
| 每日签到 | 每天 08:00（UTC+8） | 开机已过 08:00 则延迟 60 秒补签；失败 5 分钟后重试，每天最多 3 次 |
| 主动续期 | 每天 22:00 | `-keepalive-hours` 可改，留空关闭 |
| 状态快照 | 3 秒 | 写 `workbuddy-status.json` |
| 凭据热加载 | 5 秒 | 扫描新增 / 更新 / 删除 |

签到尝试次数持久化在 `workbuddy-checkin-state.json`，服务重启不会突破每日 3 次上限，跨天自动重置。

## 数据文件

| 文件 | 用途 | 是否入库 |
|---|---|---|
| `config.json` | 运行配置（含管理 Key、模型 API Key） | 否 |
| `workbuddy*.json` | 账号登录凭据 | 否 |
| `workbuddy-status.json` | 账号与模型状态快照 | 否 |
| `workbuddy-checkin-state.json` | 当天签到尝试次数 | 否 |
| `wb-models-cache.json` | 模型目录、倍率、探测结论 | 否 |
| `stats.json` | 请求统计 | 否 |
| `logs/` | 运行日志 | 否 |
| `config.example.json` | 配置示例（密钥留空） | 是 |

## 项目结构

生产源码统一为 `package main`，按职责合并为 18 个文件：

| 文件 | 职责 |
|---|---|
| `main.go` | 入口、命令行、serve 启动编排、账号池初始化 |
| `config.go` | 配置文件读写、热加载、网页配置编辑 |
| `accounts.go` | 账号池、凭据文件、健康检查与告警 |
| `credentials.go` | 登录、凭据编辑与校验、Token 刷新、主动续期 |
| `checkin.go` | 每日签到与幂等判定 |
| `quota.go` | 额度扫描、套餐识别、重置时间解析 |
| `models.go` | 模型目录、倍率、免费/收费学习、站点能力 |
| `modelstats.go` | 模型统计、价格探测、max_tokens 注入 |
| `chatapi.go` | OpenAI Chat Completions 入口与流式处理 |
| `messages.go` | Anthropic Messages 与 OpenAI Responses 入口及互转 |
| `repair.go` | 工具调用序列修复、reasoning 修复、系统提示注入 |
| `webui.go` | 网页控制台、鉴权、策略与设置接口 |
| `logs.go` | 日志写入、轮转、脱敏、查看 |
| `admin.go` | 管理 API Key、管理端操作 |
| `stats.go` | 请求统计、状态快照 |
| `debug.go` | 调试日志、trace、诊断接口 |
| `runtime.go` | 原子写文件、协程兜底、上游请求与中间件 |
| `reset.go` | `reset` 命令：清理本地状态但保留凭据 |

测试文件保持「一个特性一个文件」：43 个 `*_test.go` 与源码同包，能直接验证未导出函数与全局状态，
并且 `go build` 会自动排除它们。这样做的原因是失败时能一眼定位到特性，而不是在几千行的巨型测试文件里翻找；
合并测试文件不会减少任何逻辑，只会降低可读性。

其余目录：

```text
web/         网页控制台静态资源（编译时 go:embed 进二进制）
docs/        使用手册、部署、贡献者与目录约定
tests/       网页与构建脚本的独立测试
scripts/     统一测试入口
tools/       Windows 服务安装 / 更新 / 探测脚本
archive/     历史材料（本地内容不入库）
```

## 构建与测试

```bash
go vet ./...
go test ./...
```

PowerShell：

```powershell
.\scripts\test.ps1                 # Go + 网页 + 构建脚本回归
.\build.ps1                       # 校验并构建全部 9 个平台/架构目标
.\build.ps1 -Only linux -Architecture amd64
```

## 部署

**Windows（NSSM 服务）**

```powershell
.\tools\install-service.ps1        # 安装并启动 workbuddy-gateway 服务
.\tools\update.ps1                 # 构建新版本并热切换
```

**Linux（systemd）**

参考 [Linux 部署清单](docs/deploy-linux.md)，仓库内提供了 `tools/workbuddy-gateway.service` 模板。

## 安全与隐私

- 仓库只包含源码、示例配置与文档；`config.json`、账号凭据、状态文件、日志均由 `.gitignore` 排除。
- 提交前请确认 `git status` 中没有 `workbuddy*.json`、`config.json`、`stats.json`、`*.log`。
- 网关默认只监听 `127.0.0.1`。需要局域网 / 公网访问时，请开启模型 API Key 校验，或置于带认证的反向代理之后。
- 管理接口默认只允许回环地址完成首次 Key 生成；远程使用建议走 SSH 隧道。
- 日志与网页展示对 Token、Cookie、API Key 做脱敏，调试模式也不会记录原始请求体。
- 脚本中的密钥一律从环境变量读取，不要在脚本里写死。
- 本项目仅用于个人学习与技术研究，请遵守上游服务条款。

## 已知边界

- 国际站没有已确认可用的签到接口，签到只对国内站账号生效。
- 价格探测会产生真实生成请求，默认关闭。
- 上游模型目录与倍率随时可能调整，网关以「实时接口优先、缓存兜底」的方式尽力对齐。
- 协议互转覆盖常用字段，上游新增的非标准字段可能被忽略。

## 文档

| 内容 | 位置 |
|---|---|
| 完整使用手册、配置、客户端接入与部署 | [docs/user-guide.md](docs/user-guide.md) |
| 目录职责与命名约定 | [docs/project-layout.md](docs/project-layout.md) |
| 贡献者及方案署名 | [docs/contributors.md](docs/contributors.md) |
| Linux 服务器部署清单（systemd） | [docs/deploy-linux.md](docs/deploy-linux.md) |
| 测试入口与说明 | [tests/README.md](tests/README.md) |

完整安全提示与免责声明见[使用手册](docs/user-guide.md#安全提示)。
