# Linux 服务器部署清单（systemd）

本网关是纯 Go、零 CGO 的单二进制，Windows 与 Linux 行为一致；差异只在服务管理方式（NSSM → systemd）与文件权限。

## 1. 交叉编译

```powershell
.\build.ps1 -Only linux -Architecture amd64     # 或： $env:GOOS="linux"; $env:GOARCH="amd64"; go build -trimpath -ldflags "-s -w" -o workbuddy-gateway-linux .
```

## 2. 上传内容

| 内容 | 说明 |
|---|---|
| `workbuddy-gateway-linux` | 重命名为 `workbuddy-gateway`，`chmod +x` |
| `config.json` | 服务器专用一份；确认 `gateway.adminKey` / `apiKey` 已设置（暴露端口时 apiKey 必开） |
| `workbuddy*.json` 凭据 | 或者在服务器上重新 `./workbuddy-gateway login` |
| `tools/workbuddy-gateway.service` | systemd unit |

不需要上传：`web/`（已 go:embed 进二进制）、`logs/`、`stats.json`、`*.bak`。

## 3. 目录、用户与权限

```bash
sudo useradd -r -s /usr/sbin/nologin workbuddy
sudo mkdir -p /opt/workbuddy-gateway && sudo chown workbuddy:workbuddy /opt/workbuddy-gateway
# 凭据与配置只留给服务用户：
sudo chmod 600 config.json workbuddy*.json
```

## 4. 安装服务

```bash
sudo cp tools/workbuddy-gateway.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now workbuddy-gateway
systemctl status workbuddy-gateway
curl -s http://127.0.0.1:8327/health     # 按 config.json 的 apiPort
```

日志：网关自有日志在 `/opt/workbuddy-gateway/logs/`（按保留天数自动清理，含 service-*.log 等价物）；进程输出在 `journalctl -u workbuddy-gateway -f`。

## 5. 暴露面（重要）

- 默认只监听 `127.0.0.1`。远程使用：优先 SSH 隧道（`ssh -L 8327:127.0.0.1:8327 user@server`）或反向代理加认证；必须直接暴露时用 `-addr 0.0.0.0` 并**强制开启 apiKey**，防火墙只放行需要的来源。
- 管理台首次生成管理 Key 仅允许回环访问：用 `ssh -L 8316:127.0.0.1:8316` 打开网页，或在服务器上直接编辑 config.json 设置 `gateway.adminKey`。
- 管理端 401 失败限速尚未实现，直接暴露 web 端口前建议由反向代理层做限速。

## 6. 升级

```bash
# 本机交叉编译新二进制后：
sudo systemctl stop workbuddy-gateway
sudo cp workbuddy-gateway /opt/workbuddy-gateway/workbuddy-gateway
sudo systemctl start workbuddy-gateway
curl -s http://127.0.0.1:8327/health
```

失败回滚：保留上一个二进制副本，反向重复即可（Restart=on-failure 会在崩溃时自动拉起）。
