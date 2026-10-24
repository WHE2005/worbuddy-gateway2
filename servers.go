package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"
)

func newAPIHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", handleChatCompletions)
	mux.HandleFunc("/chat/completions", handleChatCompletions)
	mux.HandleFunc("/v1/responses", handleResponses)
	mux.HandleFunc("/responses", handleResponses)
	mux.HandleFunc("/v1/messages", handleMessages)
	mux.HandleFunc("/v1/messages/count_tokens", handleCountTokens)
	mux.HandleFunc("/v1/models", handleModels)
	mux.HandleFunc("/models", handleModels)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/ping", handleHealth)
	mux.HandleFunc("/admin/probe", handleAdminProbe)
	mux.HandleFunc("/", handleIndex)
	return requestAuditMiddleware(corsMiddleware(authMiddleware(requestBodyLimitMiddleware(mux))))
}

// requestBodyLimitMiddleware 为模型 API 的请求体设硬上限：四个协议入口都是 io.ReadAll
// 全量进内存，无上限时失控客户端（或浏览器网页发起的免预检跨域 POST）可以把网关内存打爆。
// 超限后读 body 报错，各入口统一按「读取请求体失败」400 返回。
func requestBodyLimitMiddleware(next http.Handler) http.Handler {
	const maxModelRequestBytes = 64 << 20
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxModelRequestBytes)
		}
		next.ServeHTTP(w, r)
	})
}

func prepareGatewayServers() ([]*http.Server, []net.Listener, error) {
	if err := webUIPreflightError(); err != nil {
		return nil, nil, err
	}
	servers := []*http.Server{{
		Addr: net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.Port)), Handler: newAPIHandler(),
		ReadTimeout: 120 * time.Second,
		// 保留模型流无总写入时限，不把管理端限制扩散到模型 API。
		WriteTimeout: 0,
		// 回收 keep-alive 空闲连接，避免长期运行后 fd 缓慢堆积。
		IdleTimeout: 120 * time.Second,
	}}
	if cfg.WebUI {
		servers = append(servers, &http.Server{
			Addr: net.JoinHostPort(cfg.Addr, strconv.Itoa(cfg.WebPort)), Handler: newWebUIHandler(),
			ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second,
			WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		})
	}
	trace := newTraceID()
	var listeners []net.Listener
	for i, server := range servers {
		listener, err := net.Listen("tcp", server.Addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			log.Printf("[监听初始化] traceId=%s 地址=%s 结果=失败 原因=%v 业务影响=已释放先前监听，未启动后台业务", trace, server.Addr, err)
			return nil, nil, fmt.Errorf("监听 %s 失败: %w", server.Addr, err)
		}
		listeners = append(listeners, listener)
		log.Printf("[监听初始化] traceId=%s 地址=%s 管理端=%t 结果=绑定成功", trace, server.Addr, i == 1)
	}
	return servers, listeners, nil
}
