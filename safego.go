package main

import (
	"log"
	"runtime/debug"
	"time"
)

// runUntilPanic 同步运行 fn 并捕获 panic：正常返回 false；panic 时记录任务名与完整堆栈后返回 true。
// HTTP handler 的 panic 由 net/http 兜底，但后台协程 panic 会直接击穿进程——NSSM 拉起服务、
// 所有进行中的流式响应中断。这里是后台任务对进程的唯一防线。
func runUntilPanic(name string, fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[后台任务异常] 任务=%s 结果=捕获panic 说明=进程继续运行，进行中的模型请求不受影响 堆栈=%s", name, debug.Stack())
			panicked = true
		}
	}()
	fn()
	return false
}

// safeGoLoop 运行常驻后台循环：panic 时退避 15s 重启该循环；正常返回（如功能被配置关闭、
// shutdown 时 ctx 取消）则退出，不重启。闭包捕获的变量必须是调用点的快照。
func safeGoLoop(name string, fn func()) {
	go func() {
		for {
			if !runUntilPanic(name, fn) {
				return
			}
			time.Sleep(15 * time.Second)
		}
	}()
}

// safeGo 运行一次性后台任务并捕获 panic，进程不因后台任务异常而退出。
func safeGo(name string, fn func()) {
	go func() {
		runUntilPanic(name, fn)
	}()
}
