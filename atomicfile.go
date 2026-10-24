package main

import (
	"os"
)

// atomicWriteFile 以「同目录临时文件 + fsync + 改名」原子落盘：进程被强杀或断电时，
// 目标文件要么是完整旧内容、要么是完整新内容，绝不出现写一半的中间态。
// 凭据文件是唯一 refreshToken 副本、config.json 解析失败会导致启动退出，两者都必须原子替换。
// 临时文件固定用 path+".tmp" 命名，reset 与启动清理按该后缀回收孤儿临时文件；
// 调用方需自行串行化对同一路径的并发写入（各写盘点已有各自的锁）。
// 注意：替换前调用方必须关闭目标文件自身的句柄——Go 在 Windows 上打开的句柄
// 不含 FILE_SHARE_DELETE，句柄未关时 rename 会因共享冲突失败。
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp, path)
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return writeErr
	}
	return nil
}
