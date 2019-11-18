package vm

import (
	"fmt"
	"runtime"
	"time"
)

//控制程序最多可使用的内存大小，每秒轮训检查一次,如果内存使用超过上限则panic
func MemLimit(size_byte uint64) {
	go func() {
		m := new(runtime.MemStats)
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				//检查使用内存是否超过上限
				runtime.ReadMemStats(m)
				if m.Alloc > size_byte {
					panic(fmt.Sprintf("UsedSize:%d overflow maxsize:%d\n", m.Alloc, size_byte))
				}
			}
		}
	}()

}
