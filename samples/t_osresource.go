package main

import (
	"fmt"
	"my/db/db2"
	"time"
)

func main() {
	db2.ConnectDB("sample")
	r := db2.NewDb2Resource()
	r.Update(time.Second)
	s := db2.NewOsResource()
	s.Update(time.Second)
	for {
		time.Sleep(time.Second)
		fmt.Printf("Time:%-10s,User:%-5d,Sys:%-5d,OSUser:%-5d,OSSys:%-5d,OSIoWait:%-5d,OSIdle:%-5d\n", r.SnapTime.String(), r.CpuUser, r.CpuSys, s.CpuUser, s.CpuSys, s.CpuIoWait, s.CpuIdle)
		fmt.Printf("CPURatio:%-5d\n", 100*(s.CpuSys+s.CpuUser)/(s.CpuSys+s.CpuUser+s.CpuIdle+s.CpuIoWait))
	}

}
