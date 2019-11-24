package main

import (
	"fmt"
	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
)

func main() {
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.TraceLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	db2.LogRegister(log)
	db2.ConnectDB("sample")
	memPool := db2.NewMonGetMemPoolList()
	fmt.Println("打印MemPool内容")
	for _, v := range memPool {
		fmt.Printf("MemSet:%-10s,Mempool:%-10s,Size:%-10d\n", v.MemSetType, v.MemPoolType, v.MemPoolUsedKb)
	}

	memSet := db2.NewMonGetMemSetList()
	fmt.Println("打印MemSet内容")
	for _, v := range memSet {
		fmt.Printf("MemSet:%-10s,Size:%-10d\n", v.MemSetType, v.MemSizeKb)
	}
}
