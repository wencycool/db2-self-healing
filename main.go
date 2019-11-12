package main

import (
	"fmt"
	"github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
	"time"
)

func main() {
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.InfoLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	db2.LogRegister(log)
	_, _, _, _, _, _, _, _ = db2.CollectPerfData("sample", time.Duration(time.Second*10))
	//获取参数信息
	dbcfg, _ := db2.GetMonGetDbCfgMap()
	fmt.Println(dbcfg["logarchmeth1"].Name, dbcfg["logarchmeth1"].ValFlag, dbcfg["logarchmeth1"].Value)
	dbmcfg, err := db2.GetMonGetDbmCfgMap()
	fmt.Println(err)
	fmt.Println(dbmcfg)

}
