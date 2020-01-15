package main

import (
	"flag"
	"fmt"
	"github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
)

func main() {
	var dbname string
	//dbname = "sample"
	flag.StringVar(&dbname, "d", "sample", "连接数据库名字")
	flag.Parse()
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.PanicLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	db2.LogRegister(log)
	if err := db2.ConnectDB(dbname); err != nil {
		panic("Connect to db error:" + err.Error())
	}
	if m, err := db2.GetMonGetActStmtList(); err != nil {
		fmt.Println(err)
	} else {
		for _, i := range m {
			fmt.Println(i)
		}
	}
}
