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
	a, b, c, d, e, f, g, err := db2.CollectPerfData("sample", time.Duration(time.Second*10))
	if err != nil {
		log.Fatal(err)
	}
	for _, i := range a {
		fmt.Println("开始获取explain信息")
		expln, err := db2.NewMonGetExplain(i.HexId)
		if err != nil {
			log.Println("ooooooooo", err)
		} else {
			objs, err := expln.GetObj()
			if err != nil {
				log.Println("xxxxxxxx", err)
			}
			for _, obj := range objs {
				fmt.Printf("%v\n", *obj)
			}
		}
	}
	for _, i := range b {
		fmt.Println(i)
	}
	for _, i := range c {
		fmt.Println(i)
	}
	for _, i := range d {
		fmt.Println(i)
	}
	for _, i := range e {
		fmt.Println(i)
	}
	for _, i := range f {
		fmt.Println(i.ObjName, i.UtilDetail)
	}
	for _, i := range g {
		fmt.Println(i)
	}

}
