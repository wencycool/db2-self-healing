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
	acts, _, _, _, _, _, _, _ := db2.CollectPerfData("sample", time.Duration(time.Second*10))
	for _, act := range acts {
		fmt.Println(act.HexId, act.PlanId, act.TotalActTime, act.RowsRead, act.CpuTime, act.SnapTime)
		fmt.Println("对每一个SQL进行解析，检查执行计划")
		if act.HexId == "" {
			continue
		}
		expln, err := db2.NewMonGetExplain(act.HexId)
		if err != nil {
			fmt.Println(err)
		} else {
			if objs, err := expln.GetObj(); err != nil {
				fmt.Println(err)
			} else {
				for _, obj := range objs {
					fmt.Println(obj.ObjType, obj.ObjName, obj.RowCount, obj.SRowsModified, obj.FUKCard)
				}
			}
		}
	}

}
