package main

import (
	"fmt"
	"github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
	"strconv"
	"strings"
	"time"
)

func main() {
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.PanicLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	db2.LogRegister(log)
	acts, _, _, _, locks, utils, uow_extend, _ := db2.CollectPerfData("sample", time.Duration(time.Second*10))
	act_stmts := db2.GetMonGetActStmtAggByPlanid(acts)
	for _, act := range act_stmts {
		fmt.Printf("执行次数:%d，执行语句：%s\n", act.ActCount, act.HexId)
		applist := make([]string, 0)
		for _, handle := range act.AppHandleList {
			applist = append(applist, strconv.Itoa(int(handle)))
		}
		fmt.Printf("执行该语句的handle列表：%s\n", strings.Join(applist, ","))
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
					fmt.Println("对象信息:", obj.ObjType, obj.ObjName, obj.RowCount, obj.SRowsModified, obj.FUKCard)
				}
			}
		}
		fmt.Printf("打印Advis信息,执行者:%-30s,执行语句:%s\n", act.AuthId, db2.NewMonGetPkgCacheStmt(act.HexId).StmtText)

	}
	if len(uow_extend) > 0 {
		fmt.Println("打印UOW信息")
	}
	for _, uow := range uow_extend {
		switch {
		case uow.HexId != "":
			fmt.Printf("SQL语句为:%s\n", db2.NewMonGetPkgCacheStmt(uow.HexId).StmtText)
		case uow.UtilInvId != "":
			stmt_text := ""
			for _, u := range utils {
				if u.UtilInvId == uow.UtilInvId {
					stmt_text = u.StmtText
				}
			}
			fmt.Printf("Util语句为:%s\n", stmt_text)
		}
	}
	//打印锁等待相关信息
	if len(locks) > 0 {
		fmt.Printf("当前锁等待个数为:%d\n", len(locks))
		fmt.Printf("打印当前锁等待信息\n")
		for _, lock := range locks {
			fmt.Printf("当前agent:%d,等待时长:%s,LockMode:%s,SQL:%s\n",
				lock.ReqAgentTid, lock.SnapTime.Sub(lock.LockWaitStartTime).String(),
				lock.LockMode, db2.NewMonGetPkgCacheStmt(lock.ReqHexId).StmtText)
		}
		lws := db2.GetLockHeaderMap(locks)
		if len(lws) > 0 {
			fmt.Println("打印锁源头列表以及语句")
			sql := ""
			for _, v := range lws {
				for _, a := range acts {
					if v == a.AppHandle {
						sql = db2.NewMonGetPkgCacheStmt(a.HexId).StmtText
					}
				}
				fmt.Printf("APPHanld:%d,语句:%s\n", v, sql)
			}
		}

	}
}
