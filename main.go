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
	acts, Trxlogs, _, _, locks, utils, uow_extends, _ := db2.CollectPerfData("sample", time.Duration(time.Second*10))
	//整体情况分析：1、 是否存在大事务；2、找出最古老事务；3、当前活动连接情况；4、当前锁等待情况
	//给定一个APP查看当前SQL语句或者最近一次SQL语句
	findSQL := func(appHandle int32) string {
		if uow, ok := db2.LookupMonGetCurUowExtendByAppHandle(uow_extends, appHandle); ok {
			switch {
			case uow.HexId != "":
				return db2.NewMonGetPkgCacheStmt(uow.HexId).StmtText
			case uow.UtilInvId != "":
				if util, ok := db2.LookupMonGetUtilByUtilInvId(utils, uow.UtilInvId); ok {
					return util.StmtText
				}
			case uow.LastHexId != "":
				return db2.NewMonGetPkgCacheStmt(uow.LastHexId).StmtText
			}
		}
		return ""
	}
	for _, l := range Trxlogs {
		if uow, ok := db2.LookupMonGetCurUowExtendByAppHandle(uow_extends, l.OldestXact); ok {
			fmt.Printf("当前最古老事务AppHandle:%d,日志占用量为:%s,语句为:%s\n",
				uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), findSQL(uow.AppHandle))
		}

	}
	//日志空间使用率超过400MB即称作为大事务
	BigTrxLogLimit := 400 << 20 //400MB
	bigTrxUow := db2.BigTrxUow(uow_extends, BigTrxLogLimit)
	fmt.Printf("当前是否存在大事务?(%t) %d个\n", len(bigTrxUow) != 0, len(bigTrxUow))
	for _, uow := range bigTrxUow {
		fmt.Printf("大事务AppHandle:%-10d,LogUsed:%-10s,开始时间:%-10s,\n    当前语句:%s\n",
			uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), uow.UowStartTime.String(),
			findSQL(uow.AppHandle))
	}
	fmt.Printf("当前是否存在锁等待?%d个\n", len(locks))
	lockHeaders := db2.GetLockHeaderMap(locks)
	if len(lockHeaders) > 0 {
		fmt.Printf("打印锁等待的Header信息\n")
	}
	for _, header := range lockHeaders {
		if uow, ok := db2.LookupMonGetCurUowExtendByAppHandle(uow_extends, header); ok {
			//判断事务是否为大事务,是否执行过DDL语句
			var (
				isBigTrx bool
				isDDLUow bool
			)
			for _, uow := range bigTrxUow {
				if uow.AppHandle == header {
					isBigTrx = true
				}
				if uow.DDLStmts > 0 {
					isDDLUow = true
				}
				if isBigTrx && isDDLUow {
					break
				}
			}
			fmt.Printf("LockHolder的AppHandle为:%d,事务大小:%s,是否大事务?(%t),是否包含DDL语句?(%t),\n    当前语句:%s\n",
				uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), isBigTrx, isDDLUow,
				findSQL(uow.AppHandle))
		} else {
			fmt.Printf("LockHolder的AppHandle为:%d,但未获取所在事务信息\n", header)
		}
	}
	if len(locks) > 0 {
		fmt.Printf("打印处于锁等待状态的SQL语句信息,(当前锁等等待语句数为:%d)\n", len(locks))
	}
	for _, lock := range locks {
		fmt.Printf("当前AppHandle:%d,等待时长:%s,LockMode:%s,\n    处于等待状态的语句:%s\n",
			lock.ReqAgentTid, lock.SnapTime.Sub(lock.LockWaitStartTime).String(),
			lock.LockMode, findSQL(lock.ReqAppHandle))
	}
	//查看当前活动状态
	act_stmts := db2.GetMonGetActStmtAggByPlanid(acts)
	fmt.Printf("当前处于活动状态（不含任何等待状态语句)的语句个数为:%d\n", len(acts))
	for _, act := range act_stmts {
		applist := make([]string, 0)
		for _, handle := range act.AppHandleList {
			applist = append(applist, strconv.Itoa(int(handle)))
		}
		fmt.Printf("执行次数:%-10d,\n    相似SQL涉及APPHandle列表为:%s,"+
			"\n    执行语句：%s\n", act.ActCount, db2.NewMonGetPkgCacheStmt(act.HexId).StmtText,
			strings.Join(applist, ","))
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
					fmt.Printf("    对象信息:,对象类型:%-10s,对象名:%-20s,统计信息记录数:%-10d,最近变更记录数:%-10d,"+
						"索引FuKCard值:%-10d,是否小表突变?(%t)\n",
						obj.ObjType, obj.ObjName, obj.RowCount, obj.SRowsModified, obj.FUKCard, obj.RowCount < obj.SRowsModified)
				}
			}
		}
		fmt.Printf("    打印Advis信息,执行者:%-30s,执行语句:%s\n", act.AuthId, db2.NewMonGetPkgCacheStmt(act.HexId).StmtText)

	}

}
