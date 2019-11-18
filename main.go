package main

import (
	"fmt"
	"github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
	"my/vm"
	"strconv"
	"strings"
	"time"
)

func init() {
	//控制内存使用量100MB
	vm.MemLimit(100 << 20)
}
func main() {
	var dbname string
	dbname = "sample"
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.PanicLevel)
	log.SetFormatter(&logrus.TextFormatter{})
	db2.LogRegister(log)
	acts, Trxlogs, _, _, locks, utils, uow_extends, _ := db2.CollectPerfData(dbname, time.Duration(time.Second*10))
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
	BigTrxLogLimit := 100 << 20 //400MB
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
	if len(locks) > 20 {
		fmt.Printf("锁等待个数太多，只显示10条")
		locks = locks[:10]
	}
	for _, lock := range locks {
		fmt.Printf("当前AppHandle:%d,等待时长:%s,LOCK_OBJECT_TYPE:%s,LOCK_MODE:%s,LOCK_MODE_REQUESTED:%s,LOCK_STATUS:%s\n    处于等待状态的语句:%s\n",
			lock.ReqAppHandle, lock.SnapTime.Sub(lock.LockWaitStartTime).String(),
			lock.LockObjType, lock.LockMode, lock.LockModeReq, lock.LockStatus, findSQL(lock.ReqAppHandle))
	}
	//查看当前活动状态
	curAppHandle := db2.CurrentAppHandle()
	acts_except_mon_get := make([]*db2.MonGetActStmt, 0)
	for _, act := range acts {
		if act.AppHandle == curAppHandle {
			continue
		}
		acts_except_mon_get = append(acts_except_mon_get, act)
	}
	act_stmts := db2.GetMonGetActStmtAggByPlanid(acts_except_mon_get)
	fmt.Printf("当前处于活动状态（不含任何等待状态语句)的语句个数为:%d\n", len(acts))
	for i, act := range act_stmts {
		fmt.Printf("  [第%d条语句] 执行次数:%-10d,\n    相似SQL涉及APPHandle列表为:%s"+
			"\n    执行语句：%s\n", i+1, act.ActCount, intListToStr(act.AppHandleList, ","), db2.NewMonGetPkgCacheStmt(act.HexId).StmtText)
		fmt.Println("对每一个SQL进行解析，检查执行计划")
		if act.HexId == "" {
			continue
		}
		expln, err := db2.NewMonGetExplain(act.HexId)
		if err != nil {
			fmt.Println(err)
		} else {
			//获取执行计划上的对象信息
			if objs, err := expln.GetObj(); err != nil {
				fmt.Println(err)
			} else {
				for _, obj := range objs {
					fmt.Printf("    对象信息:,对象类型:%-10s,对象名:%-20s,统计信息记录数:%-10d,最近变更记录数:%-10d,"+
						"索引FuKCard值:%-10d,是否小表突变?(%t)\n",
						obj.ObjType, obj.ObjName, obj.RowCount, obj.SRowsModified, obj.FUKCard, obj.RowCount < obj.SRowsModified)
				}
			}
			//获取stream信息
			streams, err := expln.GetStream()
			if err != nil {
				fmt.Printf("打印Node报错：%s\n", err)
			} else {
				streamNode := db2.NewNode(streams)
				//streamNode.PrintData()
				t1 := time.Now()
				fmt.Printf("检查是否包含HashJoin			%t    	--高并发交易SQL不应出现\n", streamNode.HasHSJoin())
				fmt.Printf("检查NLJoin右子树是否包含IXAND:		%t    	--高并发交易SQL不应出现\n", streamNode.HasRightOperatorIXAnd())
				fmt.Printf("检查NLJoin右子树是否包含TabScan：	%t		--任何SQL不应出现\n", streamNode.HasRightOperatorTabScan())
				fmt.Printf("打印一共花费时长:%s\n", time.Now().Sub(t1).String())
			}

		}
		fmt.Printf("    打印Advis信息,执行语句:db2advis -d %s -s \"%s\" -q %s -n %s \n", dbname, db2.NewMonGetPkgCacheStmt(act.HexId).StmtText, act.AuthId, act.AuthId)

	}
	//测试执行计划

}

func intListToStr(l []int32, rep string) string {
	r := make([]string, 0)
	for _, v := range l {
		r = append(r, strconv.Itoa(int(v)))
	}
	return strings.Join(r, rep)
}
