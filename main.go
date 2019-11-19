package main

import (
	"flag"
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
	//控制内存使用量500MB
	vm.MemLimit(500 << 20)
}
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
	acts, Trxlogs, _, _, locks, utils, uow_extends, _ := db2.CollectPerfData(time.Duration(time.Second * 10))
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
			fmt.Printf("[当前最古老事务]AppHandle:%d,日志占用量为:%s,语句为:%s\n",
				uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), findSQL(uow.AppHandle))
		}

	}
	//日志空间使用率超过400MB即称作为大事务
	BigTrxLogLimit := 100 << 20 //400MB
	bigTrxUow := db2.BigTrxUow(uow_extends, BigTrxLogLimit)
	fmt.Printf("\n当前是否存在大事务?(%s) %s个\n", PrintColorf(len(bigTrxUow) > 0, len(bigTrxUow) > 0), PrintColorf(len(bigTrxUow), len(bigTrxUow) > 0))
	for _, uow := range bigTrxUow {
		fmt.Printf("大事务AppHandle:%-10d,LogUsed:%-10s,开始时间:%-10s,\n    当前语句:%s\n",
			uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), uow.UowStartTime.String(),
			findSQL(uow.AppHandle))
	}
	fmt.Printf("\n当前是否存在锁等待? %s个\n", PrintColorf(len(locks), len(locks) > 0))
	lockHeaders := db2.GetLockHeaderMap(locks)
	if len(lockHeaders) > 0 {
		fmt.Printf("\033[1;40;32m[打印锁等待的Header信息]\033[0m\n")
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
			fmt.Printf("LockHolder的AppHandle为:%d,事务大小:%s,是否大事务?(%s),是否包含DDL语句?(%s),\n    当前语句:%s\n",
				uow.AppHandle, db2.ByteSizeFormat(uow.UowLogSpaceUsed), PrintColorf(isBigTrx, isBigTrx), PrintColorf(isDDLUow, isDDLUow),
				findSQL(uow.AppHandle))
		} else {
			fmt.Printf("LockHolder的AppHandle为:%d,但未获取所在事务信息\n", header)
		}
	}
	if len(locks) > 0 {
		fmt.Printf("打印处于锁等待状态的SQL语句信息,(当前锁等等待语句数为:%s)\n", PrintColorf(len(locks), len(locks) > 0))
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
	fmt.Printf("\n当前处于活动状态的语句个数为:%s\n", PrintColorf(len(acts_except_mon_get), len(acts_except_mon_get) > 0))
	totalTimeSpend := func() int {
		Total_timeSpend := 0
		for _, act := range act_stmts {
			Total_timeSpend = Total_timeSpend + act.TimeSpend
		}
		return Total_timeSpend
	}()
	for i, act := range act_stmts {
		//SQL属性信息
		pkgCacheStmt := db2.NewMonGetPkgCacheStmt(act.HexId)
		fmt.Printf("  [第%d条语句] 执行次数:%-10d\n    相似SQL涉及APPHandle列表为:%s"+
			"\n    执行者:%s\n    执行语句：%s\n", i+1, act.ActCount, intListToStr(act.AppHandleList, ","), act.AuthId, pkgCacheStmt.StmtText)
		//耗时分析,分析当前语句执行耗时情况
		if act.ActTime > 0 {
			fmt.Printf("    \033[4;40;32m总执行时间占比:%-5d%%\033[0m,参与计算SQL数:%-5d,总执行时间:%-5dms,等待时间:%-5dms,\n    数据逻辑读:%-10d,数据逻辑读:%-10d索引物理读:%-10d,索引逻辑读:%-10d,"+
				"临时表空间逻辑读:%-10d,临时表空间物理读:%10d\n", act.TimeSpend*100/totalTimeSpend, act.ActDataCount, act.ActTime, act.ActWTime, act.PoolDLReads, act.PoolDPReads, act.PoolILReads,
				act.PoolIPReads, act.PoolTmpDLReads, act.PoolTmpDPReads)
		} else {
			//因为activity表延迟采集数据的问题，导致本来毫秒执行完毕的SQL当执行花费几秒之内也不会被采集数据（10秒左右才会采集，或者其它触发点)
			fmt.Printf("    \033[4;40;32m总执行时间占比:%-5d%%\033[0m\n", act.TimeSpend*100/totalTimeSpend)
		}
		if act.HexId == "" {
			continue
		}

		fmt.Println("    对每一个SQL进行解析，检查执行计划")
		fmt.Printf("    语句属性:PackageSchema:%-10s,PackageName:%-20s,Section:%-3d,SQLType:%-10s\n", pkgCacheStmt.PkgSchema, pkgCacheStmt.PkgName, pkgCacheStmt.Section, pkgCacheStmt.SectionType)
		//执行计划分析
		expln, err := db2.NewMonGetExplain(act.HexId)
		if err != nil {
			fmt.Println(err)
		} else {
			//获取执行计划上的对象信息
			if objs, err := expln.GetObj(); err != nil {
				fmt.Println(err)
			} else {
				runstatsTableList := make([]*db2.MonGetExplainObj, 0)
				for _, obj := range objs {
					fmt.Printf("    对象信息:,对象类型:%-10s,对象名:%-20s,统计信息记录数:%-10d,最近变更记录数:%-10d,"+
						"索引FuKCard值:%-10d,是否小表突变?(%s)\n",
						obj.ObjType, obj.ObjName, obj.RowCount, obj.SRowsModified, obj.FUKCard,
						PrintColorf(obj.RowCount < obj.SRowsModified, obj.RowCount < obj.SRowsModified))
					//如果变更记录过大则搜集统计信息
					if obj.RowCount < obj.SRowsModified && obj.ObjType == "TA" {
						runstatsTableList = append(runstatsTableList, obj)
					}
				}
				for _, obj := range runstatsTableList {
					//推荐做统计信息搜集
					//当数据大于1000万则做system抽样，不要用BERNOULLI抽样并不会加快速度
					tablesampledRatio := 100
					if (obj.RowCount + obj.SRowsModified) > 10000000 {
						tablesampledRatio = 20
					}
					fmt.Printf("    推荐执行:runstats on table %s.%s with distribution on all columns and sampled detailed index all allow write access tablesample system (%d)\n",
						obj.ObjSchema, obj.ObjName, tablesampledRatio)
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
				fmt.Printf("      检查是否包含HashJoin			%s    	--高并发交易SQL不应出现\n", PrintColorf(streamNode.HasHSJoin(), streamNode.HasHSJoin()))
				fmt.Printf("      检查NLJoin右子树是否包含IXAND:		%s    	--高并发交易SQL不应出现\n", PrintColorf(streamNode.HasRightOperatorIXAnd(), streamNode.HasRightOperatorIXAnd()))
				fmt.Printf("      检查NLJoin右子树是否包含TabScan：	%s		--任何SQL不应出现\n", PrintColorf(streamNode.HasRightOperatorTabScan(), streamNode.HasRightOperatorTabScan()))
				fmt.Printf("      打印一共花费时长:%s\n", time.Now().Sub(t1).String())
			}

		}
		fmt.Printf("    打印Advis信息,执行语句:db2advis -d %s -s \"%s\" -q %s -n %s \n", dbname, pkgCacheStmt.StmtText, act.AuthId, act.AuthId)

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

/*
fmt.Printf("\n %c[1;40;32m%s%c[0m\n\n", 0x1B, "testPrintColor", 0x1B)
其中0x1B是标记，[开始定义颜色，1代表高亮，40代表黑色背景，32代表绿色前景，0代表恢复默认颜色
// 前景 背景 颜色
    // ---------------------------------------
    // 30  40  黑色
    // 31  41  红色
    // 32  42  绿色
    // 33  43  黄色
    // 34  44  蓝色
    // 35  45  紫红色
    // 36  46  青蓝色
    // 37  47  白色
    //
    // 代码 意义
    // -------------------------
    //  0  终端默认设置
    //  1  高亮显示
    //  4  使用下划线
    //  5  闪烁
    //  7  反白显示
    //  8  不可见
*/
//打印带颜色的输出
func PrintColorf(a interface{}, flag bool) string {
	if flag {
		return fmt.Sprintf("%c[5;47;31m%v%c[0m", 0x1B, a, 0x1B)
	}
	return fmt.Sprintf("%v", a)
}
