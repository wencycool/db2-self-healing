package db2

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

var curAppId string
var curAppHandle int64

//判断agent是否可以进行做force操作，主要包括是否大事务，是否包含reorg等DDL操作
func CurrentAppId() string {
	if curAppId != "" {
		return curAppId
	}
	bs, err := exec.Command("db2", "-x", "+p", "values application_id()").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(bs))
}

func CurrentAppHandle() int64 {
	if curAppHandle != 0 {
		return curAppHandle
	}
	bs, err := exec.Command("db2", "-x", "+p", "values mon_get_application_handle()").CombinedOutput()
	if err != nil {
		return -1
	}
	r, err := strconv.Atoi(strings.TrimSpace(string(bs)))
	if err != nil {
		return -1
	}
	return int64(r)
}

type ForceApplicationLevelType int //是否可以杀APP的等级判断
type ForceApplicationType int      //是否杀掉App DO：直接杀掉,SEE查看

const (
	FORCE_APP_LEVEL_IMMEDIATE  ForceApplicationLevelType = 0 //对于当前agent全部杀掉，不考虑任何情况
	FORCE_APP_LEVEL_NODDL      ForceApplicationLevelType = 1 //对于当前agent，考虑DDL执行、大事务,离线reorg处于index rebuild状态对于此类情况不杀，其余情况杀掉
	FORCE_APP_LEVEL_ACTNOTWMDL ForceApplicationLevelType = 2 //对于当前agent，如果正在执行的SQL语句是写语句，那么不进行杀掉，如果是语句则杀掉。

	FORCE_APP_TYPE_DO  ForceApplicationType = 0 //执行删除动作
	FORCE_APP_TYPE_SEE ForceApplicationType = 1 //只查看不删除
)

//判断一个应用是否可以杀掉，需要结合UOW，ACT以及UTIL这三个进行判断
type FatUowApplication struct {
	isAct    bool //判断当前活动类型是否activity
	isUtil   bool //判断当前活动是否utility
	isForced bool //判断是否已经被杀掉
	uow      *MonGetCurUowExtend
	act      *MonGetActStmt
	util     *MonGetUtil
}

//以uow为主，检查当前正在执行的SQL是什么SQL
func NewFatUowApplications(uows []*MonGetCurUowExtend, acts []*MonGetActStmt, utils []*MonGetUtil) FatUowApplicationList {
	result := make([]*FatUowApplication, 0)
	for _, uow := range uows {
		newApp := new(FatUowApplication)
		newApp.uow = uow
		//检查uow是否activity
		for _, act := range acts {
			if uow.AppHandle == act.AppHandle {
				newApp.isAct = true
				newApp.act = act
			}
		}
		//检查uow是否包含util
		for _, util := range utils {
			if uow.AppHandle == util.AppHandle {
				newApp.isUtil = true
				newApp.util = util
			}
		}
		result = append(result, newApp)
	}
	return result
}

//根据判断是否可以杀掉
func (f *FatUowApplication) canForce(level ForceApplicationLevelType) (canforce bool, msg string) {
	if level == FORCE_APP_LEVEL_IMMEDIATE {
		return true, "强制杀掉"
	}
	if level == FORCE_APP_LEVEL_NODDL {
		//检查是否包含DDL语句，大事务，以及reorg状态的rebuild index阶段
		switch {
		case f.uow.UowLogSpaceUsed > MAX_FORCE_LOG_USED_LIMIT:
			return false, "日志量过大，不能杀掉。日志量:" + ByteSizeFormat(f.uow.UowLogSpaceUsed)
		case f.uow.DDLStmts > 0:
			return false, "事务中包含DDL语句，不能杀掉"
		case f.isUtil && f.util.UtilType == "REORG" && f.util.UtilOperType == "T":
			//jiancha 是否重建索引阶段
			bs, err := exec.Command("db2", "+p", "-x", "select TABSCHEMA,TABNAME,REORG_PHASE,REORG_TYPE from  table(snap_get_tab_reorg('')) t where REORG_STATUS='STARTED' with ur").CombinedOutput()
			if err != nil {
				log.Trace(string(bs))
			}
			for _, line := range strings.Split(string(bs), "\n") {
				fields := strings.Fields(strings.TrimSpace(line))
				if len(fields) == 4 && f.util.ObjSchema == fields[0] && f.util.ObjName == fields[1] && fields[2] == "INDEX_RECREATE" {
					return false, "索引rebuild阶段，不能杀掉"
				}
			}
			return true, "不在rebuild索引节点，可以杀掉"
		default:
			return true, ""
		}
	}
	if level == FORCE_APP_LEVEL_ACTNOTWMDL {
		if ok, msg := f.canForce(FORCE_APP_LEVEL_NODDL); !ok {
			return ok, msg
		} else if f.isAct && f.act.ActType == "WRITE_DML" {
			return false, "当前的SQL是写操作，不能删除"
		} else {
			return true, ""
		}
	}
	return true, "不在禁止范围之内，可以杀掉"
}

//杀掉单个app
func (f *FatUowApplication) forceApp(level ForceApplicationLevelType) (forced bool, msg string) {
	if ok, msg := f.canForce(level); !ok {
		return ok, msg
	}
	if f.isForced {
		return true, "Application handle已经被杀掉了"
	}
	bs, err := exec.Command("db2", fmt.Sprintf("force application(%d)", f.uow.AppHandle)).CombinedOutput()
	if checkDbErr(err) != nil {
		f.isForced = false
		return true, fmt.Sprintf("Application Handle:%d forced error!,msg:%s", f.uow.AppHandle, string(bs))
	} else {
		f.isForced = true
		return true, fmt.Sprintf("Application Handle:%d forced sucessfully", f.uow.AppHandle)
	}

}

type FatUowApplicationList []*FatUowApplication

//杀掉application，返回已经杀掉和未杀掉的app列表
func (fs FatUowApplicationList) ForceAppByHandle(handles []int64, level ForceApplicationLevelType) []*ForcedMsg {
	result := make([]*ForcedMsg, 0)
	for _, handle := range handles {
		for _, f := range fs {
			if handle == f.uow.AppHandle {
				ok, msg := f.forceApp(level)
				result = append(result, &ForcedMsg{
					AppHandle: handle,
					IsForced:  ok,
					Msg:       msg,
					Uow:       f.uow,
				})
			}
		}
	}
	return result
}

//模糊搜索查询SQL的APP
func (fs FatUowApplicationList) ForceAppByLikeStmt(sqlLike string, level ForceApplicationLevelType, doOrSee ForceApplicationType) []*ForcedMsg {
	result := make([]*ForcedMsg, 0)
	for i, f := range fs {
		if !f.isForced {
			full_sql_text := "NoSQL-9999999"
			if f.isAct {
				full_sql_text = NewMonGetPkgCacheStmt(f.act.HexId).StmtText
			} else if f.isUtil {
				full_sql_text = f.util.StmtText
			}

			if strings.Contains(strings.ToLower(full_sql_text), strings.ToLower(sqlLike)) {
				forceMsg := new(ForcedMsg)
				if doOrSee == FORCE_APP_TYPE_DO {
					forceMsg.IsForced, forceMsg.Msg = f.forceApp(level)
					forceMsg.AppHandle = f.uow.AppHandle
					if forceMsg.IsForced {
						fs[i].isForced = true
						forceMsg.Msg = "SQL:" + full_sql_text
					}
				} else if doOrSee == FORCE_APP_TYPE_SEE {
					//只是查看
					forceMsg.IsForced = false
					forceMsg.AppHandle = f.uow.AppHandle
					forceMsg.Msg = "SQL:" + full_sql_text
				}
				forceMsg.Uow = f.uow
				result = append(result, forceMsg)
			}
		}
	}
	return result
}

type ForcedMsg struct {
	AppHandle int64
	IsForced  bool
	Msg       string
	Uow       *MonGetCurUowExtend
}
