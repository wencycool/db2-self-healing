package db2

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

//判断agent是否可以进行做force操作，主要包括是否大事务，是否包含reorg等DDL操作
func CurrentAppId() string {
	bs, err := exec.Command("db2", "-x", "values application_id()").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(bs))
}

func CurrentAppHandle() int64 {
	bs, err := exec.Command("db2", "-x", "values mon_get_application_handle()").CombinedOutput()
	if err != nil {
		return -1
	}
	r, err := strconv.Atoi(strings.TrimSpace(string(bs)))
	if err != nil {
		return -1
	}
	return int64(r)
}

type ForceApplicationType int

const (
	FORCE_APP_IMMEDIATE ForceApplicationType = 0 //对于当前agent全部杀掉，不考虑任何情况
	FORCE_APP_NODDL     ForceApplicationType = 1 //对于当前agent，考虑DDL执行、大事务,离线reorg处于index rebuild状态对于此类情况不杀，其余情况杀掉
	FORCE_APP_NOWMDL    ForceApplicationType = 2 //对于当前agent，如果正在执行的SQL语句是写语句，那么不进行杀掉，如果是语句则杀掉。
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
func (f *FatUowApplication) canForce(level ForceApplicationType) (canforce bool, msg string) {
	if level == FORCE_APP_IMMEDIATE {
		return true, "强制杀掉"
	}
	if level == FORCE_APP_NODDL {
		//检查是否包含DDL语句，大事务，以及reorg状态的rebuild index阶段
		switch {
		case f.uow.UowLogSpaceUsed > MAX_FORCE_LOG_USED_LIMIT:
			return false, "日志量过大，不能杀掉。日志量:" + ByteSizeFormat(f.uow.UowLogSpaceUsed)
		case f.uow.DDLStmts > 0:
			return false, "事务中包含DDL语句，不能杀掉"
		case f.isUtil && f.util.UtilType == "REORG" && f.util.UtilOperType == "I":
			return false, "索引rebuild阶段，不能杀掉"
		default:
			return true, ""
		}
	}
	if level == FORCE_APP_NOWMDL {
		if ok, msg := f.canForce(FORCE_APP_NODDL); !ok {
			return ok, msg
		} else if f.isAct && f.act.ActType == "WRITE_DML" {
			return false, "当前的SQL是写操作，不能删除"
		} else {
			return true, ""
		}
	}
	return false, "找不到对应的force操作，不可以做force操作"
}

//杀掉单个app
func (f *FatUowApplication) forceApp(level ForceApplicationType) (forced bool, msg string) {
	if ok, msg := f.canForce(level); !ok {
		return ok, msg
	}
	if f.isForced {
		return true, "Application handle已经被杀掉了"
	}
	exec.Command("db2", fmt.Sprintf(" \"force application(%d)\"", f.uow.AppHandle)).CombinedOutput()
	f.isForced = true
	return true, fmt.Sprintf("Application Handle:%d forced sucessfully", f.uow.AppHandle)
}

type FatUowApplicationList []*FatUowApplication

//杀掉application，返回已经杀掉和未杀掉的app列表
func (fs FatUowApplicationList) ForceAppByHandle(handles []int64, level ForceApplicationType) []*ForcedMsg {
	result := make([]*ForcedMsg, 0)
	for _, handle := range handles {
		for _, f := range fs {
			if handle == f.uow.AppHandle {
				ok, msg := f.forceApp(level)
				result = append(result, &ForcedMsg{
					AppHandle: handle,
					IsForced:  ok,
					Msg:       msg,
				})
			}
		}
	}
	return result
}

//模糊搜索查询SQL的APP
func (fs FatUowApplicationList) ForceAppByLikeStmt(sqlLike string, level ForceApplicationType) []*ForcedMsg {
	result := make([]*ForcedMsg, 0)
	for i, f := range fs {
		if !f.isForced && f.isAct {
			full_sql_text := NewMonGetPkgCacheStmt(f.act.HexId).StmtText
			if strings.Contains(strings.ToLower(full_sql_text), strings.ToLower(sqlLike)) {
				forceMsg := new(ForcedMsg)
				forceMsg.IsForced, forceMsg.Msg = f.forceApp(level)
				forceMsg.AppHandle = f.act.AppHandle
				if forceMsg.IsForced {
					fs[i].isForced = true
					forceMsg.Msg = "SQL:" + full_sql_text
				}
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
}
