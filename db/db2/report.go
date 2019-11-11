package db2

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//获取所有基础数据
//设置5秒的超时时间
func CollectData(db string, duration time.Duration) ([]*MonGetActStmt, []*MonGetTrxLog, []*MonGetHadr, []*MonGetCurUow, []*MonGetLockWait, error) {
	mon_get_act_stmt := NewMonGetActStmt()
	mon_get_trx_log := NewMonGetTrxLog()
	mon_get_hdr := NewMonGetHadr()
	mon_get_cur_uow := NewMonGetCurUow()
	mon_get_lockwait := NewMonGetLockWait()
	sql_text_list := []string{mon_get_act_stmt.GetSqlText(), mon_get_trx_log.GetSqlText(), mon_get_hdr.GetSqlText(), mon_get_cur_uow.GetSqlText(), mon_get_lockwait.GetSqlText()}
	cmd := exec.Command("db2", "+p", "-x", "-t")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} //设置进程组,方便杀掉相关子进程
	var in bytes.Buffer
	cmd.Stdin = &in
	sql_text := strings.Join(sql_text_list, "")
	log.Debug(sql_text)
	in.WriteString(fmt.Sprintf("connect to %s ;\n", db))
	in.WriteString(sql_text)
	//设置超时
	time.AfterFunc(duration, func() {
		//判断pid是否大于0，如果不大于0，则不进行杀掉
		if cmd.Process.Pid > 0 {
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			log.Error(err)
		}

	})
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	//对于sql语句，如果结果大于0则是告警，可以忽略
	if err != nil {
		if v, e := strconv.Atoi(strings.Fields(err.Error())[len(strings.Fields(err.Error()))-1]); e == nil && v >= 0 {
			err = nil
		}
	}
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}
	return GetMonGetActStmtList(result), GetMonGetTrxLogList(result),
		GetMonGetHadrList(result), GetMonGetCurUowList(result),
		GetMonGetLockWaitList(result), nil

}
