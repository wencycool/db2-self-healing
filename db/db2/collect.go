package db2

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//获取所有性能相关数据
//设置5秒的超时时间
func CollectPerfData(duration time.Duration) ([]*MonGetActStmt, []*MonGetTrxLog, []*MonGetHadr, []*MonGetCurUow, []*MonGetLockWait, []*MonGetUtil, []*MonGetCurUowExtend, error) {
	mon_get_act_stmt := NewMonGetActStmt()
	mon_get_trx_log := NewMonGetTrxLog()
	mon_get_hdr := NewMonGetHadr()
	mon_get_cur_uow := NewMonGetCurUow()
	mon_get_lockwait := NewMonGetLockWait()
	mon_get_util := NewMonGetUtil()
	mon_get_cur_uow_extend := NewMonGetCurUowExtend()
	sql_text_list := []string{mon_get_act_stmt.GetSqlText(), mon_get_trx_log.GetSqlText(),
		mon_get_hdr.GetSqlText(), mon_get_cur_uow.GetSqlText(),
		mon_get_lockwait.GetSqlText(), mon_get_util.GetSqlText(),
		mon_get_cur_uow_extend.GetSqlText()}
	t1 := time.Now()
	cmd := exec.Command("db2", "+p", "-x", "-t")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} //设置进程组,方便杀掉相关子进程
	var in bytes.Buffer
	cmd.Stdin = &in
	sql_text := strings.Join(sql_text_list, "")
	log.Debug(sql_text)
	in.WriteString(sql_text)
	//设置超时
	time.AfterFunc(duration, func() {
		//判断pid是否大于0，如果不大于0，则不进行杀掉
		if cmd.Process.Pid > 0 && !cmd.ProcessState.Exited() {
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			log.Error(err)
		}

	})
	bs, err := cmd.CombinedOutput()
	log.Infof("获取表结构一共花费时间为:%s\n", time.Now().Sub(t1).String())
	result := string(bs)
	//对于sql语句，如果结果大于0则是告警，可以忽略
	if err != nil {
		if v, e := strconv.Atoi(strings.Fields(err.Error())[len(strings.Fields(err.Error()))-1]); e == nil && v >= 0 {
			err = nil
		}
	}
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	return GetMonGetActStmtList(result), GetMonGetTrxLogList(result),
		GetMonGetHadrList(result), GetMonGetCurUowList(result),
		GetMonGetLockWaitList(result), GetMonGetUtilList(result), GetMonGetCurUowExtendList(result), nil

}

//获取表空间数据
func CollectTbspData(db string, duration time.Duration) {

}
