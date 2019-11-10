package db2

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

//获取所有基础数据

func CollectData(db string) ([]*MonGetActStmt, []*MonGetTrxLog, []*MonGetHadr, []*MonGetCurUow, error) {
	mon_get_act_stmt := NewMonGetActStmt()
	mon_get_trx_log := NewMonGetTrxLog()
	mon_get_hdr := NewMonGetHadr()
	mon_get_cur_uow := NewMonGetCurUow()
	sql_text_list := []string{mon_get_act_stmt.GetSqlText(), mon_get_trx_log.GetSqlText(), mon_get_hdr.GetSqlText(), mon_get_cur_uow.GetSqlText()}
	cmd := exec.Command("db2", "+p", "-x", "-t")
	var in bytes.Buffer
	cmd.Stdin = &in
	in.WriteString(fmt.Sprintf("connect to %s ;\n", db))
	in.WriteString(strings.Join(sql_text_list, ""))
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	//对于sql语句，如果结果大于0则是告警，可以忽略
	if err != nil {
		if v, e := strconv.Atoi(strings.Fields(err.Error())[len(strings.Fields(err.Error()))-1]); e == nil && v >= 0 {
			err = nil
		}
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return GetMonGetActStmtList(result), GetMonGetTrxLogList(result), GetMonGetHadrList(result), GetMonGetCurUowList(result), nil

}
