package db2

import (
	"bytes"
	"os/exec"
	"strings"
	"time"
)

//记录活动日志的信息，每个数据库就一条该信息
type MonGetTrxLog struct {
	MataData
	SnapTime    time.Time `column:"CURRENT TIMESTAMP"`
	TotalAval   int       `column:"TOTAL_LOG_AVAILABLE"`
	TotalUsed   int       `column:"TOTAL_LOG_USED"`
	OldestXact  int64     `column:"APPLID_HOLDING_OLDEST_XACT"`
	RedoSize    int       `column:"LOG_TO_REDO_FOR_RECOVERY"` //bytes
	FirstActLog int64     `column:"FIRST_ACTIVE_LOG"`         //第一个活动日志号
	HdrWaitTime int       `column:"LOG_HADR_WAIT_TIME"`
	HdrWaits    int       `column:"LOG_HADR_WAITS_TOTAL"`
}

func NewMonGetTrxLog() *MonGetTrxLog {
	m := new(MonGetTrxLog)
	m.rep = mon_get_rep
	m.tabname = "TABLE(MON_GET_TRANSACTION_LOG(-1))"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetTrxLog) GetSqlText() string {
	return genSql(m)
}

func GetMonGetTrxLogList() ([]*MonGetTrxLog, error) {
	m := NewMonGetTrxLog()
	argSql := m.GetSqlText()
	cmd := exec.Command("db2", "+p", "-x", "-t")
	var in bytes.Buffer
	cmd.Stdin = &in
	log.Debug(argSql)
	in.WriteString(argSql)
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	return getMonGetTrxLogListFromStr(result), err
}

//通过从数据库返回的结果，生成结果集
func getMonGetTrxLogListFromStr(str string) []*MonGetTrxLog {
	m := NewMonGetTrxLog()
	ms := make([]*MonGetTrxLog, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetTrxLog()
		d.tabname = ""
		d.start_flag = ""
		d.end_flag = ""
		if err := renderStruct(d, line); err != nil {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}
