package db2

import (
	"bytes"
	"os/exec"
	"strings"
	"time"
)

//记录Hadr同步状态信息
type MonGetHadr struct {
	MataData
	SnapTime        time.Time `column:"CURRENT TIMESTAMP"`
	HdrRole         string    `column:"HADR_ROLE"`     //hadr主从角色
	HdrMode         string    `column:"HADR_SYNCMODE"` //hadr复制模式,ASYNC,STANDBY ,SYNC ,SUPERASYNC
	StandbyId       int64     `column:"STANDBY_ID"`
	HdrState        string    `column:"HADR_STATE"`          //bytes
	PrimaryHost     string    `column:"PRIMARY_MEMBER_HOST"` //第一个活动日志号
	PrimaryInstance string    `column:"PRIMARY_INSTANCE"`
	StandbyHost     string    `column:"STANDBY_MEMBER_HOST"`
	StandbyInstance string    `column:"STANDBY_INSTANCE"`
	HdrStatus       string    `column:"HADR_CONNECT_STATUS"`
	HdrStatusTime   time.Time `column:"HADR_CONNECT_STATUS"`
}

func NewMonGetHadr() *MonGetHadr {
	m := new(MonGetHadr)
	m.rep = mon_get_rep
	m.tabname = "TABLE(MON_GET_HADR(-1))"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetHadr) GetSqlText() string {
	return genSql(m)
}

func GetMonGetHadrList() ([]*MonGetHadr, error) {
	m := NewMonGetHadr()
	argSql := m.GetSqlText()
	cmd := exec.Command("db2", "+p", "-x", "-t")
	var in bytes.Buffer
	cmd.Stdin = &in
	log.Debug(argSql)
	in.WriteString(argSql)
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	return getMonGetHadrListFromStr(result), err
}

//通过从数据库返回的结果，生成结果集
func getMonGetHadrListFromStr(str string) []*MonGetHadr {
	m := NewMonGetHadr()
	ms := make([]*MonGetHadr, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetHadr()
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
