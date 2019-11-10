package db2

import (
	"strings"
	"time"
)

//记录活动日志的信息，每个数据库就一条该信息
type MonGetHadr struct {
	MataData
	SnapTime        time.Time `column:"CURRENT TIMESTAMP"`
	HdrRole         string    `column:"HADR_ROLE"`     //hadr主从角色
	HdrMode         string    `column:"HADR_SYNCMODE"` //hadr复制模式,ASYNC,STANDBY ,SYNC ,SUPERASYNC
	StandbyId       int       `column:"STANDBY_ID"`
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

//通过从数据库返回的结果，生成结果集
func GetMonGetHadrList(str string) []*MonGetHadr {
	m := NewMonGetTrxLog()
	ms := make([]*MonGetHadr, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		d := NewMonGetHadr()
		if err := renderStruct(d, line); err != nil {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}
