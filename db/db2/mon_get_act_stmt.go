package db2

import (
	"strings"
	"time"
)

type MonGetActStmt struct {
	MataData
	SnapTime  time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle int       `column:"APPLICATION_HANDLE"`
	ActId     int       `column:"ACTIVITY_ID"`
	ActType   string    `column:"ACTIVITY_TYPE"`
	StmtId    int       `column:"STMTID"`
	PlanId    int       `column:"PLANID"`
	HexId     string    `column:"EXECUTABLE_ID"`
	StmtNo    int       `column:"STMTNO"`
	CpuTime   int       `column:"TOTAL_CPU_TIME"`
	ActState  string    `column:"ACTIVITY_STATE"`
}

func NewMonGetActStmt() *MonGetActStmt {
	m := new(MonGetActStmt)
	m.rep = mon_get_rep
	m.tabname = "TABLE(MON_GET_ACTIVITY(NULL, -1))"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetActStmt) GetSqlText() string {
	return genSql(m)
}

//通过从数据库返回的结果，生成结果集
func GetMonGetActStmtList(str string) []*MonGetActStmt {
	m := NewMonGetActStmt()
	ms := make([]*MonGetActStmt, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		d := NewMonGetActStmt()
		if err := renderStruct(d, line); err != nil {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}
