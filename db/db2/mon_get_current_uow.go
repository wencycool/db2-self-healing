package db2

import (
	"strings"
	"time"
)

//获取当前正在执行的uow事务，只包含未做提交的所有事务
type MonGetCurUow struct {
	MataData
	SnapTime         time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle        int       `column:"APPLICATION_HANDLE"`
	UowId            int       `column:"UOW_ID"`
	UowStartTime     time.Time `column:"UOW_START_TIME"`
	AuthId           string    `column:"SESSION_AUTH_ID"`
	DDLSqlStmts      int       `column:"DDL_SQL_STMTS"`
	NumLocksHeld     int       `column:"NUM_LOCKS_HELD"`
	SinceLastCmtSqls int       `column:"SQL_REQS_SINCE_COMMIT"`
	UowLogSpaceUsed  int       `column:"UOW_LOG_SPACE_USED"`
	LastHexId        string    `column:"LAST_EXECUTABLE_ID"`
	TotalRunstats    int       `column:"TOTAL_RUNSTATS"`
	TotalReorgs      int       `column:"TOTAL_REORGS"`
	TotalLoads       int       `column:"TOTAL_LOADS"`
}

func NewMonGetCurUow() *MonGetCurUow {
	m := new(MonGetCurUow)
	m.rep = mon_get_rep
	m.tabname = "table(MON_GET_UNIT_OF_WORK(null,-1)) as t where t.UOW_STOP_TIME is null"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetCurUow) GetSqlText() string {
	return genSql(m)
}

//通过从数据库返回的结果，生成结果集
func GetMonGetCurUowList(str string) []*MonGetCurUow {
	m := NewMonGetCurUow()
	ms := make([]*MonGetCurUow, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetCurUow()
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
