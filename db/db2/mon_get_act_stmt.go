package db2

import (
	"strings"
	"time"
)

type MonGetActStmt struct {
	MataData
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle     int       `column:"APPLICATION_HANDLE"`
	ActId         int       `column:"ACTIVITY_ID"`
	UowId         int       `column:"UOW_ID"`
	ActType       string    `column:"ACTIVITY_TYPE"`
	StmtId        int       `column:"STMTID"`
	PlanId        int       `column:"PLANID"`
	HexId         string    `column:"EXECUTABLE_ID"`
	StmtNo        int       `column:"STMTNO"`
	CpuTime       int       `column:"TOTAL_CPU_TIME"`
	ActState      string    `column:"ACTIVITY_STATE"`
	NestLevel     int       `column:"NESTING_LEVEL"` //记录嵌套层深，值越大说明被调用的层数越深
	TotalCpuTime  int       `column:"TOTAL_CPU_TIME"`
	TotalActTime  int       `column:"TOTAL_ACT_TIME"`      //总的执行时间milliseconds
	TotalWaitTime int       `column:"TOTAL_ACT_WAIT_TIME"` //总等待时间
	TotalExecTime int       `column:"STMT_EXEC_TIME"`      //语句的总执行时间
	RowsRead      int       `column:"ROWS_READ"`
	RowsDelete    int       `column:"ROWS_DELETED"`
	RowsInsert    int       `column:"ROWS_INSERTED"`
	RowsUpdate    int       `column:"ROWS_UPDATED"`
	HashJoins     int       `column:"TOTAL_HASH_JOINS"`
	HashLoops     int       `column:"TOTAL_HASH_LOOPS"`
	HashFlows     int       `column:"HASH_JOIN_OVERFLOWS"`
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
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetActStmt()
		if err := renderStruct(d, line); err != nil {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}
