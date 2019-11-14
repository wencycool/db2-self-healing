package db2

import (
	"strings"
	"time"
)

//获取当前正在执行的uow事务，只包含未做提交的所有事务并且包含相关agent信息,对于reorg runstats等作业，agent无UOWID 但可以根据Apphandle关联出相关语句
type MonGetCurUowExtend struct {
	MataData
	SnapTime         time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle        int32     `column:"APPLICATION_HANDLE"`
	AppId            string    `column:"APPLICATION_ID"`
	UowId            int32     `column:"UOW_ID"`
	ClientHostName   string    `column:"CLIENT_HOSTNAME"` //连接进来的客户端主机名
	UowStartTime     time.Time `column:"UOW_START_TIME"`
	AuthId           string    `column:"SESSION_AUTH_ID"` //执行用户ID
	DDLSqlStmts      int       `column:"DDL_SQL_STMTS"`
	NumLocksHeld     int       `column:"NUM_LOCKS_HELD"`
	SinceLastCmtSqls int       `column:"SQL_REQS_SINCE_COMMIT"`
	UowLogSpaceUsed  int       `column:"UOW_LOG_SPACE_USED"`
	LastHexId        string    `column:"LAST_EXECUTABLE_ID"`
	TotalRunstats    int       `column:"TOTAL_RUNSTATS"`
	TotalReorgs      int       `column:"TOTAL_REORGS"`
	TotalLoads       int       `column:"TOTAL_LOADS"`
	RowsRead         int       `column:"ROWS_READ"`
	RowsReturned     int       `column:"ROWS_RETURNED"`
	RowsModified     int       `column:"ROWS_MODIFIED"`
	RowsDelete       int       `column:"ROWS_DELETED"`
	RowsInsert       int       `column:"ROWS_INSERTED"`
	RowsUpdate       int       `column:"ROWS_UPDATED"`
	SelectStmts      int       `column:"SELECT_SQL_STMTS"`
	UIDStmts         int       `column:"UID_SQL_STMTS"`
	DDLStmts         int       `column:"DDL_SQL_STMTS"`
	AgentState       string    `column:"AGENT_STATE"`           // 从agent中获取
	EventType        string    `column:"EVENT_TYPE"`            // 从agent中获取
	EventObj         string    `column:"EVENT_OBJECT"`          // 从agent中获取
	EventState       string    `column:"EVENT_STATE"`           // 从agent中获取
	ReqType          string    `column:"REQUEST_TYPE"`          // 从agent中获取
	ActId            int32     `column:"ACTIVITY_ID"`           //从agent中获取  //当前正在执行的语句的actid，假如为存储过程，那么该agent是最内层正在执行的SQL的agentid
	NestLevel        int       `column:"NESTING_LEVEL"`         // 从agent中获取
	UtilInvId        string    `column:"UTILITY_INVOCATION_ID"` // 从agent中获取
	HexId            string    `column:"EXECUTABLE_ID"`         // 从agent中获取,当前actid对应的HexId ，如果是reorg，runstats等运维作业，则为空
}

//unit_of_work表和agent表相结合
func NewMonGetCurUowExtend() *MonGetCurUowExtend {
	m := new(MonGetCurUowExtend)
	m.rep = mon_get_rep
	//m.tabname = "table(MON_GET_UNIT_OF_WORK(null,-1)) as t where t.UOW_STOP_TIME is null"
	m.tabname = "(select uow.*,agent.AGENT_STATE,agent.EVENT_TYPE,agent.EVENT_OBJECT,agent.EVENT_STATE,agent.REQUEST_TYPE,agent.ACTIVITY_ID,agent.NESTING_LEVEL,agent.EXECUTABLE_ID,agent.UTILITY_INVOCATION_ID from table(MON_GET_UNIT_OF_WORK(null,-1)) as uow left join table(MON_GET_AGENT('','',cast(NULL as bigint), -1)) agent on uow.APPLICATION_HANDLE=agent.APPLICATION_HANDLE  and uow.UOW_STOP_TIME is null and agent.AGENT_TYPE='COORDINATOR' and uow.APPLICATION_ID != application_id())"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetCurUowExtend) GetSqlText() string {
	return genSql(m)
}

//通过从数据库返回的结果，生成结果集
func GetMonGetCurUowExtendList(str string) []*MonGetCurUowExtend {
	m := NewMonGetCurUowExtend()
	ms := make([]*MonGetCurUowExtend, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetCurUowExtend()
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
