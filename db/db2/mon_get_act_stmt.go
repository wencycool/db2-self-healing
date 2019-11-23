package db2

import (
	"reflect"
	"sort"
	"strings"
	"time"
)

//act中只包含目前正在执行的SQL,排除处于任何等待状态的SQL
type MonGetActStmt struct {
	MataData
	SnapTime        time.Time `column:"CURRENT TIMESTAMP"`
	StartTime       time.Time `column:"LOCAL_START_TIME"`
	TimeSpend       int       `column:"INT((CURRENT TIMESTAMP - LOCAL_START_TIME)*1000) AS TIMESPEND"` //已执行时间毫秒
	AppHandle       int64     `column:"APPLICATION_HANDLE"`
	ActId           int64     `column:"ACTIVITY_ID"`
	UowId           int64     `column:"UOW_ID"`
	ActType         string    `column:"ACTIVITY_TYPE"`
	StmtId          int64     `column:"STMTID"`
	PlanId          int64     `column:"PLANID"`
	HexId           string    `column:"EXECUTABLE_ID"`
	StmtNo          int64     `column:"STMTNO"`
	ActState        string    `column:"ACTIVITY_STATE"`
	NestLevel       int64     `column:"NESTING_LEVEL"`  //记录嵌套层深，值越大说明被调用的层数越深
	ActTime         int       `column:"TOTAL_ACT_TIME"` //总的执行时间milliseconds
	CpuTime         int       `column:"TOTAL_CPU_TIME"`
	ActWTime        int       `column:"TOTAL_ACT_WAIT_TIME"`
	LockWTime       int       `column:"LOCK_WAIT_TIME"`
	LatchTime       int       `column:"TOTAL_EXTENDED_LATCH_WAIT_TIME"`
	RowsRead        int       `column:"ROWS_READ"`
	RowsDelete      int       `column:"ROWS_DELETED"`
	RowsInsert      int       `column:"ROWS_INSERTED"`
	RowsUpdate      int       `column:"ROWS_UPDATED"`
	PoolDLReads     int       `column:"POOL_DATA_L_READS"`
	PoolDPReads     int       `column:"POOL_DATA_P_READS"`
	PoolILReads     int       `column:"POOL_INDEX_L_READS"`
	PoolIPReads     int       `column:"POOL_INDEX_P_READS"`
	PoolTmpDLReads  int       `column:"POOL_TEMP_DATA_L_READS"`
	PoolTmpDPReads  int       `column:"POOL_TEMP_DATA_P_READS"`
	PoolTmpILReads  int       `column:"POOL_INDEX_L_READS"`
	PoolTmpIPReads  int       `column:"POOL_INDEX_P_READS"`
	ActiveHashJoins int       `column:"ACTIVE_HASH_JOINS"`
	ActiveSorts     int       `column:"ACTIVE_SORTS"`
	HashJoins       int       `column:"TOTAL_HASH_JOINS"`
	HashLoops       int       `column:"TOTAL_HASH_LOOPS"`
	HashFlows       int       `column:"HASH_JOIN_OVERFLOWS"`
	AuthId          string    `column:"SESSION_AUTH_ID"` //从agent中获取
	AppId           string    `column:"APPLICATION_ID"`  //从agent中获取
	AgentState      string    `column:"AGENT_STATE"`     // 从agent中获取
	EventType       string    `column:"EVENT_TYPE"`      // 从agent中获取
	EventObj        string    `column:"EVENT_OBJECT"`    // 从agent中获取
	EventState      string    `column:"EVENT_STATE"`     // 从agent中获取
	ReqType         string    `column:"REQUEST_TYPE"`    // 从agent中获取
}

func NewMonGetActStmt() *MonGetActStmt {
	m := new(MonGetActStmt)
	m.rep = mon_get_rep
	m.tabname = "(select act.*,agent.SESSION_AUTH_ID,agent.APPLICATION_ID,agent.AGENT_STATE,agent.EVENT_TYPE,agent.EVENT_OBJECT,agent.EVENT_STATE,agent.REQUEST_TYPE from TABLE(MON_GET_ACTIVITY(NULL, -1)) act left join table(MON_GET_AGENT('','',cast(NULL as bigint), -1)) as agent on act.APPLICATION_HANDLE=agent.APPLICATION_HANDLE and act.UOW_ID=agent.UOW_ID and act.ACTIVITY_ID=agent.ACTIVITY_ID and agent.AGENT_TYPE='COORDINATOR' )"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetActStmt) GetSqlText() string {
	return genSql(m)
}

//通过从数据库返回的结果，生成结果集,处于锁等待或者其他等待状态的不当做正在执行的SQL语句
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
		d.tabname = ""
		d.start_flag = ""
		d.end_flag = ""
		if err := renderStruct(d, line); err != nil {
			continue
		}
		//如果agent空闲状态，即处于等待状态，则当做正在执行的SQL语句
		if d.EventState == "IDLE" {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}

//根据当前的mon_get_activity的palnid发生聚合，将所有int类型指标进行聚合，将所有其它类型指标进行更新
type MonGetActStmtPlanid struct {
	*MonGetActStmt
	RootHexId     string  //该SQL调用者，如果RootHexId等同于HexId则该SQL未被任何调用
	ActCount      int     //一共发生的聚合次数
	ActDataCount  int     //聚合后mon_get_activity表中有指标记录的集合次数，但是TimeSpend会聚合所有的
	AppHandleList []int64 //存放相同Planid的application handle
}

type MonGetActStmtPlanidList []*MonGetActStmtPlanid

func (m MonGetActStmtPlanidList) Len() int {
	return len(m)
}
func (m MonGetActStmtPlanidList) Less(i, j int) bool {
	return m[i].ActCount > m[j].ActCount
}
func (m MonGetActStmtPlanidList) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

//所有id属性的数据类型in32,int64不参与聚合，int类型参与聚合并按照执行次数从大到小排序
func GetMonGetActStmtAggByPlanid(acts []*MonGetActStmt) []*MonGetActStmtPlanid {
	ByPlanidMap := make(map[int64]*MonGetActStmtPlanid)
	HexIdMap := make(map[int64]string, 0) //存放每一个apphandle的nestlevel=0的Hexid
	ByPlanidList := make([]*MonGetActStmtPlanid, 0)
	for _, act := range acts {
		//存放nestlevel=0的HexId
		if _, ok := HexIdMap[act.AppHandle]; !ok {
			if act.NestLevel == 0 {
				HexIdMap[act.AppHandle] = act.HexId
			}
		}
		actObj_value := reflect.ValueOf(act).Elem()
		//如果ActTime大于0，那么指标信息为有效信息
		if _, ok := ByPlanidMap[act.PlanId]; ok {
			ByPlanidMap[act.PlanId].AppHandleList = append(ByPlanidMap[act.PlanId].AppHandleList, act.AppHandle)
			ByPlanidMap[act.PlanId].ActCount++
			obj_type := reflect.TypeOf(ByPlanidMap[act.PlanId].MonGetActStmt).Elem()
			obj_value := reflect.ValueOf(ByPlanidMap[act.PlanId].MonGetActStmt).Elem()
			if actTimeVal := actObj_value.FieldByName("ActTime"); actTimeVal.CanAddr() && actTimeVal.Int() > 0 {
				ByPlanidMap[act.PlanId].ActDataCount++
			}
			numFields := obj_value.NumField()
			for i := 0; i < numFields; i++ {
				obj_tp := obj_type.Field(i).Type.String()
				if obj_tp == "int" {
					//可以进行累加
					tmp_obj_val := obj_value.Field(i).Int()
					fname := obj_type.Field(i).Name
					if actObj_value.FieldByName(fname).CanAddr() && actObj_value.FieldByName(fname).Type().String() == obj_tp {
						obj_value.Field(i).SetInt(tmp_obj_val + actObj_value.FieldByName(fname).Int())
					} else {
						obj_value.Field(i).SetInt(tmp_obj_val + 1)
					}

				}
			}
		} else {
			var hexid string
			var actCnt int
			if v, ok := HexIdMap[act.AppHandle]; ok {
				hexid = v
			} else {
				hexid = act.HexId
			}
			//如果ActTime大于0，那么指标信息为有效信息
			if actTimeVal := actObj_value.FieldByName("ActTime"); actTimeVal.CanAddr() && actTimeVal.Int() > 0 {
				actCnt++
			}
			AppHandleList := make([]int64, 0)
			AppHandleList = append(AppHandleList, act.AppHandle)
			ByPlanidMap[act.PlanId] = &MonGetActStmtPlanid{act, hexid, 1, actCnt, AppHandleList}
		}
	}
	for k, _ := range ByPlanidMap {
		ByPlanidList = append(ByPlanidList, ByPlanidMap[k])
	}
	//对ByPlanidList进行排序
	sort.Sort(MonGetActStmtPlanidList(ByPlanidList))
	return ByPlanidList
}
