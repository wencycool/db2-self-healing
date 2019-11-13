package db2

import (
	"reflect"
	"sort"
	"strings"
	"time"
)

type MonGetActStmt struct {
	MataData
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle     int32     `column:"APPLICATION_HANDLE"`
	ActId         int32     `column:"ACTIVITY_ID"`
	UowId         int32     `column:"UOW_ID"`
	ActType       string    `column:"ACTIVITY_TYPE"`
	StmtId        int32     `column:"STMTID"`
	PlanId        int32     `column:"PLANID"`
	HexId         string    `column:"EXECUTABLE_ID"`
	StmtNo        int32     `column:"STMTNO"`
	CpuTime       int       `column:"TOTAL_CPU_TIME"`
	ActState      string    `column:"ACTIVITY_STATE"`
	NestLevel     int32     `column:"NESTING_LEVEL"` //记录嵌套层深，值越大说明被调用的层数越深
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

//根据当前的mon_get_activity的palnid发生聚合，将所有int类型指标进行聚合，将所有其它类型指标进行更新
type MonGetActStmtPlanid struct {
	MonGetActStmt
	RootHexId string //该SQL调用者，如果RootHexId等同于HexId则该SQL未被任何调用
	ActCount  int
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
	ByPlanidMap := make(map[int32]*MonGetActStmtPlanid)
	HexIdMap := make(map[int32]string, 0) //存放每一个apphandle的nestlevel=0的Hexid
	ByPlanidList := make([]*MonGetActStmtPlanid, 0)
	for _, act := range acts {
		//存放nestlevel=0的HexId
		if _, ok := HexIdMap[act.AppHandle]; !ok {
			if act.NestLevel == 0 {
				HexIdMap[act.AppHandle] = act.HexId
			}
		}
		if _, ok := ByPlanidMap[act.PlanId]; ok {
			obj_type := reflect.TypeOf(ByPlanidMap[act.PlanId]).Elem()
			obj_value := reflect.ValueOf(ByPlanidMap[act.PlanId]).Elem()
			actObj_value := reflect.ValueOf(act).Elem()
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
			if v, ok := HexIdMap[act.AppHandle]; ok {
				hexid = v
			} else {
				hexid = act.HexId
			}
			ByPlanidMap[act.PlanId] = &MonGetActStmtPlanid{*act, hexid, 1}
		}
	}
	for k, _ := range ByPlanidMap {
		ByPlanidList = append(ByPlanidList, ByPlanidMap[k])
	}
	//对ByPlanidList进行排序
	sort.Sort(MonGetActStmtPlanidList(ByPlanidList))
	return ByPlanidList
}
