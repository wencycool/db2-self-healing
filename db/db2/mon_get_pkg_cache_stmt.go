package db2

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

//注意最后一位一定要是stmtText这种可能存在空格符号的文本
type MonGetPkgCacheStmt struct {
	MataData
	SnapTime     time.Time `column:"CURRENT TIMESTAMP"`
	SectionType  string    `column:"SECTION_TYPE"`
	InsertTime   time.Time `column:"INSERT_TIMESTAMP"`
	PkgSchema    string    `column:"PACKAGE_SCHEMA"`
	PkgName      string    `column:"PACKAGE_NAME"`
	Section      int64     `column:"SECTION_NUMBER"`
	Executions   int       `column:"NUM_EXEC_WITH_METRICS"`
	ActTime      int       `column:"TOTAL_ACT_TIME"`
	WaitTime     int       `column:"TOTAL_ACT_WAIT_TIME"`
	CpuTime      int       `column:"TOTAL_CPU_TIME"`
	LockWTime    int       `column:"LOCK_WAIT_TIME"`
	LatchWTime   int       `column:"TOTAL_EXTENDED_LATCH_WAIT_TIME"`
	RowsReturned int       `column:"ROWS_RETURNED"`
	RowsRead     int       `column:"ROWS_READ"`
	HexId        string    `column:"EXECUTABLE_ID"`
	PlanId       int64     `column:"PLANID"`
	StmtText     string    `column:"replace(STMT_TEXT,char(10)||char(13),' ') as STMT_TEXT"`
}

//当执行计划发生改变时,HexId是否会改变?
//根据HexId获取MonGetPkgCacheStmt
func NewMonGetPkgCacheStmt(HexId string) *MonGetPkgCacheStmt {
	m := new(MonGetPkgCacheStmt)
	if HexId == "" {
		return m
	}
	sqlArgs := fmt.Sprintf("select %s from table(MON_GET_PKG_CACHE_STMT(NULL,%s,null,-1)) as t with ur", reflectMonGet(m), HexId)
	log.Tracef("获取pkgcache语句为:%s\n", sqlArgs)
	cmd := exec.Command("db2", "-x", sqlArgs)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	err = renderStruct(m, string(bs))
	if err != nil {
		log.Warn(err)
	}
	return m
}

//从数据库内存中按照palnid获取条目,有待改进！
//Use this monitor element with the stmtid and semantic_env_id monitor elements to detect changes in access plan that might affect performance.
func NewMonGetPkgCacheStmtByPlanId(planid int64) *MonGetPkgCacheStmt {
	m := new(MonGetPkgCacheStmt)
	sqlArgs := fmt.Sprintf("select %s from table(MON_GET_PKG_CACHE_STMT(NULL,null,null,-1)) as t  where planid=%d with ur", reflectMonGet(m), planid)
	cmd := exec.Command("db2", "-x", sqlArgs)
	fmt.Println(sqlArgs)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	err = renderStruct(m, strings.Split(string(bs), "\n")[0])
	if err != nil {
		log.Warn(err)
	}
	return m
}
