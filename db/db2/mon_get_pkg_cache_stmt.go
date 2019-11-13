package db2

import (
	"fmt"
	"os/exec"
	"time"
)

//注意最后一位一定要是stmtText这种可能存在空格符号的文本
type MonGetPkgCacheStmt struct {
	MataData
	SnapTime    time.Time `column:"CURRENT TIMESTAMP"`
	SectionType string    `column:"SECTION_TYPE"`
	InsertTime  time.Time `column:"INSERT_TIMESTAMP"`
	PkgSchema   string    `column:"PACKAGE_SCHEMA"`
	PkgName     string    `column:"PACKAGE_NAME"`
	Section     int32     `column:"SECTION_NUMBER"`
	Executions  int       `column:"NUM_EXEC_WITH_METRICS"`
	ActTime     int       `column:"TOTAL_ACT_TIME"`
	WaitTime    int       `column:"TOTAL_ACT_WAIT_TIME"`
	CpuTime     int       `column:"TOTAL_CPU_TIME"`
	LockWTime   int       `column:"LOCK_WAIT_TIME"`
	LatchWTime  int       `column:"TOTAL_EXTENDED_LATCH_WAIT_TIME"`
	HexId       string    `column:"EXECUTABLE_ID"`
	StmtText    string    `column:"replace(STMT_TEXT,char(10)||char(13),' ') as STMT_TEXT"`
}

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
