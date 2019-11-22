package main

import (
	"fmt"
	"my/db/db2"
	"time"
)

func main() {
	db2.ConnectDB("sample")
	acts, _, _, _, _, _, _ := db2.CollectPerfData(time.Duration(time.Second * 10))
	acts_f := db2.GetMonGetActStmtAggByPlanid(acts)
	for _, act := range acts_f {
		fmt.Println(db2.NewMonGetPkgCacheStmt(act.HexId).StmtText)
		expln, err := db2.NewMonGetExplain(act.HexId)
		if err != nil {
			panic(err)
		}
		dlist, err := expln.GetStream()
		if err != nil {
			panic(err)
		}
		pkg_stmt := db2.NewMonGetPkgCacheStmt(act.HexId)
		fmt.Println("预计数据读取量:", db2.NewNode(dlist).PredicateRowsScan(), pkg_stmt.RowsRead/pkg_stmt.Executions)
	}
}
