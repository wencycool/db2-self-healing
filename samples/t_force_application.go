package main

import (
	"fmt"
	"my/db/db2"
	"time"
)

func main() {
	db2.ConnectDB("sample")
	acts, uows, _, _, _, utils, _ := db2.CollectPerfData(time.Duration(time.Second * 10))
	fs := db2.NewFatUowApplications(uows, db2.GetMonGetActStmtMaxLevelList(acts), utils)
	for _, result := range fs.ForceAppByLikeStmt("reorg", db2.FORCE_APP_LEVEL_NODDL, db2.FORCE_APP_TYPE_DO) {
		fmt.Println(result.AppHandle, result.IsForced, result.Msg)
	}
	for _, result := range fs.ForceAppByLikeStmt("select", db2.FORCE_APP_LEVEL_ACTNOTWMDL, db2.FORCE_APP_TYPE_SEE) {
		fmt.Println(result.AppHandle, result.IsForced, result.Msg, db2.ByteSizeFormat(result.Uow.UowLogSpaceUsed))
	}
}
