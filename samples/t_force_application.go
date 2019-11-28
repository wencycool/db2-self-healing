package main

import (
	"flag"
	"fmt"
	"my/db/db2"
	"strconv"
	"strings"
	"time"
)

func main() {
	db2.ConnectDB("sample")
	acts, uows, _, _, _, utils, _ := db2.CollectPerfData(time.Duration(time.Second * 10))
	fs := db2.NewFatUowApplications(uows, db2.GetMonGetActStmtMaxLevelList(acts), utils)
	for _, result := range fs.ForceAppByLikeStmt("reorg", db2.FORCE_APP_LEVEL_NODDL, db2.FORCE_APP_TYPE_DO) {
		fmt.Println(result.AppHandle, result.IsForced, result.Msg)
	}
	//按照APP进行杀掉
	var idList string
	flag.StringVar(&idList, "ids", "", "需要杀掉的handle列表,如果多个按照逗号做分隔符")
	flag.Parse()
	if len(strings.TrimSpace(idList)) == 0 {
		fmt.Println("No handle list")
	}
	ids := make([]int64, 0)
	for _, handleId := range strings.Split(strings.TrimSpace(idList), ",") {
		if v, err := strconv.Atoi(handleId); err != nil {
			panic("给定handle列表中存在不合法Id，无法解析为数字类型")
		} else {
			ids = append(ids, int64(v))
		}
	}
	for _, msg := range fs.ForceAppByHandle(ids, db2.FORCE_APP_LEVEL_ACTNOTWMDL) {
		fmt.Println(msg.AppHandle, msg.IsForced, msg.Msg)
	}
}
