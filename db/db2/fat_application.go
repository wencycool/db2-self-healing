package db2

import (
	"os/exec"
	"strconv"
	"strings"
)

//判断agent是否可以进行做force操作，主要包括是否大事务，是否包含reorg等DDL操作
func CurrentAppId() string {
	bs, err := exec.Command("db2", "-x", "values application_id()").CombinedOutput()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(bs))
}

func CurrentAppHandle() int32 {
	bs, err := exec.Command("db2", "-x", "values mon_get_application_handle()").CombinedOutput()
	if err != nil {
		return -1
	}
	r, err := strconv.Atoi(strings.TrimSpace(string(bs)))
	if err != nil {
		return -1
	}
	return int32(r)
}
