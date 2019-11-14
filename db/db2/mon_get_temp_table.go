package db2

import (
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//获取用户和系统临时表信息,TabName需要放在最后因为表中可能包含分隔符
type MonGetTempTable struct {
	SnapTime   time.Time `column:"CURRENT TIMESTAMP"`
	TabType    string    `column:"TAB_TYPE"` //TEMP_TABLE系统临时表,USER_TABLE用户临时表
	TbspId     int32     `column:"TBSP_ID"`
	RowsRead   int       `column:"ROWS_READ"`
	RowsInsert int       `column:"ROWS_INSERTED"`
	RowsUpdate int       `column:"ROWS_UPDATED"`
	RowsDelete int       `column:"ROWS_DELETED"`
	DataLPages int       `column:"DATA_OBJECT_L_PAGES"`
	IdxLPages  int       `column:"INDEX_OBJECT_L_PAGES"`
	AppHandle  int32
	TabSchema  string `column:"TABSCHEMA"`
	TabName    string `column:"TABNAME"`
}

func NewMonGetTempTableList() []*MonGetTempTable {
	patt := regexp.MustCompile(`^<(\d+)><(\w+)>$`)
	m := new(MonGetTempTable)
	ms := make([]*MonGetTempTable, 0)
	sqlArg := fmt.Sprintf("select %s from TABLE(MON_GET_TABLE(NULL,NULL,-1)) AS T "+
		"where tab_type in ('TEMP_TABLE','USER_TABLE') with ur", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		d := new(MonGetTempTable)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		//查看d的schema是否<int><str>形式
		if !patt.MatchString(d.TabSchema) {
			log.Warn("Cannot analysis apphandle from schema,schema:" + d.TabSchema)
			continue
		}
		submatch := patt.FindAllStringSubmatch(d.TabSchema, 1)
		handle, err := strconv.Atoi(submatch[0][1])
		if err != nil {
			log.Warn(err)
			continue
		}
		d.AppHandle = int32(handle)
		d.TabSchema = submatch[0][2]
		ms = append(ms, d)
	}

	return ms
}
