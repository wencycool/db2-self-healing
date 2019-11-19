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

//按照AppHandle和TbspId进行聚合,主要解决临时表空间占用的问题
type TempSpaceInfo struct {
	AppHandle  int32
	TbspId     int32
	RowsRead   int
	RowsInsert int
	RowsUpdate int
	RowsDelete int
	DataLPages int
	IdxLPages  int
}

//以TbspId和AppHandle为单位进行聚合计算空间使用大小
func GetTempSpaceInfoAggByTbspIdAndAppHandle(ts []*MonGetTempTable) []*TempSpaceInfo {
	tempSpaceInfoList := make([]*TempSpaceInfo, 0)
	ts_map := make(map[[2]int32]*TempSpaceInfo, 0)
	for _, t := range ts {
		key := [2]int32{t.TbspId, t.AppHandle}
		if _, ok := ts_map[key]; ok {
			//所有int数据进行累加
			ts_map[key].RowsRead = ts_map[key].RowsRead + t.RowsRead
			ts_map[key].RowsInsert = ts_map[key].RowsInsert + t.RowsInsert
			ts_map[key].RowsUpdate = ts_map[key].RowsUpdate + t.RowsUpdate
			ts_map[key].RowsDelete = ts_map[key].RowsDelete + t.RowsDelete
			ts_map[key].DataLPages = ts_map[key].DataLPages + t.DataLPages
			ts_map[key].IdxLPages = ts_map[key].IdxLPages + t.IdxLPages

		} else {
			ts_map[key] = &TempSpaceInfo{
				AppHandle:  t.AppHandle,
				TbspId:     t.TbspId,
				RowsRead:   t.RowsRead,
				RowsInsert: t.RowsInsert,
				RowsUpdate: t.RowsUpdate,
				RowsDelete: t.RowsDelete,
				DataLPages: t.DataLPages,
				IdxLPages:  t.IdxLPages,
			}
		}
	}
	for k, _ := range ts_map {
		tempSpaceInfoList = append(tempSpaceInfoList, ts_map[k])
	}
	return tempSpaceInfoList
}

//获取指定表空间上AppHandle占用的临时表空间大小和总占用页面数,按照使用率的降序排列
func GetTempSpaceInfoListByTbspId(ts []*TempSpaceInfo, id int32) ([]*TempSpaceInfo, int) {
	tempSpaceInfoList := make([]*TempSpaceInfo, 0)
	sumTotalPages := 0
	for _, t := range ts {
		if t.TbspId == id {
			tempSpaceInfoList = append(tempSpaceInfoList, t)
			sumTotalPages = sumTotalPages + t.DataLPages + t.IdxLPages
		}
	}
	return tempSpaceInfoList, sumTotalPages
}
