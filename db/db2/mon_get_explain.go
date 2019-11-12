package db2

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

/*Object Type:
Value	Description
IX	Index
NK	Nickname
RX	RCT Index
DP	Data partitioned table
TA	Table
TF	Table Function
+A	Compiler-referenced Alias
+C	Compiler-referenced Constraint
+F	Compiler-referenced Function
+G	Compiler-referenced Trigger
+N	Compiler-referenced Nickname
+T	Compiler-referenced Table
+V	Compiler-referenced View
XI	Logical XML index
PI	Physical XML index
LI	Partitioned index
LX	Partitioned logical XML index
LP	Partitioned physical XML index
CO	Column-organized table
*/
//获取执行计划信息
type MonGetExplainObj struct {
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	ExplnReq      string    `column:"EXPLAIN_REQUESTER"` //explain的发起者
	ExplnTime     string    `column:"EXPLAIN_TIME"`
	SrcName       string    `column:"SOURCE_NAME"`
	SrcSchema     string    `column:"SOURCE_SCHEMA"`
	ExplnLevel    string    `column:"EXPLAIN_LEVEL"`
	StmtNo        int       `column:"STMTNO"`
	SectionNo     int       `column:"SECTNO"`
	ObjSchema     string    `column:"OBJECT_SCHEMA"`
	ObjName       string    `column:"OBJECT_NAME"`
	ObjType       string    `column:"OBJECT_TYPE"`
	CreatTime     time.Time `column:"CREATE_TIME"`     //对象创建时间，如果是表函数则为null
	StatsTime     time.Time `column:"STATISTICS_TIME"` //统计信息发起时间，如果对象不存在则为null
	ColCount      int       `column:"COLUMN_COUNT"`    //字段个数
	RowCount      int       `column:"ROW_COUNT"`       //统计信息表card值
	TbspName      string    `column:"TABLESPACE_NAME"`
	F1KCard       int       `column:"FIRSTKEYCARD"` //Number of distinct first key values. Set to -1 for a table, table function, or if this statistic is not available.
	F2KCard       int       `column:"FIRST2KEYCARD"`
	F3KCard       int       `column:"FIRST3KEYCARD"`
	FUKCard       int       `column:"FULLKEYCARD"`
	TabReads      int       //ROWS_READ 表被扫描的次数
	TabScans      int       //表扫描次数,只有在ObjType='TA' 即table的时候才会获取
	SRowsModified int       //自动上次统计信息依赖，表的修改记录数,只有在ObjType='TA' 即table的时候才会获取
	DataLPages    int       //表的逻辑page页数，包括表和索引的page页面,只有在ObjType='TA' 即table的时候才会获取

}

//获取SQL的执行计划信息
type MonGetExplain struct {
	SnapTime    time.Time `column:"CURRENT TIMESTAMP"`
	HexId       string    `column:"executable_id"`
	ExplnSchema string    `column:"EXPLAIN_SCHEMA"`
	ExplnReq    string    `column:"explain_requester"`
	ExplnTime   string    `column:"EXPLAIN_TIME"`
	SrcName     string    `column:"SOURCE_NAME"`
	SrcSchema   string    `column:"SOURCE_SCHEMA"`
	SrcVersion  string    `column:"SOURCE_VERSION"`
}

//返回执行计划的结构体和错误信息
func NewMonGetExplain(hexid string) (*MonGetExplain, error) {
	self := new(MonGetExplain)
	argSql := fmt.Sprintf("CALL EXPLAIN_FROM_SECTION(%s,'M',NULL,0,'%s',?,?,?,?,?)", hexid, strings.ToUpper(GetCurInstanceName()))
	cmd := exec.Command("db2", "-x", argSql)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.New(string(bs))
	}
	self.HexId = hexid
	last_line := "" //定义延迟行
	for _, line := range strings.Split(string(bs), "\n") {
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			last_line = line
			continue
		}
		val := strings.TrimSpace(fields[1])
		switch {
		case strings.Contains(last_line, "EXPLAIN_SCHEMA"):
			self.ExplnSchema = val
		case strings.Contains(last_line, "EXPLAIN_REQUESTER"):
			self.ExplnReq = val
		case strings.Contains(last_line, "EXPLAIN_TIME"):
			self.ExplnTime = val
		case strings.Contains(last_line, "SOURCE_NAME"):
			self.SrcName = val
		case strings.Contains(last_line, "SOURCE_SCHEMA"):
			self.SrcSchema = val
		case strings.Contains(last_line, "SOURCE_VERSION"):
			self.SrcVersion = val
			return self, nil
		}
		last_line = line
	}
	return nil, errors.New("call explain sucess but cannot get explain information")
}

//获取执行计划的SQL相关的表以及索引等对象信息
func (m *MonGetExplain) GetObj() ([]*MonGetExplainObj, error) {
	col_str := reflectMonGet(new(MonGetExplainObj))
	argSql := fmt.Sprintf("select %s from EXPLAIN_OBJECT where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	ms := make([]*MonGetExplainObj, 0)
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetExplainObj)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	//修改ms中mon_get_table相关字段属性
	for _, d := range ms {
		//如果是普通表或者分区表，则获取表中信息
		if d.ObjType == "TA" || d.ObjType == "DP" {
			argSqlgetTable := fmt.Sprintf("select sum(TABLE_SCANS) as TABLE_SCANS,sum(ROWS_READ) as ROWS_READ,"+
				"sum(DATA_OBJECT_L_PAGES+INDEX_OBJECT_L_PAGES)as tabpages,"+
				"sum(STATS_ROWS_MODIFIED) as STATS_ROWS_MODIFIED from "+
				"table(MON_GET_TABLE('%s','%s',-1)) as t group by TABSCHEMA,TABNAME,MEMBER with ur",
				d.ObjSchema, d.ObjName)
			cmd := exec.Command("db2", "-x", argSqlgetTable)
			bs, err := cmd.CombinedOutput()
			if err != nil {
				log.Warn(err)
			}
			for _, line := range strings.Split(string(bs), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) != 4 {
					log.Warn(line + " fields Not equal than :" + strconv.Itoa(len(fields)))
					continue
				}
				if scans, err := strconv.Atoi(fields[0]); err == nil {
					d.TabScans = scans
				}
				if reads, err := strconv.Atoi(fields[1]); err == nil {
					d.TabReads = reads
				}
				if pages, err := strconv.Atoi(fields[2]); err == nil {
					d.DataLPages = pages
				}
				if modifieds, err := strconv.Atoi(fields[3]); err == nil {
					d.SRowsModified = modifieds
				}
			}
		}
	}

	return ms, nil
}
