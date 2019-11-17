package db2

import (
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

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
					d.RDataLPages = pages
				}
				if modifieds, err := strconv.Atoi(fields[3]); err == nil {
					d.SRowsModified = modifieds
				}
			}
		}
	}

	return ms, nil
}

//获取执行计划的operator信息
func (m *MonGetExplain) getOperator() ([]*MonGetExplainOperator, error) {
	col_str := reflectMonGet(new(MonGetExplainOperator))
	argSql := fmt.Sprintf("select %s from EXPLAIN_OPERATOR where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	ms := make([]*MonGetExplainOperator, 0)
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetExplainOperator)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms, nil
}

//获取执行计划的Stream信息
func (m *MonGetExplain) getStream() ([]*MonGetExplainStream, error) {
	col_str := reflectMonGet(new(MonGetExplainStream))
	//必须对stream_id进行排序，保持左右节点顺序
	argSql := fmt.Sprintf("select %s from (select a.*,b.OPERATOR_TYPE,b.TOTAL_COST from EXPLAIN_STREAM a left join EXPLAIN_OPERATOR  b on a.EXPLAIN_REQUESTER=b.EXPLAIN_REQUESTER and a.EXPLAIN_TIME=b.EXPLAIN_TIME and a.SOURCE_ID=b.OPERATOR_ID) where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' order by stream_id asc with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	ms := make([]*MonGetExplainStream, 0)
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetExplainStream)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms, nil
}

func (m *MonGetExplain) GetOperator() ([]*MonGetExplainOperator, error) {
	return m.getOperator()
}
func (m *MonGetExplain) GetStream() ([]*MonGetExplainStream, error) {
	return m.getStream()
}

func (m *MonGetExplain) hasOperaType(opType string) bool {
	if ops, err := m.getOperator(); err == nil {
		for _, op := range ops {
			if op.OPType == opType {
				return true
			}
		}
	}
	return false
}

//查看执行计划中是否存在HashJoin信息，当并发执行较多且有hashJoin的情况下并不是良好现象
//在判断长时间执行SQL的时候如果发生大量的rows_read 是两个表数据量的N倍以上，往往是由于没有走hashjoin导致，
// 因此也可以通过此粗略短判断执行计划是否存在问题
func (m *MonGetExplain) HasHashJoin() bool {
	return m.hasOperaType("HSJOIN")
}

//检查执行计划是否存在IXAND操作当并发执行较多且有IXAND的情况下并不是良好的现象
//尤其IXAND操作发生在执行计划JOIN操作右侧的时候，作为内表数据会有极大效率问题，需要尝试添加索引来进行解决
func (m *MonGetExplain) HasIxand() bool {
	return m.hasOperaType("IXAND")
}

//当不存在任何Join即单表操作且有where条件的时候要有索引
//当存在join，那么NLJOIN的右侧必须包含索引操作

//执行计划
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
//获取执行计划对象信息
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
	RDataLPages   int       //表的真实的逻辑page页数从mon_get_table中获取，包括表和索引的page页面,只有在ObjType='TA' 即table的时候才会获取

}

/*
Operator Type:
Value	Description
DELETE	Delete
EISCAN	Extended Index Scan
FETCH	Fetch
FILTER	Filter rows
GENROW	Generate Row
GRPBY	Group By
HSJOIN	Hash Join
INSERT	Insert
IXAND	Dynamic Bitmap Index ANDing
IXSCAN	Relational index scan
MSJOIN	Merge Scan Join
NLJOIN	Nested loop Join
REBAL	Rebalance rows between SMP subagents
RETURN	Result
RIDSCN	Row Identifier (RID) Scan
RPD	Remote PushDown
SHIP	Ship query to remote system
SORT	Sort
TBFUNC	In-stream table function operator
TBSCAN	Table Scan
TEMP	Temporary Table Construction
TQ	Table Queue
UNION	Union
UNIQUE	Duplicate Elimination
UPDATE	Update
XISCAN	Index scan over XML data
XSCAN	XML document navigation scan
XANDOR	Index ANDing and ORing over XML data
ZZJOIN	Zigzag join
*/
//获取执行计划operator信息
type MonGetExplainOperator struct {
	SnapTime   time.Time `column:"CURRENT TIMESTAMP"`
	ExplnReq   string    `column:"EXPLAIN_REQUESTER"` //explain的发起者
	ExplnTime  string    `column:"EXPLAIN_TIME"`
	SrcName    string    `column:"SOURCE_NAME"`
	SrcSchema  string    `column:"SOURCE_SCHEMA"`
	ExplnLevel string    `column:"EXPLAIN_LEVEL"`
	StmtNo     int       `column:"STMTNO"`
	SectionNo  int       `column:"SECTNO"`
	OpId       int32     `column:"OPERATOR_ID"` //在一个explain中唯一
	OPType     string    `column:"OPERATOR_TYPE"`
	TotalCost  int       `column:"TOTAL_COST"`
	IoCost     int       `column:"IO_COST"`
	CpuCost    int       `column:"CPU_COST"`
}

type MonGetExplainOperatorList []*MonGetExplainOperator

//根据operatorid返回该operator
func (m MonGetExplainOperatorList) LookupOperatorById(operatorId int32) (*MonGetExplainOperator, bool) {
	for i, _ := range m {
		if m[i].OpId == operatorId && operatorId != -1 {
			return m[i], true
		}
	}
	return new(MonGetExplainOperator), false
}

//获取执行计划的stream信息
//stream是explain的树结构，作为中间枢纽和其它操作相关联
type MonGetExplainStream struct {
	SnapTime    time.Time `column:"CURRENT TIMESTAMP"`
	ExplnReq    string    `column:"EXPLAIN_REQUESTER"` //explain的发起者
	ExplnTime   string    `column:"EXPLAIN_TIME"`
	ExplnLevel  string    `column:"EXPLAIN_LEVEL"`
	StmtNo      int       `column:"STMTNO"`
	SectionNo   int       `column:"SECTNO"`
	StreamId    int       `column:"STREAM_ID"`     //每一个执行计划中streamId唯一
	SrcType     string    `column:"SOURCE_TYPE"`   //O:Operator,D:Data Object
	SrcId       int       `column:"SOURCE_ID"`     //-1 if TARGET_TYPE is 'D',可认为是当前Id
	SrcOpType   string    `column:"OPERATOR_TYPE"` //返回源操作的类型
	SrcOpCost   int       `column:"TOTAL_COST"`    //返回源操作的总代价
	TgtType     string    `column:"TARGET_TYPE"`   //O:Operator,D:Data Object
	TgtId       int       `column:"TARGET_ID"`     //-1 if TARGET_TYPE is 'D',可认为是ParentId
	ObjSchema   string    `column:"OBJECT_SCHEMA"` //Schema to which the affected data object belongs. Set to null if both SOURCE_TYPE and TARGET_TYPE are 'O'
	ObjName     string    `column:"OBJECT_NAME"`   //Name of the object that is the subject of data stream. Set to null if both SOURCE_TYPE and TARGET_TYPE are 'O'
	StreamCount int       `column:"STREAM_COUNT"`  //Estimated cardinality of data stream.
	ColCount    int       `column:"COLUMN_COUNT"`  //Number of columns in data stream.
	PredicateId int       `column:"PREDICATE_ID"`  //If this stream is part of a subquery for a predicate, the predicate ID will be reflected here, otherwise the column is set to -1.

}
