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
	objs        []*MonGetExplainObj
	operators   []*MonGetExplainOperator
	predicates  []*MonGetExplainPredicate
	streams     []*MonGetExplainStream
	planNode    *Node
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
	argSql := fmt.Sprintf("CALL EXPLAIN_FROM_SECTION(%s,'M',NULL,0,NULL,?,?,?,?,?)", hexid)
	cmd := exec.Command("db2", "-x", argSql)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(bs), "SQL0219N") {
			//没有安装explain表，需要进行安装
			log.Warn("没有安装expain表，需要进行安装")
			install_explain_sql := fmt.Sprintf("CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', CAST (NULL AS VARCHAR(128)), '%s')", CurrentSchema())
			cmd := exec.Command("db2", "-x", install_explain_sql)
			log.Warn(fmt.Sprintf("安装explain表语句为:%s", install_explain_sql))
			bs, err := cmd.CombinedOutput()
			if err != nil {
				msg := fmt.Sprintf("无法安装explain表，ddl:%s,msg:%s", install_explain_sql, string(bs))
				log.Error(msg)
				return nil, errors.New(msg)
			} else {
				//已经成功安装explain表需要重新调用该函数
				log.Warn("成功安装explain表")
				return NewMonGetExplain(hexid)
			}
		}
		return nil, errors.New(string(bs))
	}
	self.HexId = hexid
	last_line := "" //定义延迟行
FOR1:
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
			//return self, nil
			break FOR1
		}
		last_line = line
	}
	if self.operators, err = self.getOperator(); err != nil {
		return nil, err
	}
	if self.objs, err = self.getObj(); err != nil {
		return nil, err
	}
	if self.streams, err = self.getStream(); err != nil {
		return nil, err
	}
	if self.predicates, err = self.getPredicate(); err != nil {
		//可能不存在predicates
		return nil, err
	}
	self.planNode = newNode(self.streams)
	return self, nil
}

//获取执行计划的SQL相关的表以及索引等对象信息
/*
func (m *MonGetExplain) GetStream() ([]*MonGetExplainStream, error) {
	return m.getStream()
}
func (m *MonGetExplain) GetOperator() ([]*MonGetExplainOperator, error) {
	return m.getOperator()
}
func (m *MonGetExplain) GetObj() ([]*MonGetExplainObj, error) {
	return m.getObj()
}
func (m *MonGetExplain) GetPredicate() ([]*MonGetExplainPredicate, error) {
	return m.getPredicate()
}
*/
//获取执行计划的operator信息
func (m *MonGetExplain) getOperator() ([]*MonGetExplainOperator, error) {
	col_str := reflectMonGet(new(MonGetExplainOperator))
	argSql := fmt.Sprintf("select %s from EXPLAIN_OPERATOR where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if checkDbErr(err) != nil {
		return nil, errors.New(string(bs))
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
	if checkDbErr(err) != nil {
		return nil, errors.New(string(bs))
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

//获取执行计划涉及到的对象信息
func (m *MonGetExplain) getObj() ([]*MonGetExplainObj, error) {
	col_str := reflectMonGet(new(MonGetExplainObj))
	argSql := fmt.Sprintf("select %s from EXPLAIN_OBJECT where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if checkDbErr(err) != nil {
		return nil, errors.New(string(bs))
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

//获取每一步骤中的操作预测信息
func (m *MonGetExplain) getPredicate() ([]*MonGetExplainPredicate, error) {
	col_str := reflectMonGet(new(MonGetExplainPredicate))
	argSql := fmt.Sprintf("select %s from EXPLAIN_PREDICATE where EXPLAIN_REQUESTER='%s' and EXPLAIN_TIME='%s' with ur",
		col_str, m.ExplnReq, m.ExplnTime)
	cmd := exec.Command("db2", "-x", argSql)
	//找到相关字段以后进行字段解析
	bs, err := cmd.CombinedOutput()
	if checkDbErr(err) != nil {
		return nil, errors.New(string(bs))
	}
	ms := make([]*MonGetExplainPredicate, 0)
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetExplainPredicate)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms, nil
}

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
	StmtNo        int64     `column:"STMTNO"`
	SectionNo     int64     `column:"SECTNO"`
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
	StmtNo     int64     `column:"STMTNO"`
	SectionNo  int64     `column:"SECTNO"`
	OpId       int64     `column:"OPERATOR_ID"` //在一个explain中唯一
	OPType     string    `column:"OPERATOR_TYPE"`
	TotalCost  int       `column:"TOTAL_COST"`
	IoCost     int       `column:"IO_COST"`
	CpuCost    int       `column:"CPU_COST"`
}

type MonGetExplainOperatorList []*MonGetExplainOperator

//根据operatorid返回该operator
func (m MonGetExplainOperatorList) lookupOperatorById(operatorId int64) (*MonGetExplainOperator, bool) {
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
	StmtNo      int64     `column:"STMTNO"`
	SectionNo   int64     `column:"SECTNO"`
	StreamId    int64     `column:"STREAM_ID"`     //每一个执行计划中streamId唯一
	SrcType     string    `column:"SOURCE_TYPE"`   //O:Operator,D:Data Object
	SrcId       int64     `column:"SOURCE_ID"`     //source operator id;-1 if TARGET_TYPE is 'D',可认为是当前Id
	SrcOpType   string    `column:"OPERATOR_TYPE"` //返回源操作的类型
	SrcOpCost   int       `column:"TOTAL_COST"`    //返回源操作的总代价
	TgtType     string    `column:"TARGET_TYPE"`   //O:Operator,D:Data Object
	TgtId       int64     `column:"TARGET_ID"`     //target operator id;;-1 if TARGET_TYPE is 'D',可认为是ParentId
	ObjSchema   string    `column:"OBJECT_SCHEMA"` //Schema to which the affected data object belongs. Set to null if both SOURCE_TYPE and TARGET_TYPE are 'O'
	ObjName     string    `column:"OBJECT_NAME"`   //Name of the object that is the subject of data stream. Set to null if both SOURCE_TYPE and TARGET_TYPE are 'O'
	StreamCount int       `column:"STREAM_COUNT"`  //Estimated cardinality of data stream.
	ColCount    int       `column:"COLUMN_COUNT"`  //Number of columns in data stream.
	//PredicateId 	int64       `column:"PREDICATE_ID"`  //If this stream is part of a subquery for a predicate, the predicate ID will be reflected here, otherwise the column is set to -1.

}

//获取执行计划的predicate信息，一个operator可能有多个predicate
type MonGetExplainPredicate struct {
	SnapTime     time.Time `column:"CURRENT TIMESTAMP"`
	ExplnReq     string    `column:"EXPLAIN_REQUESTER"` //explain的发起者
	ExplnTime    string    `column:"EXPLAIN_TIME"`
	ExplnLevel   string    `column:"EXPLAIN_LEVEL"`
	StmtNo       int64     `column:"STMTNO"`
	SectionNo    int64     `column:"SECTNO"`
	OperatorId   int64     `column:"OPERATOR_ID"`   //Unique ID for this operator within this query
	PredicateId  int64     `column:"PREDICATE_ID"`  //Unique ID for this predicate for the specified operator,A value of "-1" is shown for operator predicates constructed by the Explain tool which are not optimizer objects and do not exist in the optimizer plan.
	HowApplied   string    `column:"HOW_APPLIED"`   // 重要指标,How predicate is being used by the specified operator
	RelopType    string    `column:"RELOP_TYPE"`    //重要指标The type of relational operator used in this predicate
	FilterFactor int       `column:"FILTER_FACTOR"` //The estimated fraction of rows that will be qualified by this predicate.A value of "-1" is shown when FILTER_FACTOR is not applicable. FILTER_FACTOR is not applicable for operator predicates constructed by the Explain tool which are not optimizer objects and do not exist in the optimizer plan.
}

//根据operatorid返回该操作下的所有predicate操作
type MonGetExplainPredicateList []*MonGetExplainPredicate

func (m MonGetExplainPredicateList) lookupPredicatesByOperatorId(operatorId int64) (MonGetExplainPredicateList, bool) {
	result := make([]*MonGetExplainPredicate, 0)
	for _, v := range m {
		if v.OperatorId == operatorId {
			result = append(result, v)
		}
	}
	if len(result) > 0 {
		return result, true
	} else {
		return result, false
	}

}

//检查指定operatorId内的预测中是否存在指定HOW_APPLIED
func (m MonGetExplainPredicateList) hasAppliedByOperatorId(operatorId int64, howApplied string) bool {
	if predicates, ok := m.lookupPredicatesByOperatorId(operatorId); ok {
		for _, v := range predicates {
			if v.HowApplied == howApplied {
				return true
			}
		}
	}
	return false
}

/*
objects,stream,operator,predicate表中explain_level唯一,均为s
When a section explain has generated an explain output, the EXPLAIN_LEVEL column is set to value S. It is important to note that the EXPLAIN_LEVEL column is part of the primary key of the table and part of the foreign key of most other EXPLAIN tables; hence, this EXPLAIN_LEVEL value will also be present in those other tables.

In the EXPLAIN_STATEMENT table, the remaining column values that are usually associated with a row with EXPLAIN_LEVEL = P, are instead present when EXPLAIN_LEVEL = S, with the exception of SNAPSHOT. SNAPSHOT is always NULL when EXPLAIN_LEVEL is S.

If the original statement was not available at the time the section explain was generated (for example, if the statement text was not provided to the EXPLAIN_FROM_DATA procedure), STATEMENT_TEXT is set to the string UNKNOWN when EXPLAIN_LEVEL is set to O.

*/
