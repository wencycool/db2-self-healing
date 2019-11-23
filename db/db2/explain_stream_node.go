package db2

import (
	"fmt"
	"math"
	"strings"
)

//解析explain_stream,数据并构建多叉树
//explain_stream列表数据会得到唯一多叉树

type StreamList []*MonGetExplainStream

func (ds StreamList) findSrcId(tgtId int64) bool {
	for _, d := range ds {
		if d.SrcId == tgtId {
			return true
		}
	}
	return false
}

//将dlist中数据存放到Node中
func newNode(dlist StreamList) *Node {
	//查找root节点,TgtId不是其它节点SrcId值的时候则为root节点
	LfPr := make([][3]int64, 0) //保留未被添加的节点
	root := new(Node)
	for i, d := range dlist {
		if !dlist.findSrcId(d.TgtId) {
			//不可以有多个root节点
			root.ParentId = math.MaxInt16
			root.Id = d.TgtId
			root.Stream = dlist[i]
			root.Level = 0
		}
		LfPr = append(LfPr, [3]int64{int64(i), d.SrcId, d.TgtId})
	}
	//开始进行节点添加，如果节点被添加后则踢出,最大尝试1万次循环
	nbr := 0
	for len(LfPr) > 0 {
		for i, v := range LfPr {
			if nbr > 10000 {
				return root //超过1万次遍历则退出
			}
			nbr++
			if parentNode, ok := root.searchParendNode(v[2]); ok {
				d := dlist[v[0]]
				parentNode.NextList = append(parentNode.NextList, &Node{d, d.SrcId, d.TgtId, parentNode.Level + 1, make([]*Node, 0)})
				LfPr = append(LfPr[:i], LfPr[i+1:len(LfPr)]...)
				break
			}
		}
	}

	return root
}

type Node struct {
	Stream   *MonGetExplainStream //当前数据
	Id       int64                //SrcId 当前节点
	ParentId int64                //父节点
	Level    int                  //当前节点的层级
	NextList []*Node              //孩子节点
}

func (n *Node) add(nd *Node) bool {
	if ParentNode, ok := n.searchParendNode(nd.ParentId); ok {
		ParentNode.NextList = append(ParentNode.NextList, nd)
		return true
	}
	return false
}

//查找是否存在指定SrcId,如果找到则返回该Node节点指针，DFS深度优先
func (n *Node) searchParendNode(ParentId int64) (*Node, bool) {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Id == ParentId {
			return nd, true
		}
		for _, c := range nd.NextList {
			stack.push(c)
		}
	}
	return nil, false
}

type Stack struct {
	nodeList []*Node
}

func (s *Stack) pop() *Node {
	p := s.nodeList[len(s.nodeList)-1]
	s.nodeList = s.nodeList[:len(s.nodeList)-1]
	return p
}

func (s *Stack) push(node *Node) {
	s.nodeList = append(s.nodeList, node)
}

func (s *Stack) isEmpty() bool {
	return len(s.nodeList) == 0
}

func (s *Stack) len() int {
	return len(s.nodeList)
}

//打印root节点所有数据
func (n *Node) PrintData() {
	rep := "    "
	if n.ParentId != math.MaxInt16 {
		fmt.Printf("%s", strings.Repeat(rep, n.Level))
		if n.Id == -1 {
			fmt.Printf("Id:%-3dOpType:%-10sObject:%-10s\n", n.Id, n.Stream.SrcType, n.Stream.ObjSchema+"."+n.Stream.ObjName)
		} else {
			fmt.Printf("Id:%-3dOpType:%-10sCost:%-10d\n", n.Id, n.Stream.SrcOpType, n.Stream.SrcOpCost)
		}

	}
	if len(n.NextList) == 0 {
		return
	}
	for _, v := range n.NextList {
		fmt.Printf("%s", "  ")
		v.PrintData()
	}
}

//对于任何一个节点,如果该节点存在子节点，且存在两个子节点，则打印右侧节点
func (n *Node) printRightNode() {
	if len(n.NextList) == 0 {
		return
	}
	if len(n.NextList) == 2 {
		fmt.Println(n.NextList[1].Id)
	}
	for _, n1 := range n.NextList {
		n1.printRightNode()
	}
}

func (n *Node) hasNLJoin() bool {
	return n.hasOperatorType("NLJOIN")
}
func (n *Node) hasIXAnd() bool {
	return n.hasOperatorType("IXAND")
}

func (n *Node) hasIXScan() bool {
	return n.hasOperatorType("IXSCAN")
}
func (n *Node) hasRIDScan() bool {
	return n.hasOperatorType("RIDSCN")
}
func (n *Node) numberAllJoins() int {
	return n.numberJoins("ALL")
}

//计算有多少个Join节点,当opType节点名为ALL的时候返回所有Join节点
func (n *Node) numberJoins(opType string) int {
	cnt := 0
	stack := new(Stack)
	stack.push(n)
	if !stack.isEmpty() {
		nd := stack.pop()
		//如果nd有且只有两个节点，那么nd为JOIN节点
		if len(nd.NextList) == 2 {
			if strings.ToUpper(opType) == "ALL" || nd.Stream.SrcOpType == opType {
				cnt++
			}
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return cnt
}

//遍历树查看是否含有指定操作
func (n *Node) hasOperatorType(opType string) bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == opType {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return false
}

//遍历左子树查看是否包含指定操作,当某节点只有一个孩子的时候要继续遍历这个孩子
func (n *Node) hasLeftOperatorType(opType string) bool {
	if len(n.NextList) > 0 && n.NextList[0].Stream.SrcOpType == opType {
		return true //当存在某节点的左子树的值为指定值时返回true
	}
	for _, v := range n.NextList {
		if v.hasLeftOperatorType(opType) {
			return true
		}
	}
	return false
}

//----------------------------------------高并发下常见错误执行计划分析-------------------------------------//
/*
select a.CHAR_COL8,b.VARCHAR_COL25 from ttt a,t1 b where a.VARCHAR_COL25=b.VARCHAR_COL25 and a.VARCHAR_COL26=b.VARCHAR_COL26 and b.VARCHAR_COL27='sadf' and b.VARCHAR_COL28='cfd' with ur
/
<OPTGUIDELINES>
  <HSJOIN>
    <TBSCAN  TABLE='a' FIRST='TRUE'/>
    <IXAND TABLE='b'>
      <INDEX IXNAME='T1_IDX3'/>
      <INDEX IXNAME='T1_IDX4'/>
    </IXAND>
  </HSJOIN>
</OPTGUIDELINES>
/
Access Plan:
-----------
        Total Cost:             225690
        Query Degree:           1

                 Rows
                RETURN
                (   1)
                 Cost
                  I/O
                  |
              1.20779e-10
                HSJOIN
                (   2)
                225690
                194087
         /--------+--------\
     581853              6.53552e-05
     TBSCAN                FETCH
     (   3)                (   4)
     225522                14.1078
     194085                2.00007
       |                /----+-----\
     581853       6.53552e-05       15301
 TABLE: DB2INST1    RIDSCN     TABLE: DB2INST1
       TTT          (   5)           T1
       Q2           14.1072          Q1
                       2
                      |
                  6.53552e-05
                    SORT
                    (   6)
                    14.107
                       2
                      |
                  6.53552e-05
                    IXAND
                    (   7)
                    14.1067
                       2
                /-----+------\
               1                1
            IXSCAN           IXSCAN
            (   8)           (   9)
            7.05392          7.05268
               1                1
              |                |
             15301            15301
        INDEX: DB2INST1  INDEX: DB2INST1
            T1_IDX3          T1_IDX4
              Q1               Q1


*/
//对于高并发短事务查询不应该存在hashJoin操作，hashJoin是比较消耗资源的情况，即判断执行计划树中是否存在hashJoin操作
func (n *Node) hasHSJoin() bool {
	return n.hasOperatorType("HSJOIN")
}

//遍历右子树查看是否包含指定操作,当某节点只有一个孩子的时候要继续遍历这个孩子
func (n *Node) hasRightOperatorType(opType string) bool {
	if len(n.NextList) > 0 {
		for _, v := range n.NextList[1:] {
			if v.Stream.SrcOpType == opType {
				return true
			} else {
				if v.hasRightOperatorType(opType) {
					return true
				}
			}
		}
	}
	return false
}

/*
select a.CHAR_COL8,b.VARCHAR_COL25 from ttt a,t1 b where a.VARCHAR_COL25=b.VARCHAR_COL25 and a.VARCHAR_COL26=b.VARCHAR_COL26 and b.VARCHAR_COL27='sadf' and b.VARCHAR_COL28='suiji' with ur
/
<OPTGUIDELINES>
  <NLJOIN>
    <TBSCAN  TABLE='a' FIRST='TRUE'/>
    <IXAND TABLE='b'>
      <INDEX IXNAME='T1_IDX3'/>
      <INDEX IXNAME='T1_IDX4'/>
    </IXAND>
  </NLJOIN>
</OPTGUIDELINES>
/
Access Plan:
-----------
        Total Cost:             229019
        Query Degree:           1

                 Rows
                RETURN
                (   1)
                 Cost
                  I/O
                  |
              1.20779e-10
                NLJOIN
                (   2)
                229019
                194121
         /--------+--------\
     581853              2.07577e-16
     TBSCAN                FETCH
     (   3)                (   4)
     225522                14.1078
     194085                2.00007
       |                /----+-----\
     581853       6.53552e-05       15301
 TABLE: DB2INST1    RIDSCN     TABLE: DB2INST1
       TTT          (   5)           T1
       Q2           14.1072          Q1
                       2
                      |
                  6.53552e-05
                    SORT
                    (   6)
                    14.107
                       2
                      |
                  6.53552e-05
                    IXAND
                    (   7)
                    14.1067
                       2
                /-----+------\
               1                1
            IXSCAN           IXSCAN
            (   8)           (   9)
            7.05392          7.05268
               1                1
              |                |
             15301            15301
        INDEX: DB2INST1  INDEX: DB2INST1
            T1_IDX3          T1_IDX4
              Q1               Q1

*/
//当NLJoin右侧出现IXAND操作的时候会出现严重的latch竞争索引的hash bucket lopp confict(注意：必须是NLJoin的右侧)
//对于高并发操作，NLJOIN的右侧子operator为FETCH的左子树不应该出现IXAND操作,DFS深度搜索遍历子节点
func (n *Node) hasRightOperatorIXAnd() bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == "NLJOIN" && len(nd.NextList) == 2 && nd.NextList[1].Stream.SrcOpType == "FETCH" && len(nd.NextList[1].NextList) == 2 && nd.NextList[1].NextList[0].hasIXAnd() {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return false
}

/*
select a.CHAR_COL8,b.VARCHAR_COL25 from ttt a,t1 b where a.VARCHAR_COL25=b.VARCHAR_COL25 and a.VARCHAR_COL26=b.VARCHAR_COL26 and b.VARCHAR_COL27='sadf' and b.VARCHAR_COL28='suiji' with ur
/
<OPTGUIDELINES>
  <NLJOIN>
    <TBSCAN TABLE='a' FIRST='TRUE'/>
    <TBSCAN TABLE='b' />
  </NLJOIN>
</OPTGUIDELINES>
/

Access Plan:
-----------
        Total Cost:             3.45612e+09
        Query Degree:           1

              Rows
             RETURN
             (   1)
              Cost
               I/O
               |
           1.20779e-10
             NLJOIN
             (   2)
           3.45612e+09
           2.97114e+09
         /-----+------\
     581853         2.07577e-16
     TBSCAN           TBSCAN
     (   3)           (   4)
     225522           5939.47
     194085            5106
       |                |
     581853            15301
 TABLE: DB2INST1  TABLE: DB2INST1
       TTT              T1
       Q2               Q1

如下执行计划如果4）步骤评估不准确，结果集可能是较大的数据比如评估出来的结果是10以上，那么5)就会发生严重的表扫;除非SQL191112223538190为唯一索引或者主键
Access Plan:
-----------
        Total Cost:             225567
        Query Degree:           1

                          Rows
                         RETURN
                         (   1)
                          Cost
                           I/O
                           |
                          1.119
                         NLJOIN
                         (   2)
                         225567
                         194087
                 /---------+---------\
                1                     1.119
             FETCH                   TBSCAN
             (   3)                  (   5)
             14.1116                 225553
                2                    194085
           /---+----\                  |
          1         581853           581853
       IXSCAN   TABLE: DB2INST1  TABLE: DB2INST1
       (   4)         TTT              TTT
       7.06487        Q1               Q2
          1
         |
       581853
   INDEX: SYSIBM
 SQL191112223538190
         Q1


*/
//对于NLJoin，右侧子operator不应该是tabscan操作
func (n *Node) hasRightOperatorTabScan() bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == "NLJOIN" && len(nd.NextList) == 2 && nd.NextList[1].Stream.SrcOpType == "TBSCAN" {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return false
}

//在高并发下的SQL不应该出现全索引扫描的情况,不应出现begin index,end index,即索引的扫描不应该是Sargbal类型
//查找所有索引节点中是否存在sargbal类型的扫描
func (n *Node) hasIdxSargePredicate(predicateList MonGetExplainPredicateList) bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == "IXSCAN" && predicateList.hasAppliedByOperatorId(nd.Stream.SrcId, "SARG") {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return false
}

//数据发生倾斜导致问题
//条件范围内，数据发生倾斜导致问题(比如一天之内发生严重倾斜)
//执行计划中往往存在数据分布倾斜的问题导致数据不准确，比如整体表数据量非常巨大，但是一天之内做大量的变更。
//有一种场景是早晨大量的开工单处理，到晚上把单几乎全部关闭，那么这一天之内的工单的状态分布就会出现巨大的变化
//无法用小表突变的技术来检查数据量的变化，该问题属于局部范围内的突变，这种情况较为棘手
//对于这种问题单纯从执行计划上很难找出原因，必须结合快照来判断当前执行计划是否存在问题
//出现数据倾斜的问题往往执行时间较长，需要结合mon_get_pkg_cache_stmt中的度量指标和执行计划中的预估值来判断偏差是否过大(一般至少有数倍的差距)
//高并发下行扫描不应该过多，适当设置阈值判断是否发生如此多的rowsread是合理情况。
//MSJOIN=a+b;HSJOIN=a+b;NLjoin=a*b
//叶子节点的父节点为TBSCAN,或者fetch（IX操作+TABSCAN)操作
//根据执行计划预测需要扫描多少行数据,在pkg_cache中对应的rowsread的值如果小于此值，则执行计划评估准确
func (n *Node) predicateRowsScan() int {
	//NLJOIN,HSJOIN,MSJOIN   #ZZJOIN暂不考虑
	//大于2个节点的节点不作考虑，多节点情况不会涉及join问题
	var cursor *Node
	cursor = n
	if len(cursor.NextList) > 2 {
		return 0
	}
	//如果一直是1个节点那么进行下探
	for len(cursor.NextList) == 1 && cursor.NextList[0].Stream.SrcId != -1 {
		cursor = cursor.NextList[0]
	}
	//处理tabscan的情况
	if len(cursor.NextList) == 1 && cursor.NextList[0].Stream.SrcType == "D" {
		return cursor.NextList[0].Stream.StreamCount
	}
	//处理IX+TABSCAN的情况
	if len(cursor.NextList) == 2 && cursor.NextList[1].Stream.SrcType == "D" {
		return cursor.NextList[0].Stream.StreamCount
	}
	if cursor.Stream.SrcOpType == "NLJOIN" {
		return cursor.NextList[0].predicateRowsScan() + cursor.NextList[0].predicateRowsScan()*cursor.NextList[1].predicateRowsScan()
	}
	if cursor.Stream.SrcOpType == "HSJOIN" {
		return cursor.NextList[0].predicateRowsScan() + cursor.NextList[1].predicateRowsScan()
	}
	if cursor.Stream.SrcOpType == "MSJOIN" {
		return cursor.NextList[0].predicateRowsScan() + cursor.NextList[1].predicateRowsScan()
	}
	return 0
}

//----------------------------------------慢查询情况常见错误执行计划分析-------------------------------------//
//在一条SQL执行非常缓慢的时候常见的有如下几点:
// 1. load场景，数据量大，索引多rebuild时间长，主要发生在rebuild时间；reorg场景，runstats场景等，这些不涉及执行计划剔除
// 2. 一个存储过程中嵌套多个SQL循环语句，每一个SQL执行可能不是特别慢，但是循环次数太多导致时间增加较多
// 3. 一条SQL实际执行时间过长,这个是最为常见最容易出问题的地方，从执行计划结合快照主要分析此类语句。分析方法：
// 是否涉及到排序？ 是否出现了hashloop？ 是否表读记录数极多？是否出现了大量的索引扫描？ 是否出现了大量的临时表空间读写？不管哪种问题，作为自动推荐
//最常见的方式为：1. 统计信息是否合理？ 2. 索引是否多余？是否缺失？ 3. 是否数据量突增导致缓慢为正常现象,数据清理?
