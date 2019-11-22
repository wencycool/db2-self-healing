package db2

import (
	"strings"
)

//进行explain经验性分析

func (n *Node) HasNLJoin() bool {
	return n.hasOperatorType("NLJOIN")
}
func (n *Node) HasIXAnd() bool {
	return n.hasOperatorType("IXAND")
}

func (n *Node) HasIXScan() bool {
	return n.hasOperatorType("IXSCAN")
}
func (n *Node) HasRIDScan() bool {
	return n.hasOperatorType("RIDSCN")
}
func (n *Node) NumberJoins() int {
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
			if strings.ToUpper(opType) == "ALL" {
				cnt++
			} else if nd.Stream.SrcOpType == opType {
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
func (n *Node) HasHSJoin() bool {
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
func (n *Node) HasRightOperatorIXAnd() bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == "NLJOIN" && len(nd.NextList) == 2 && nd.NextList[1].Stream.SrcOpType == "FETCH" && len(nd.NextList[1].NextList) == 2 && nd.NextList[1].NextList[0].HasIXAnd() {
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
*/
//对于NLJoin，右侧子operator不应该是tabscan操作
func (n *Node) HasRightOperatorTabScan() bool {
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

//不应该出现全索引扫描的情况,不应出现begin index,end index,即索引的扫描不应该是Sargbal类型

//数据发生倾斜导致问题
//条件范围内，数据发生倾斜导致问题(比如一天之内发生严重倾斜)
//执行计划中往往存在数据分布倾斜的问题导致数据不准确，比如整体表数据量非常巨大，但是一天之内做大量的变更。
//有一种场景是早晨大量的开工单处理，到晚上把单几乎全部关闭，那么这一天之内的工单的状态分布就会出现巨大的变化
//无法用小表突变的技术来检查数据量的变化，该问题属于局部范围内的突变，这种情况较为棘手
//对于这种问题单纯从执行计划上很难找出原因，必须结合快照来判断当前执行计划是否存在问题
//出现数据倾斜的问题往往执行时间较长，需要结合mon_get_pkg_cache_stmt中的度量指标和执行计划中的预估值来判断偏差是否过大(一般至少有数倍的差距)

//MSJOIN=a+b;HSJOIN=a+b;NLjoin=a*b
//叶子节点的父节点为TBSCAN,或者fetch（IX操作+TABSCAN)操作
//根据执行计划预测需要扫描多少行数据
func (n *Node) PredicateRowsScan() int {
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
		return cursor.NextList[0].PredicateRowsScan() + cursor.NextList[0].PredicateRowsScan()*cursor.NextList[1].PredicateRowsScan()
	}
	if cursor.Stream.SrcOpType == "HSJOIN" {
		return cursor.NextList[0].PredicateRowsScan() + cursor.NextList[1].PredicateRowsScan()
	}
	if cursor.Stream.SrcOpType == "MSJOIN" {
		return cursor.NextList[0].PredicateRowsScan() + cursor.NextList[1].PredicateRowsScan()
	}
	return 0
}
