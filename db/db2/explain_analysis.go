package db2

//进行explain经验性分析
//对于高并发短事务查询不应该存在hashJoin操作，即判断执行计划树中是否存在hashJoin操作
func (n *Node) HasHSJoin() bool {
	return n.hasOperatorType("HSJOIN")
}
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
