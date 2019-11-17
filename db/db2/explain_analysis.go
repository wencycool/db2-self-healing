package db2

//进行explain经验性分析
//对于高并发短事务查询不应该存在hashJoin操作，即判断执行计划树中是否存在hashJoin操作
func (n *Node) HasHSJoin() bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if nd.Stream.SrcOpType == "HSJOIN" {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
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
		if nd.Stream.SrcOpType == "NLJOIN" && len(nd.NextList) == 2 && nd.NextList[1].Stream.SrcOpType == "FETCH" && len(nd.NextList[1].NextList) == 2 && nd.NextList[1].NextList[0].Stream.SrcOpType == "RIDSCN" {
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
