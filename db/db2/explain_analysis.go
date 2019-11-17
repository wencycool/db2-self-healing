package db2

//进行explain经验性分析
//对于搞并发短事务查询不应该存在hashJoin操作，即判断执行计划树中是否存在hashJoin操作
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

//对于搞并发操作，右侧子operator不应该出现IXAND操作
func (n *Node) HasRightOperatorIXAnd() bool {
	stack := new(Stack)
	stack.push(n)
	for !stack.isEmpty() {
		nd := stack.pop()
		if len(nd.NextList) == 2 && nd.NextList[1].Stream.SrcOpType == "IXAND" {
			return true
		}
		for _, v := range nd.NextList {
			stack.push(v)
		}
	}
	return false
}

//对于
