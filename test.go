package main

import (
	"fmt"
	"math"
)

func main() {
	nds := make([]*Data, 0)
	nds = append(nds, &Data{1, -1, 4})
	nds = append(nds, &Data{2, 4, 3})
	nds = append(nds, &Data{3, -1, 5})
	nds = append(nds, &Data{4, 5, 3})
	nds = append(nds, &Data{5, 3, 2})
	nds = append(nds, &Data{6, -1, 6})
	nds = append(nds, &Data{7, 6, 2})
	nds = append(nds, &Data{8, 2, 1})
	root := NewNode(nds)
	root.PrintData()

}

//创树
type Data struct {
	Id    int
	SrcId int
	TgtId int
}

type DataList []*Data

func (ds DataList) FindSrcId(tgtId int) bool {
	for _, d := range ds {
		if d.SrcId == tgtId {
			return true
		}
	}
	return false
}

//将dlist中数据存放到Node中
func NewNode(dlist DataList) *Node {
	//查找root节点,TgtId不是其它节点SrcId值的时候则为root节点
	LfPr := make([][3]int, 0) //保留未被添加的节点
	root := new(Node)
	for i, d := range dlist {
		if !dlist.FindSrcId(d.TgtId) {
			//不可以有多个root节点
			root.ParentId = math.MaxInt64
			root.Id = d.TgtId
			root.d = Data{0, d.TgtId, math.MaxInt64}
		}
		LfPr = append(LfPr, [3]int{i, d.SrcId, d.TgtId})
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
				parentNode.NextList = append(parentNode.NextList, &Node{*d, d.SrcId, d.TgtId, make([]*Node, 0)})
				LfPr = append(LfPr[:i], LfPr[i+1:len(LfPr)]...)
				break
			}
		}
	}

	return root
}

type Node struct {
	d        Data    //当前数据
	Id       int     //SrcId 当前节点
	ParentId int     //父节点
	NextList []*Node //孩子节点
}

func (n *Node) add(nd *Node) bool {
	if ParentNode, ok := n.searchParendNode(nd.ParentId); ok {
		ParentNode.NextList = append(ParentNode.NextList, nd)
		return true
	}
	return false
}

//查找是否存在指定SrcId,如果找到则返回该Node节点指针，DFS深度优先
func (n *Node) searchParendNode(ParentId int) (*Node, bool) {
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

//打印root节点所有数据
func (n *Node) PrintData() {
	fmt.Printf("SrcId:%-10dTgtId:%-10d\n", n.d.SrcId, n.d.TgtId)
	if len(n.NextList) == 0 {
		return
	}
	for _, n1 := range n.NextList {
		n1.PrintData()
	}
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
