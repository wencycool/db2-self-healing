package db2

import (
	"fmt"
	"math"
	"strings"
)

//解析explain_stream,数据并构建多叉树
//explain_stream列表数据会得到唯一多叉树

type StreamList []*MonGetExplainStream

func (ds StreamList) FindSrcId(tgtId int) bool {
	for _, d := range ds {
		if d.SrcId == tgtId {
			return true
		}
	}
	return false
}

//将dlist中数据存放到Node中
func NewNode(dlist StreamList) *Node {
	//查找root节点,TgtId不是其它节点SrcId值的时候则为root节点
	LfPr := make([][3]int, 0) //保留未被添加的节点
	root := new(Node)
	for i, d := range dlist {
		if !dlist.FindSrcId(d.TgtId) {
			//不可以有多个root节点
			root.ParentId = math.MaxInt16
			root.Id = d.TgtId
			root.Stream = dlist[i]
			root.Level = 0
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
	Id       int                  //SrcId 当前节点
	ParentId int                  //父节点
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
func (n *Node) PrintRightNode() {
	if len(n.NextList) == 0 {
		return
	}
	if len(n.NextList) == 2 {
		fmt.Println(n.NextList[1].Id)
	}
	for _, n1 := range n.NextList {
		n1.PrintRightNode()
	}
}
