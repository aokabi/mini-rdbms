package lib

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
)

type items []tuple

func (s items) String() (res string) {
	a := []tuple(s)
	for i := 0; i < len(a); i++ {
		res += fmt.Sprintf("{ Key: %+v Value: %+v } ", string(a[i].Key), string(a[i].Value))
	}
	res += fmt.Sprintln("")
	return
}

// nibutan
//
func (s *items) find(key []byte) (int, bool) {
	i := sort.Search(len(*s), func(i int) bool {
		return bytes.Compare(key, (*s)[i].Key) == -1
	})
	// if found
	// i == 0のときはs(items)内のどの要素よりも小さい時
	if i > 0 && bytes.Equal((*s)[i-1].Key, key) {
		return i - 1, true
	}
	return i, false
}

func (s *items) insertAt(i int, item tuple) {
	*s = append(*s, tuple{})
	if i < len(*s) {
		copy((*s)[i+1:], (*s)[i:])
	}
	(*s)[i] = item
}

// gobでbyteと変換するためフィールドはpublic
type meta struct {
	RootPageID      PageID
	FirstLeafPageID PageID // 一番左にあるleafのPageID
}

func newMeta() *meta {
	return &meta{invalidPageID, invalidPageID}
}

// gobでbyteと変換するためフィールドはpublic
type node struct {
	Items    items
	Children children
	NextLeaf PageID
	PrevLeaf PageID
}

func newNode() *node {
	return &node{NextLeaf: invalidPageID, PrevLeaf: invalidPageID}
}

func (n *node) String() string {
	return fmt.Sprintf("{items: %v, children: %+v}", n.Items, n.Children)
}

// return value
func (n *node) search(bufManager *BufferPoolManager, key []byte) ([]byte, error) {
	i, found := n.Items.find(key)
	fmt.Println(i)
	if found {
		fmt.Println(string(n.Items[i].Value))
		return n.Items[i].Value, nil
	} else if len(n.Children) > 0 {
		children, _ := bufManager.fetchPage(n.Children[i])
		return children.page.(*node).search(bufManager, key)
	}
	return nil, errors.New("not found")
}

// もしnをsplitしたらを返す
func (n *node) insert(bufferManager *BufferPoolManager, pageID PageID, item tuple, order uint) (*node, PageID) {
	i, found := n.Items.find(item.Key)
	// if leaf
	if len(n.Children) == 0 {
		// すでにあったらreplace
		if found {
			n.Items[i] = item
			return nil, invalidPageID
		}
		n.Items.insertAt(i, item)
		if len(n.Items) > int(order-1) {
			return n.split(bufferManager, pageID, int(order/2))
		}
	} else { // if non-leaf
		buf, _ := bufferManager.fetchPage(n.Children[i])
		newNode, newPageID := buf.page.(*node).insert(bufferManager, buf.pageID, item, order)
		if newNode != nil {
			n.Items.insertAt(i, tuple{newNode.Items[0].Key, newPageID.bytes()})
			n.Children.insertAt(i+1, newPageID)
			if len(n.Items) > int(order-1) {
				return n.split(bufferManager, pageID, int(order/2))
			}
		}
	}
	return nil, invalidPageID
}

type children []PageID

func (c *children) insertAt(i int, pageID PageID) {
	*c = append(*c, invalidPageID)
	if i < len(*c) {
		copy((*c)[i+1:], (*c)[i:])
	}
	(*c)[i] = pageID
}

// iでsplit
func (n *node) split(bufferManager *BufferPoolManager, pageID PageID, i int) (*node, PageID) {
	secondBuffer := bufferManager.createPage()
	second := newNode()
	secondBuffer.page = second
	second.Items = append(second.Items, n.Items[i:]...)
	n.Items = n.Items[:i]

	// leaf同士のリンクをつなぎ替える
	if n.NextLeaf != invalidPageID {
		second.NextLeaf = n.NextLeaf
		rightBuffer, _ := bufferManager.fetchPage(n.NextLeaf)
		right := rightBuffer.page.(*node)
		right.PrevLeaf = secondBuffer.pageID
	}
	n.NextLeaf = secondBuffer.pageID
	second.PrevLeaf = pageID
	return second, secondBuffer.pageID
}

type Btree struct {
	order           uint // 最大で持てる子供の数
	root            *node
	metaPageID      PageID
	firstLeafPageID PageID
}

func CreateBtree(bufManager *BufferPoolManager) *Btree {
	metaBuffer := bufManager.createPage()
	meta := newMeta()
	metaBuffer.page = meta
	// 新しいページを作る
	buffer := bufManager.createPage()
	root := newNode()
	buffer.page = root
	meta.RootPageID = buffer.pageID
	meta.FirstLeafPageID = buffer.pageID
	return &Btree{
		order:           4,
		root:            root,
		metaPageID:      metaBuffer.pageID,
		firstLeafPageID: buffer.pageID,
	}
}

// btreeの情報がすでにどこかのページにある場合に使う
func NewBtree(bufManager *BufferPoolManager, metaPageID PageID) *Btree {
	metaPage, _ := bufManager.fetchPage(metaPageID)
	meta := metaPage.page.(*meta)
	rootPage, _ := bufManager.fetchPage(meta.RootPageID)
	fmt.Println("new btree", rootPage.page)
	return &Btree{4, rootPage.page.(*node), rootPage.pageID, meta.FirstLeafPageID}
}

func (b *Btree) String() string {
	return fmt.Sprintf("order: %v, root: %v", b.order, b.root)
}

// return value
func (b *Btree) Search(bufManager *BufferPoolManager, key []byte) ([]byte, error) {
	return b.root.search(bufManager, key)
}

func (b *Btree) SearchAll(bufManager *BufferPoolManager) (iter, error) {
	buffer, _ := bufManager.fetchPage(b.firstLeafPageID)
	return iter{buffer, 0, true}, nil
}

func (b *Btree) Insert(bufManager *BufferPoolManager, key []byte, value []byte) error {
	// leaf nodeにたどり着くまで再帰的に
	metaBuffer, _ := bufManager.fetchPage(b.metaPageID)
	meta := metaBuffer.page.(*meta)
	node, newPageID := b.root.insert(bufManager, meta.RootPageID, tuple{key, value}, b.order)
	if node == nil {
		return nil
	}
	oldRootPageID := meta.RootPageID
	rootBuffer := bufManager.createPage()
	b.root = newNode()
	rootBuffer.page = b.root
	meta.RootPageID = rootBuffer.pageID
	b.root.Children = append(b.root.Children, oldRootPageID, newPageID)
	b.root.Items = append(b.root.Items, tuple{node.Items[0].Key, newPageID.bytes()})

	return nil
}

type tuple struct {
	Key   []byte
	Value []byte
}

type iter struct {
	buffer  *Buffer
	idx     int // node内の要素の位置
	hasNext bool
}

func (i *iter) HasNext() bool {
	return i.hasNext
}

func (i *iter) Next(bufManager *BufferPoolManager) *tuple {
	// get element
	leafNode := i.buffer.page.(*node)
	value := &leafNode.Items[i.idx]

	// increment index
	i.idx++

	// もうleaf内にイテレートする要素が残っていなければ
	if i.idx >= len(leafNode.Items) {
		i.idx = 0
		// 次のleafに
		nextLeaf := leafNode.NextLeaf
		if nextLeaf == invalidPageID {
			i.hasNext = false
		}
		buffer, err := bufManager.fetchPage(leafNode.NextLeaf)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		i.buffer = buffer
	}

	return value

}

type tuples struct {
	items    []*tuple
	iterator *iter
}

func (t *tuples) HasNext() bool {
	return t.iterator.HasNext()
}

func (t *tuples) Next(bufManager *BufferPoolManager) *tuple {
	return t.iterator.Next(bufManager)
}
