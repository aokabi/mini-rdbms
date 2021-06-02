package btree

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"mini-rdbms/lib/buffer"
	"mini-rdbms/lib/disk"
	"sort"
)

type Items []Tuple
type PageID = disk.PageID

func (s Items) String() (res string) {
	a := []Tuple(s)
	for i := 0; i < len(a); i++ {
		res += fmt.Sprintf("{ Key: %+v Value: %+v } ", string(a[i].Key), string(a[i].Value))
	}
	res += fmt.Sprintln("")
	return
}

// nibutan
//
func (s *Items) find(key []byte) (int, bool) {
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

func (s *Items) insertAt(i int, item Tuple) {
	*s = append(*s, Tuple{})
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
	return &meta{disk.InvalidPageID, disk.InvalidPageID}
}

func (n *meta) Read(dst []byte) (int, error) {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(n)
	if err != nil {
		return 0, err
	}
	copy(dst, buffer.Bytes())
	return buffer.Len(), nil
}

func (n *meta) Write(src []byte) (int, error) {
	buffer := bytes.NewReader(src)
	dec := gob.NewDecoder(buffer)
	meta := newMeta()
	err := dec.Decode(meta)
	if err != nil {
		return 0, err
	}
	n.FirstLeafPageID = meta.FirstLeafPageID
	n.RootPageID = meta.RootPageID
	return buffer.Len(), nil
}

// gobでbyteと変換するためフィールドはpublic
type node struct {
	Items    Items
	Children Children
	NextLeaf PageID
	PrevLeaf PageID
}

func newNode() *node {
	return &node{
		make([]Tuple, 0),
		make([]PageID, 0),
		disk.InvalidPageID,
		disk.InvalidPageID}
}

func (n *node) String() string {
	return fmt.Sprintf("{Items: %v, Children: %+v}", n.Items, n.Children)
}

func (n *node) Read(dst []byte) (int, error) {
	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	err := enc.Encode(n)
	if err != nil {
		return 0, err
	}
	copy(dst, buffer.Bytes())
	return buffer.Len(), nil
}

func (n *node) Write(src []byte) (int, error) {
	buffer := bytes.NewReader(src)
	dec := gob.NewDecoder(buffer)
	node := newNode()
	err := dec.Decode(node)
	if err != nil {
		return 0, err
	}

	n.Children = node.Children
	n.Items = node.Items
	n.NextLeaf = node.NextLeaf
	n.PrevLeaf = node.PrevLeaf
	return buffer.Len(), nil
}

// return value
func (n *node) search(bufManager *buffer.BufferPoolManager, pageID PageID, key []byte) (*Iter, error) {
	i, found := n.Items.find(key)
	if len(n.Children) == 0 && found {
		return newIter(pageID, i), nil
	} else if len(n.Children) > 0 {
		if found {
			i +=1
		}
		children, _ := bufManager.FetchPage(n.Children[i])
		childNode := newNode()
		defer func() {
			children.SetPage(childNode)
			children.Close()
		}()
		children.GetPage(childNode)
		return childNode.search(bufManager, children.PageID, key)
	}
	return nil, errors.New("not found")
}

// もしnをsplitしたらを返す
func (n *node) insert(bufferManager *buffer.BufferPoolManager, pageID PageID, item Tuple, order uint) (*node, PageID) {
	i, found := n.Items.find(item.Key)
	// if leaf
	if len(n.Children) == 0 {
		// すでにあったらreplace
		if found {
			n.Items[i] = item
			return nil, disk.InvalidPageID
		}
		n.Items.insertAt(i, item)
		if len(n.Items) > int(order-1) {
			return n.split(bufferManager, pageID, int(order/2))
		}
	} else { // if non-leaf
		buf, _ := bufferManager.FetchPage(n.Children[i])
		childNode := newNode()
		defer func() {
			buf.SetPage(childNode)
			buf.Close()
		}()
		buf.GetPage(childNode)
		newNode, newPageID := childNode.insert(bufferManager, buf.PageID, item, order)
		if newNode != nil {
			n.Items.insertAt(i, Tuple{newNode.Items[0].Key, newPageID.Bytes()})
			n.Children.insertAt(i+1, newPageID)
			if len(n.Items) > int(order-1) {
				return n.split(bufferManager, pageID, int(order/2))
			}
		}
	}
	return nil, disk.InvalidPageID
}

func (n *node) insertLoop(bufferManager *buffer.BufferPoolManager, pageID PageID, item Tuple, order uint) (*node, PageID) {
	i, found := n.Items.find(item.Key)
	currentNode := n
	for len(currentNode.Children) != 0 {
		buf, _ := bufferManager.FetchPage(currentNode.Children[i])
		childNode := newNode()
		defer func() {
			buf.SetPage(childNode)
			buf.Close()
		}()
		buf.GetPage(childNode)

	}

	// すでにあったらreplace
	if found {
		currentNode.Items[i] = item
		return nil, disk.InvalidPageID
	}
	currentNode.Items.insertAt(i, item)
	if len(currentNode.Items) > int(order-1) {
		return currentNode.split(bufferManager, pageID, int(order/2))
	}

	return nil, disk.InvalidPageID
}

type Children []PageID

func (c *Children) insertAt(i int, pageID PageID) {
	*c = append(*c, disk.InvalidPageID)
	if i < len(*c) {
		copy((*c)[i+1:], (*c)[i:])
	}
	(*c)[i] = pageID
}

// iでsplit
func (n *node) split(bufferManager *buffer.BufferPoolManager, pageID PageID, i int) (*node, PageID) {
	secondBuffer := bufferManager.CreatePage()
	second := newNode()
	defer func() {
		secondBuffer.SetPage(second)
		secondBuffer.Close()
	}()

	second.Items = append(second.Items, n.Items[i:]...)
	n.Items = n.Items[:i]

	// leaf同士のリンクをつなぎ替える
	if n.NextLeaf != disk.InvalidPageID {
		second.NextLeaf = n.NextLeaf
		rightBuffer, _ := bufferManager.FetchPage(n.NextLeaf)
		right := newNode()
		defer func() {
			rightBuffer.SetPage(right)
			rightBuffer.Close()
		}()
		rightBuffer.GetPage(right)
		right.PrevLeaf = secondBuffer.PageID
	}
	n.NextLeaf = secondBuffer.PageID
	second.PrevLeaf = pageID
	return second, secondBuffer.PageID
}

type Btree struct {
	order           uint // 最大で持てる子供の数
	MetaPageID      PageID
	firstLeafPageID PageID
}

func CreateBtree(bufManager *buffer.BufferPoolManager) *Btree {
	metaBuffer := bufManager.CreatePage()
	meta := newMeta()
	// 新しいページを作る
	buffer := bufManager.CreatePage()
	root := newNode()
	defer func() {
		metaBuffer.SetPage(meta)
		buffer.SetPage(root)
		metaBuffer.Close()
		buffer.Close()
	}()
	meta.RootPageID = buffer.PageID
	meta.FirstLeafPageID = buffer.PageID
	return &Btree{
		order:           4,
		MetaPageID:      metaBuffer.PageID,
		firstLeafPageID: buffer.PageID,
	}
}

// btreeの情報がすでにどこかのページにある場合に使う
func NewBtree(bufManager *buffer.BufferPoolManager, metaPageID PageID) *Btree {
	metaPage, err := bufManager.FetchPage(metaPageID)
	if err != nil {
		panic(err)
	}
	meta := newMeta()
	metaPage.GetPage(meta)
	fmt.Println("root: ", meta.RootPageID)
	rootPage, err := bufManager.FetchPage(meta.RootPageID)
	if err != nil {
		panic(err)
	}
	root := newNode()
	rootPage.GetPage(root)

	defer func() {
		metaPage.SetPage(meta)
		rootPage.SetPage(root)
		metaPage.Close()
		rootPage.Close()
	}()

	fmt.Println("new btree", root)
	return &Btree{4, metaPage.PageID, meta.FirstLeafPageID}
}

func (b *Btree) String() string {
	return fmt.Sprintf("order: %v", b.order)
}

// return value
func (b *Btree) Search(bufManager *buffer.BufferPoolManager, key []byte) (*Iter, error) {
	// get root node
	metaBuffer, _ := bufManager.FetchPage(b.MetaPageID)
	meta := newMeta()
	metaBuffer.GetPage(meta)
	rootBuffer, _ := bufManager.FetchPage(meta.RootPageID)
	root := newNode()
	rootBuffer.GetPage(root)

	defer func() {
		metaBuffer.SetPage(meta)
		rootBuffer.SetPage(root)
		metaBuffer.Close()
		rootBuffer.Close()
	}()

	return root.search(bufManager, rootBuffer.PageID, key)
}

func (b *Btree) SearchAll(bufManager *buffer.BufferPoolManager) (*Iter, error) {
	return &Iter{b.firstLeafPageID, 0, true}, nil
}

func (b *Btree) Insert(bufManager *buffer.BufferPoolManager, key []byte, value []byte) error {
	// leaf nodeにたどり着くまで再帰的に
	metaBuffer, err := bufManager.FetchPage(b.MetaPageID)
	if err != nil {
		return fmt.Errorf("[%v], %w", key, err)
	}
	meta := newMeta()
	metaBuffer.GetPage(meta)
	rootBuffer, err := bufManager.FetchPage(meta.RootPageID)
	if err != nil {
		return fmt.Errorf("[%v], %w", key, err)
	}
	root := newNode()
	rootBuffer.GetPage(root)
	defer func() {
		metaBuffer.SetPage(meta)
		metaBuffer.Close()
		rootBuffer.SetPage(root)
		rootBuffer.Close()
	}()

	node, newPageID := root.insert(bufManager, meta.RootPageID, Tuple{key, value}, b.order)
	if node == nil {
		return nil
	}
	// rootを分割
	newRoot := newNode()
	newRootBuffer := bufManager.CreatePage()
	defer func() {
		newRootBuffer.SetPage(newRoot)
		newRootBuffer.Close()
	}()
	oldRootPageID := meta.RootPageID
	meta.RootPageID = newRootBuffer.PageID
	newRoot.Children = append(newRoot.Children, oldRootPageID, newPageID)
	newRoot.Items = append(newRoot.Items, Tuple{node.Items[0].Key, newPageID.Bytes()})

	return nil
}

type Tuple struct {
	Key   []byte
	Value []byte
}

type Iter struct {
	pageID  PageID
	idx     int // node内の要素の位置
	hasNext bool
}

func newIter(pageID PageID, idx int) *Iter {
	return &Iter{pageID: pageID, idx: idx, hasNext: true}
}

func (i *Iter) HasNext() bool {
	return i.hasNext
}

func (i *Iter) Next(bufManager *buffer.BufferPoolManager) *Tuple {
	// get element
	leafBuffer, _ := bufManager.FetchPage(i.pageID)
	leafNode := newNode()
	leafBuffer.GetPage(leafNode)
	defer func() {
		leafBuffer.SetPage(leafNode)
		leafBuffer.Close()
	}()

	value := &leafNode.Items[i.idx]

	// increment index
	i.idx++

	// もうleaf内にイテレートする要素が残っていなければ
	if i.idx >= len(leafNode.Items) {
		i.idx = 0
		// 次のleafに
		nextLeaf := leafNode.NextLeaf
		if nextLeaf == disk.InvalidPageID {
			i.hasNext = false
		}
		i.pageID = nextLeaf
	}

	return value

}

type tuples struct {
	items    []*Tuple
	iterator *Iter
}

func (t *tuples) HasNext() bool {
	return t.iterator.HasNext()
}

func (t *tuples) Next(bufManager *buffer.BufferPoolManager) *Tuple {
	return t.iterator.Next(bufManager)
}
