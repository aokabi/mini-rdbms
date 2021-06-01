package sql

import (
	"fmt"
	"mini-rdbms/lib/btree"
	"mini-rdbms/lib/buffer"
)

type Executor interface {
	Next(bufManager *buffer.BufferPoolManager) *btree.Tuple
}

type ExecSeqScan struct {
	tableIter *btree.Iter
	// recordを受け取って一致しているかを返す
	whileCond func(*btree.Tuple) bool
}

type ExecFilter struct {
	innerIter Executor
	cond      func(*btree.Tuple) bool
}

func (e *ExecSeqScan) Next(bufManager *buffer.BufferPoolManager) *btree.Tuple {
	if !e.tableIter.HasNext() {
		return nil
	}
	tuple := e.tableIter.Next(bufManager)
	if !e.whileCond(tuple) {
		return nil
	}
	return tuple
}

func (e *ExecFilter) Next(bufManager *buffer.BufferPoolManager) *btree.Tuple {
	for {
		tuple := e.innerIter.Next(bufManager)
		if tuple == nil {
			return nil
		}
		if e.cond(tuple) {
			return tuple
		}
	}
}

// クエリエクスキュータを生成・初期化する
type PlanNode interface {
	Start(bufManager *buffer.BufferPoolManager) Executor
}

type SeqScan struct {
	metaPageID buffer.PageID
	key        []byte
	whileCond      func(*btree.Tuple) bool
}

func NewSeqScan(metPageID buffer.PageID, key []byte, whileCond func(*btree.Tuple) bool) *SeqScan {
	return &SeqScan{metaPageID: metPageID, key: key, whileCond: whileCond}
}

type Filter struct {
	cond      func(*btree.Tuple) bool
	innerPlan PlanNode
}

func NewFilter(cond func(*btree.Tuple) bool, innerPlan PlanNode) *Filter {
	return &Filter{cond, innerPlan}
}

func (p *SeqScan) Start(bufManager *buffer.BufferPoolManager) Executor {
	t := btree.NewBtree(bufManager, p.metaPageID)
	iter, err := t.Search(bufManager, Encode(p.key))
	if err != nil {
		fmt.Println(err)
	}
	return &ExecSeqScan{
		tableIter: iter,
		whileCond: p.whileCond,
	}
}

func (p *Filter) Start(bufManager *buffer.BufferPoolManager) Executor {
	iter := p.innerPlan.Start(bufManager)
	return &ExecFilter{
		innerIter: iter,
		cond:      p.cond,
	}
}
