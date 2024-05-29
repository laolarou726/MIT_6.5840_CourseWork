package kvsrv

type TransactionType uint8

const (
	GetTransaction TransactionType = 1 << iota
	PutAppendTransaction
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	TransId int
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	TransId int
}

type GetReply struct {
	Value string
}
