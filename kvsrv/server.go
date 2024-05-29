package kvsrv

import (
	"log"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type TransactionInfo struct {
	TransId   int
	Value string
	LastTransTime time.Time
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kvStorage map[string]string
	historyQueries map[int64]*TransactionInfo
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if old, executed := kv.hasDuplicateId(args.ClientId, args.TransId); executed {
		*reply = GetReply {
			old,
		}
		return
	}

	val, ok := kv.kvStorage[args.Key]
	kv.addRecord(args.ClientId, args.TransId, val)

	if !ok {
		DPrintf("[Server] Client trying to get a value with key [%v] which does not exist!", args.Key)
		return
	}

	*reply = GetReply{
		val,
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if old, executed := kv.hasDuplicateId(args.ClientId, args.TransId); executed {
		*reply = PutAppendReply {
			old,
		}
		return
	}

	kv.kvStorage[args.Key] = args.Value
	kv.addRecord(args.ClientId, args.TransId, args.Value)

	*reply = PutAppendReply{
		args.Value,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if old, executed := kv.hasDuplicateId(args.ClientId, args.TransId); executed {
		*reply = PutAppendReply {
			old,
		}
		return
	}

	old, ok := kv.kvStorage[args.Key]

	if !ok {
		kv.kvStorage[args.Key] = args.Value
		kv.addRecord(args.ClientId, args.TransId, args.Value)

		*reply = PutAppendReply {
			"",
		}
		
		return
	}

	newVal := old + args.Value
	kv.addRecord(args.ClientId, args.TransId, old)
	kv.kvStorage[args.Key] = newVal

	*reply = PutAppendReply {
		old,
	}
}

func (kv *KVServer) TransactionComplete(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.historyQueries, args.ClientId)
}

func (kv *KVServer) addRecord(clientId int64, transId int, val string) {
	current, ok := kv.historyQueries[clientId]

	if ok {
		current.TransId = transId
		current.Value = val
		current.LastTransTime = time.Now()

		return
	}

	kv.historyQueries[clientId] = &TransactionInfo{
		transId,
		val,
		time.Now(),
	}
}

func (kv *KVServer) hasDuplicateId(clientId int64, transId int) (string, bool) {
	val, clientOk := kv.historyQueries[clientId]

	if !clientOk {
		return "", false
	}

	if val.TransId == transId {
		return val.Value, true
	}

	return "", false
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.kvStorage = make(map[string]string)
	kv.historyQueries = make(map[int64]*TransactionInfo)

	return kv
}
