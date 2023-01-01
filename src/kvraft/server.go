package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"log"
	"sync/atomic"
	"time"
)

const MAXWAITTINGTIME = 100 * time.Millisecond
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key      string
	Value    string
	Operator string
}

type KVServer struct {
	//mu      sync.Mutex
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	//幂等性判定map
	idempotenceMap map[string]bool
	kvPersisterMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// rpc请求幂等验证
	kv.mu.Lock()
	if sign, ok := kv.idempotenceMap[args.Id]; ok && sign == true {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//1 调用start()
	op := Op{
		Key:      args.Key,
		Operator: GET,
	}
	_, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[server%v Get] [Start()] {key%v}\n", kv.me, op.Key)

	//2 等待数据在raft集群中一致性
	t := time.NewTicker(MAXWAITTINGTIME)
	select {
	case temp := <-kv.applyCh:
		t.Stop()
		reply.Err = OK
		kv.mu.Lock()
		kv.idempotenceMap[args.Id] = true
		kv.mu.Unlock()
		//处理kvPersisterMap
		reply.Value = kv.getValueByKey(temp.Command)
		fmt.Printf("[server%v Get] [applier] {key%v}\n", kv.me, op.Key)

	case <-t.C:
		reply.Err = TIMEOUT

		t.Stop()
	}

	return

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// rpc请求幂等验证
	kv.mu.Lock()
	if sign, ok := kv.idempotenceMap[args.Id]; ok && sign == true {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//1 调用start()
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		Operator: args.Op,
	}

	_, _, ok := kv.rf.Start(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	fmt.Printf("[server%v PutAppend] [Start()] {key%v,value%v}\n", kv.me, op.Key, op.Value)

	//2 等待数据在raft集群中一致性
	t := time.NewTicker(MAXWAITTINGTIME)
	select {
	case temp := <-kv.applyCh:
		t.Stop()
		reply.Err = OK
		kv.mu.Lock()
		kv.idempotenceMap[args.Id] = true
		kv.mu.Unlock()
		//处理kvPersisterMap
		kv.processKvPersisterMap(temp.Command)
		fmt.Printf("[server%v PutAppend] [applier] {key%v,value%v}\n", kv.me, op.Key, op.Value)

	case <-t.C:
		reply.Err = TIMEOUT
		t.Stop()
	}

	//todo 判断 kv.applyCh中的数据 start()的是一条数据

	//3 返回rpc
	return

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register() on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.idempotenceMap = make(map[string]bool)
	kv.kvPersisterMap = make(map[string]string)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}

// 将成功一致性的op操作中包含的kv值,放在kvPersisterMap中
func (kv *KVServer) processKvPersisterMap(Command interface{}) {
	opTemp := Command.(Op)

	kv.mu.Lock()
	switch opTemp.Operator {
	case PUT:
		kv.kvPersisterMap[opTemp.Key] = opTemp.Value
	case APPEND:
		if _, ok := kv.kvPersisterMap[opTemp.Key]; ok {
			kv.kvPersisterMap[opTemp.Key] += opTemp.Value
		} else {
			kv.kvPersisterMap[opTemp.Key] = opTemp.Value
		}
	}
	kv.mu.Unlock()

}

// 根据key找value key不存在,则返回""
func (kv *KVServer) getValueByKey(Command interface{}) string {
	opTemp := Command.(Op)
	kv.mu.Lock()
	result, ok := kv.kvPersisterMap[opTemp.Key]
	kv.mu.Unlock()

	if !ok {
		result = ""
	}
	return result
}
