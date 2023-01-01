package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"strconv"
	"time"
)
import cryptoRand "crypto/rand"
import "math/big"
import mathRand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := cryptoRand.Int(cryptoRand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{Key: key, Id: strconv.Itoa(mathRand.Int())}
	reply := GetReply{}

	//随机得到一个leaderId
	pos := mathRand.Intn(len(ck.servers))
	fmt.Printf("[client Get] {Key%v}\n", args.Key)

LOOP:

	ok := ck.servers[pos].Call("KVServer.Get", &args, &reply)
	for !ok {
		ok = ck.servers[pos].Call("KVServer.Get", &args, &reply)
	}

	//若server不是leader 向得到的leaderId发送rpc
	if reply.Err == ErrWrongLeader {
		pos = (pos + 1) % len(ck.servers)
		goto LOOP
	}

	if reply.Err == TIMEOUT {

		time.Sleep(50 * time.Millisecond)
		goto LOOP
	}

	if reply.Err == ErrNoKey {
		return ""
	}

	fmt.Printf("[client Get] {Err%v, value%v}\n", reply.Err, reply.Value)

	return reply.Value

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    strconv.Itoa(mathRand.Int()),
	}
	reply := PutAppendReply{}

	//随机得到一个leaderId
	pos := mathRand.Intn(len(ck.servers))
	fmt.Printf("[client %v] {Key%v,Value%v}\n", op, args.Key, args.Value)
LOOP:
	ok := ck.servers[pos].Call("KVServer.PutAppend", &args, &reply)
	for !ok {
		ok = ck.servers[pos].Call("KVServer.PutAppend", &args, &reply)
	}

	//若server不是leader 向得到的leaderId发送rpc
	if reply.Err == ErrWrongLeader {
		pos = (pos + 1) % len(ck.servers)
		goto LOOP

	}
	if reply.Err == TIMEOUT {
		time.Sleep(50 * time.Millisecond)
		goto LOOP
	}
	fmt.Printf("[client %v] {LeaderId%v,Err%v}\n", op, pos, reply.Err)

	return

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
