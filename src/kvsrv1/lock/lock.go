package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck        kvtest.IKVClerk
	sharedKey string
	id        string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, sharedKey: l, id: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, version, _ := lk.ck.Get(lk.sharedKey)
		if val != "" {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		err := lk.ck.Put(lk.sharedKey, lk.id, version)
		if err != rpc.OK{
			time.Sleep(10 * time.Millisecond)
			continue
		}
		
		break
	}
}

func (lk *Lock) Release() {
	for {
		_, version, _ := lk.ck.Get(lk.sharedKey)
		err := lk.ck.Put(lk.sharedKey, "", version)
		if err != rpc.OK{
			time.Sleep(5 * time.Millisecond)
			continue
		}
		
		break
	}
}
