package lock

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockid   string
	mu       sync.Mutex
	cilentID string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, lockid: l, cilentID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	lk.mu.Lock()
	defer lk.mu.Unlock()
	for {
		value, version, err := lk.ck.Get(lk.lockid)
		// println("获取锁版本号", version)
		if err == rpc.ErrNoKey {
			err := lk.ck.Put(lk.lockid, lk.cilentID, 0)
			if err == rpc.OK {
				// println("创建，获得锁并且返回")
				return
			}
		} else if err == rpc.OK {
			if value == lk.cilentID {
				// println("我草")
				return
			}
			if value == "empty" {
				err := lk.ck.Put(lk.lockid, lk.cilentID, version)
				if err == rpc.OK {
					return
				}
			}
		}
		// println("循环中")
		time.Sleep(2 * time.Second)
	}

}

func (lk *Lock) Release() {
	// Your code here
	lk.mu.Lock()
	defer lk.mu.Unlock()
	for {
		value, version, err := lk.ck.Get(lk.lockid)
		// println("解锁版本号", version)
		if err == rpc.OK {
			if value == lk.cilentID {
				errRelease := lk.ck.Put(lk.lockid, "empty", version)
				// println("解锁版本号", version)
				if errRelease == rpc.OK {
					return
				}
			} else {
				return
			}
		} else if err == rpc.ErrNoKey {
			return
		}
		time.Sleep(2 * time.Second)

	}

}
