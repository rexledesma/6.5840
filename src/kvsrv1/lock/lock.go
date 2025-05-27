package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const LockStateReleased = ""

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck         kvtest.IKVClerk
	identifier string
	key        string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, identifier: kvtest.RandValue(8), key: l}

	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if !(err == rpc.ErrNoKey || value == LockStateReleased) {
			continue
		}

		ok := lk.ck.Put(lk.key, lk.identifier, version)
		if ok == rpc.OK {
			return
		}
	}
}

func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.key)

	if err != rpc.OK || value != lk.identifier {
		return
	}

	lk.ck.Put(lk.key, LockStateReleased, version)
}
