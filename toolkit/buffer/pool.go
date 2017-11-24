package buffer

import (
	"sync"
	"fmt"
	"github.com/allbuleyu/creawler/errors"
	"sync/atomic"
)

// Pool 代表数据缓冲池的接口类型。
type Pool interface {
	// BufferCap 用于获取池中缓冲器的统一容量。
	BufferCap() uint32
	// MaxBufferNumber 用于获取池中缓冲器的最大数量。
	MaxBufferNumber() uint32
	// BufferNumber 用于获取池中缓冲器的数量。
	BufferNumber() uint32
	// Total 用于获取缓冲池中数据的总数。
	Total() uint64
	// Put 用于向缓冲池放入数据。
	// 注意！本方法应该是阻塞的。
	// 若缓冲池已关闭则会直接返回非nil的错误值。
	Put(datum interface{}) error
	// Get 用于从缓冲池获取数据。
	// 注意！本方法应该是阻塞的。
	// 若缓冲池已关闭则会直接返回非nil的错误值。
	Get() (datum interface{}, err error)
	// Close 用于关闭缓冲池。
	// 若缓冲池之前已关闭则返回false，否则返回true。
	Close() bool
	// Closed 用于判断缓冲池是否已关闭。
	Closed() bool
}

// myPool 代表数据缓冲池接口的实现类型。
type myPool struct {
	// bufferCap 代表缓冲器的统一容量。
	bufferCap uint32
	// maxBufferNumber 代表缓冲器的最大数量。
	maxBufferNumber uint32
	// bufferNumber 代表缓冲器的实际数量。
	bufferNumber uint32
	// total 代表池中数据的总数。
	total uint64
	// bufCh 代表存放缓冲器的通道。
	bufCh chan Buffer
	// closed 代表缓冲池的关闭状态：0-未关闭；1-已关闭。
	closed uint32
	// lock 代表保护内部共享资源的读写锁。
	rwlock sync.RWMutex
}

func NewPool(bufferCap uint32, maxBufferNumber uint32) (Pool, error) {
	if bufferCap == 0 {
		errMsg := fmt.Sprintf("illegal buffer cap for buffer pool: %d", bufferCap)

		return nil, errors.NewIllegalParameterError(errMsg)
	}

	if maxBufferNumber == 0 {
		errMsg := fmt.Sprintf("illegal max buffer number for buffer pool: %d", maxBufferNumber)

		return nil, errors.NewIllegalParameterError(errMsg)
	}

	buffer,_ := NewBuffer(bufferCap)

	bufCh := make(chan Buffer, maxBufferNumber)
	bufCh <- buffer

	return &myPool{
		bufferCap:       bufferCap,
		maxBufferNumber: maxBufferNumber,
		bufferNumber:    1,
		bufCh:           bufCh,
	}, nil
}

func (pool *myPool) BufferCap() uint32 {
	return pool.bufferCap
}

func (pool *myPool) MaxBufferNumber() uint32 {
	return pool.maxBufferNumber
}

func (pool *myPool) BufferNumber() uint32 {
	return atomic.LoadUint32(&pool.bufferNumber)
}

func (pool *myPool) Total() uint64 {
	return atomic.LoadUint64(&pool.total)
}

func (pool *myPool) Put(datum interface{}) (err error) {
	if pool.Closed() {
		return ErrClosedBufferPool
	}

	var count uint32
	maxCount := pool.bufferNumber * 5   // 尝试5次放入数据,如果不成功表示失败

	for buf := range pool.bufCh {
		ok, err := pool.putData(buf, datum, &count, maxCount)
		if ok || err != nil {   // 没有错误表示数据放入成功
			break
		}

	}

	return
}

func (pool *myPool) putData(buf Buffer, datum interface{}, count *uint32, maxCount uint32) (ok bool, err error) {
	if pool.Closed() {
		return false, ErrClosedBufferPool
	}

	defer func() {
		pool.rwlock.RLock()
		if pool.Closed() {
			atomic.AddUint32(&pool.bufferNumber, ^uint32(0))
			err = ErrClosedBufferPool
		}else {
			pool.bufCh <- buf
		}
		pool.rwlock.RUnlock()

	}()

	ok, err = buf.Put(datum)
	if ok {
		atomic.AddUint64(&pool.total, 1)
		return
	}
	if err != nil {
		return
	}

	// 若因缓冲器已满而未放入数据就递增计数。
	(*count)++

	// 如果尝试向缓冲器放入数据的失败次数达到阈值，
	// 并且池中缓冲器的数量未达到最大值，
	// 那么就尝试创建一个新的缓冲器，先放入数据再把它放入池。
	if *count >= maxCount && pool.BufferNumber() < pool.MaxBufferNumber() {
		pool.rwlock.Lock()
		if pool.Closed() {
			pool.rwlock.Unlock()
			return false, ErrClosedBufferPool
		}

		newBuf, _ := NewBuffer(pool.BufferCap())
		newBuf.Put(datum)
		pool.bufCh <- newBuf

		atomic.AddUint64(&pool.total, 1)
		atomic.AddUint32(&pool.bufferNumber, 1)

		pool.rwlock.Unlock()
		ok = true
	}
	return
}


func (pool *myPool) Get() (datum interface{}, err error) {
	if pool.Closed() {
		return nil, ErrClosedBufferPool
	}
	var count uint32
	maxCount := pool.bufferNumber * 5

	for buf := range pool.bufCh {
		datum, err = pool.getData(buf, &count, maxCount)

		if datum != nil && err == nil {
			break
		}
	}

	return
}

func (pool *myPool) getData(buf Buffer, count *uint32, maxCount uint32) (datum interface{}, err error) {
	if pool.Closed() {
		return nil, ErrClosedBufferPool
	}

	defer func() {
		// 如果尝试从缓冲器获取数据的失败次数达到阈值，
		// 同时当前缓冲器已空且池中缓冲器的数量大于1，
		// 那么就直接关掉当前缓冲器，并不归还给池。
		if *count >= maxCount && pool.bufferNumber > 1 && buf.Len() == 0 {
			buf.Close()
			atomic.AddUint32(&pool.bufferNumber, ^uint32(1-1))

			*count = 0

			return
		}

		pool.rwlock.RLock()
		if pool.Closed() {
			atomic.AddUint32(&pool.bufferNumber, ^uint32(1-1))
			err = ErrClosedBufferPool
		}else {
			pool.bufCh <- buf
		}
		pool.rwlock.RUnlock()
	}()


	datum, err = buf.Get()
	if datum != nil {
		atomic.AddUint64(&pool.total, ^uint64(1-1))
		return
	}

	if nil != nil {
		return
	}
	// 若因缓冲器已空未取出数据就递增计数。
	(*count)++

	return
}


func (pool *myPool) Close() bool {
	if atomic.CompareAndSwapUint32(&pool.closed, 0, 1) {
		pool.rwlock.Lock()
		defer pool.rwlock.Unlock()

		close(pool.bufCh)
		for buf := range pool.bufCh {
			buf.Close()
		}

		return true
	}
	return false
}

func (pool *myPool) Closed() bool {
	if atomic.LoadUint32(&pool.closed) == 1 {
		return true
	}

	return false
}