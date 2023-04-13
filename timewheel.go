package timewheel

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type TimeWheel struct {
	taskSet        []*task
	mu             sync.Mutex
	tickCount      int32
	timeOfOnceTick time.Duration // 最小刻度 1ms
	curTickIndex   int32
	ctx            context.Context
	cancel         context.CancelFunc
}

type task struct {
	next    *task
	f       func()
	timeOut time.Duration
	circle  bool
	deleted bool
}

// NewTimeWheel timeOfOnceTick 最小刻度1ms
func NewTimeWheel(tickCount int32, timeOfOnceTick time.Duration) *TimeWheel {
	ctx, cancel := context.WithCancel(context.Background())
	tw := &TimeWheel{
		taskSet:        make([]*task, tickCount),
		tickCount:      tickCount,
		timeOfOnceTick: timeOfOnceTick,
		curTickIndex:   0,
		ctx:            ctx,
		cancel:         cancel,
	}
	return tw
}

func (tw *TimeWheel) Start() {
	go tw.run()
}

func (tw *TimeWheel) run() {
	timer := time.NewTicker(tw.timeOfOnceTick)
	defer timer.Stop()
	var head *task
	for {
		select {
		case <-tw.ctx.Done():
			return
		case <-timer.C:
		}
		tw.mu.Lock()
		curTickIndex := atomic.LoadInt32(&tw.curTickIndex)
		head = tw.taskSet[curTickIndex]
		tw.taskSet[curTickIndex] = nil
		for head != nil {
			cur := head
			head = head.next
			if cur.deleted {
				continue
			}
			go func() {
				cur.f()
				if cur.circle { // 重新插入集合，需要在当前任务执行之后插入
					tw.insert(cur, tw.calTickIndex(cur.timeOut))
				}
			}()
		}
		atomic.StoreInt32(&tw.curTickIndex, (curTickIndex+1)%tw.tickCount)
		tw.mu.Unlock()
	}
}

func (tw *TimeWheel) calTickIndex(timeOut time.Duration) int {
	curTick := atomic.LoadInt32(&tw.curTickIndex)
	spinTickNum := int32(math.Floor(float64(timeOut / tw.timeOfOnceTick)))
	insertTickIndex := int((curTick + spinTickNum) % tw.tickCount)
	return insertTickIndex
}

func (tw *TimeWheel) Stop() {
	tw.cancel()
}

// AddTaskAfter timeOut毫秒
func (tw *TimeWheel) AddTaskAfter(timeOut time.Duration, circle bool, f func()) *task {
	t := &task{
		f:       f,
		timeOut: timeOut,
		circle:  circle,
		deleted: false,
	}
	tw.insert(t, tw.calTickIndex(timeOut))
	return t
}

func (tw *TimeWheel) insert(t *task, insertTickIndex int) {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if tw.taskSet[insertTickIndex] == nil {
		//fmt.Println("insert first", insertTickIndex)
		tw.taskSet[insertTickIndex] = t
	} else {
		//fmt.Println("insert after", insertTickIndex)
		t.next = tw.taskSet[insertTickIndex]
		tw.taskSet[insertTickIndex] = t
	}
}

func (tw *TimeWheel) Remove(t *task) {
	t.deleted = true
}