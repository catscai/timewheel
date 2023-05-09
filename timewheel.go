package timewheel

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type TimeWheel struct {
	taskSet        []*Task
	mu             sync.Mutex
	tickCount      int32
	timeOfOnceTick time.Duration // 最小刻度 1ms
	curTickIndex   int32
	ctx            context.Context
	cancel         context.CancelFunc
	started        bool
	sync           bool
}

type Task struct {
	next    *Task
	f       func()
	timeOut time.Duration
	circle  bool
	deleted bool
}

// NewTimeWheel timeOfOnceTick 最小刻度1ms
func NewTimeWheel(tickCount int32, timeOfOnceTick time.Duration) *TimeWheel {
	tw := &TimeWheel{
		tickCount:      tickCount,
		timeOfOnceTick: timeOfOnceTick,
		started:        false,
		sync:           false,
	}
	return tw
}

// NewTimeWheelSync 处理任务时 同步处理，不再创建新的协程
func NewTimeWheelSync(tickCount int32, timeOfOnceTick time.Duration) *TimeWheel {
	tw := &TimeWheel{
		tickCount:      tickCount,
		timeOfOnceTick: timeOfOnceTick,
		started:        false,
		sync:           true,
	}
	return tw
}

func (tw *TimeWheel) Start() {
	tw.reStart()
}

func (tw *TimeWheel) reStart() {
	if tw == nil {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	if !tw.started {
		ctx, cancel := context.WithCancel(context.Background())
		tw.ctx, tw.cancel = ctx, cancel
		tw.curTickIndex, tw.started = 0, true
		tw.taskSet = make([]*Task, tw.tickCount)
		go tw.run()
	}
}

func (tw *TimeWheel) run() {
	timer := time.NewTicker(tw.timeOfOnceTick)
	defer timer.Stop()
	var head *Task
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
		atomic.StoreInt32(&tw.curTickIndex, (curTickIndex+1)%tw.tickCount)
		tw.mu.Unlock()

		for head != nil {
			cur := head
			head = head.next
			if cur.deleted {
				continue
			}
			if tw.sync {
				cur.f()
				if cur.circle { // 重新插入集合，需要在当前任务执行之后插入
					tw.mu.Lock()
					tw.insert(cur, tw.calTickIndex(cur.timeOut))
					tw.mu.Unlock()
				}
				continue
			}
			go func() {
				cur.f()
				if cur.circle { // 重新插入集合，需要在当前任务执行之后插入
					tw.mu.Lock()
					defer tw.mu.Unlock()
					tw.insert(cur, tw.calTickIndex(cur.timeOut))
				}
			}()
		}

	}
}

func (tw *TimeWheel) calTickIndex(timeOut time.Duration) int {
	curTick := atomic.LoadInt32(&tw.curTickIndex)
	spinTickNum := int32(math.Floor(float64(timeOut / tw.timeOfOnceTick)))
	insertTickIndex := int((curTick + spinTickNum) % tw.tickCount)
	return insertTickIndex
}

func (tw *TimeWheel) Stop() {
	if tw == nil {
		return
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.cancel()
	tw.started = false
}

// AddTaskAfter timeOut毫秒
func (tw *TimeWheel) AddTaskAfter(timeOut time.Duration, circle bool, f func()) *Task {
	if tw == nil {
		return nil
	}
	t := &Task{
		f:       f,
		timeOut: timeOut,
		circle:  circle,
		deleted: false,
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.insert(t, tw.calTickIndex(timeOut))
	return t
}

func (tw *TimeWheel) insert(t *Task, insertTickIndex int) {
	if tw.taskSet[insertTickIndex] == nil {
		tw.taskSet[insertTickIndex] = t
	} else {
		t.next = tw.taskSet[insertTickIndex]
		tw.taskSet[insertTickIndex] = t
	}
}

func (tw *TimeWheel) Remove(t *Task) {
	if tw == nil {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()
	t.deleted = true
}
