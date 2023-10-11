package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorStatus int

const (
	Idle CoordinatorStatus = iota
	Mapping
	Reducing
)

type TaskStatus int

const (
	Unassigned TaskStatus = iota
	Excuting
	Finished
)

type mapTask struct {
	status   TaskStatus
	fileName string
}

type reduceTask struct {
	status TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	status CoordinatorStatus
	mux    sync.Mutex

	MapTaskPool []mapTask
	MapCounter  int
	MapLock     sync.Mutex

	NReduce        int
	ReduceTaskPool []reduceTask
	ReduceCounter  int
	ReduceLock     sync.Mutex
}

func (c *Coordinator) assignMapTask() (string, int) {
	c.MapLock.Lock()
	defer c.MapLock.Unlock()

	for i := range c.MapTaskPool {
		if c.MapTaskPool[i].status == Unassigned {
			c.MapTaskPool[i].status = Excuting

			time.AfterFunc(10*time.Second, func() {
				c.MapLock.Lock()
				defer c.MapLock.Unlock()

				if c.MapTaskPool[i].status == Excuting {
					c.MapTaskPool[i].status = Unassigned
				}
			})

			return c.MapTaskPool[i].fileName, i
		}
	}
	return "", -1
}

func (c *Coordinator) assignReduceTask() int {
	c.ReduceLock.Lock()
	defer c.ReduceLock.Unlock()

	for i := range c.ReduceTaskPool {
		if c.ReduceTaskPool[i].status == Unassigned {
			c.ReduceTaskPool[i].status = Excuting

			time.AfterFunc(10*time.Second, func() {
				c.MapLock.Lock()
				defer c.MapLock.Unlock()

				if c.ReduceTaskPool[i].status == Excuting {
					c.ReduceTaskPool[i].status = Unassigned
				}
			})

			return i
		}
	}
	return -1
}

// AssignTask 分配任务
func (c *Coordinator) AssignTask(_ *TaskArgs, reply *Task) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	reply.NMap = len(c.MapTaskPool)
	reply.NReduce = c.NReduce

	switch c.status {
	case Mapping:
		reply.TaskType = MapTask
		reply.MapFilename, reply.MapId = c.assignMapTask()
		reply.IsAvailable = reply.MapId != -1
	case Reducing:
		reply.TaskType = ReduceTask
		reply.ReduceId = c.assignReduceTask()
		reply.IsAvailable = reply.ReduceId != -1
	}
	return nil
}

// FinishTask 任务完成回调
func (c *Coordinator) FinishTask(args *TaskArgs, _ *Task) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	var err error
	switch args.TaskType {
	case MapTask:
		err = func() error {
			c.MapLock.Lock()
			defer c.MapLock.Unlock()
			if c.status == Mapping && c.MapTaskPool[args.MapId].status == Excuting {
				c.MapTaskPool[args.MapId].status = Finished
				c.MapCounter++
				if c.MapCounter == len(c.MapTaskPool) {
					c.status = Reducing
				}
				return nil
			}
			return errors.New("map phase has been completed")
		}()
	case ReduceTask:
		err = func() error {
			c.ReduceLock.Lock()
			defer c.ReduceLock.Unlock()
			if c.status == Reducing && c.ReduceTaskPool[args.ReduceId].status == Excuting {
				c.ReduceTaskPool[args.ReduceId].status = Finished
				c.ReduceCounter++
				if c.ReduceCounter == len(c.ReduceTaskPool) {
					c.status = Idle
				}
				return nil
			}
			return errors.New("map phase has been completed")
		}()
	}
	return err
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done 判断任务是否完成
func (c *Coordinator) Done() bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.status == Idle
}

// MakeCoordinator 创建协调器
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.status = Mapping
	for _, v := range files {
		c.MapTaskPool = append(c.MapTaskPool, mapTask{Unassigned, v})
	}
	c.MapCounter = 0
	c.NReduce = nReduce
	c.ReduceTaskPool = make([]reduceTask, nReduce)
	c.ReduceCounter = 0
	c.server()
	return &c
}
