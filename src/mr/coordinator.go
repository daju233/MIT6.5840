package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var TIMETEST time.Time

var GlobalReduceNum int
var TaskId int
var FilesLen int

type TaskStatus int

const (
	StatusIdle TaskStatus = iota
	StatusInProgress
	StatusCompleted
)

type poolStatus int

const (
	PHASEMAP poolStatus = iota
	PHASEREDUCE
	PHASEDONE
)

type taskType string

const (
	MAP    taskType = "MAP"
	REDUCE taskType = "REDUCE"
	NONE   taskType = "NONE"
	WAIT   taskType = "WAIT"
	DONE   taskType = "DONE"
)

type TaskInfo struct {
	Task   *Task
	Time   time.Time
	Status TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	mutex                sync.Mutex
	TaskInfoManager      TaskInfoManager
	phase                poolStatus
	nMapTasks            int
	nReduceTasks         int
	MapCh                chan *Task
	ReduceCh             chan *Task
	MapNum               int
	allMapTaskDispatched bool
	//Thanks for gemini2.5!
	generateJobId func() int
}

type Task struct {
	Files    []string
	TaskType taskType
	TaskID   int
	Nreduce  int
}

type TaskInfoManager struct {
	InfoMap map[int]*TaskInfo
}

func (c *Coordinator) monirtorTasks() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mutex.Lock()
		fmt.Println(time.Since(TIMETEST))
		for _, value := range c.TaskInfoManager.InfoMap {
			if time.Since(value.Time) >= time.Second*10 && value.Status == StatusInProgress {
				if value.Task.TaskType == MAP {
					value.Time = time.Now()
					value.Status = StatusIdle
					c.MapCh <- value.Task
					fmt.Printf("MAP任务%d超时\n", value.Task.TaskID)
				} else if value.Task.TaskType == REDUCE {
					value.Time = time.Now()
					value.Status = StatusIdle
					c.ReduceCh <- value.Task
					fmt.Printf("REDUCE任务%d超时\n", value.Task.TaskID)

				}
			}
		}
		c.mutex.Unlock()
	}
}

func (j *TaskInfoManager) taskDone(taskID int) {
	// fmt.Printf("任务编号！%d", taskID)
	j.InfoMap[taskID].Status = StatusCompleted
	j.InfoMap[taskID].Time = time.Now()
}

func (c *Coordinator) nextPhase() {
	if c.phase == poolStatus(PHASEMAP) {
		c.makeReduce()
		c.phase = poolStatus(PHASEREDUCE)
	} else if c.phase == poolStatus(PHASEREDUCE) {
		c.phase = poolStatus(PHASEDONE)
	}
}

func (c *Coordinator) makeReduce() {
	for i := 0; i < GlobalReduceNum; i++ {
		pattern := fmt.Sprintf("mr-tmp-*-%d", i)
		files, _ := filepath.Glob(pattern)

		var task Task = Task{
			Files:    files,
			TaskType: taskType(REDUCE),
			TaskID:   i,
			Nreduce:  GlobalReduceNum,
		}
		var taskinfo TaskInfo = TaskInfo{
			Task:   &task,
			Status: StatusIdle,
		}
		c.TaskInfoManager.putTask(&taskinfo)
		// log.Printf("Coordinator: 创建 Map 任务 (MapID %d) 文件: %v\n", task.TaskID, task.Files)
		c.ReduceCh <- &task
	}

	// log.Printf("Coordinator: 所有 %d 个 Reduce 任务已放入通道\n", c.nReduceTasks)
}
func (c *Coordinator) makeMap(files []string, nReduce int) {
	GlobalReduceNum = nReduce
	TaskId = nReduce

	for _, filename := range files {
		var task Task = Task{
			Files:    []string{filename},
			TaskType: taskType(MAP),
			TaskID:   c.generateJobId(),
			Nreduce:  nReduce,
		}
		var taskinfo TaskInfo = TaskInfo{
			Task:   &task,
			Status: StatusIdle,
		}
		c.TaskInfoManager.putTask(&taskinfo)

		// log.Printf("Coordinator: 创建 Map 任务 (MapID %d) 文件: %s\n", task.TaskID, task.Files[0])
		c.MapCh <- &task
	}
	// log.Printf("Coordinator: 所有 %d 个 Map 任务已放入通道\n", len(files))
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) MapHandler(request *MyRequest, response *MyResponse) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if request.Result == TaskResult(MAPSUCCESS) || request.Result == TaskResult(REDUCESUCCESS) {
		c.TaskInfoManager.taskDone(request.TaskID)
		response.Task = &Task{
			TaskType: WAIT,
		}
		return nil
	}
	if c.phase == poolStatus(PHASEMAP) {
		if len(c.MapCh) > 0 {
			task := <-c.MapCh
			response.Task = task
			c.TaskInfoManager.fireThetask(task.TaskID)

		} else {
			response.Task = &Task{
				TaskType: taskType(WAIT),
			}
			if c.checkTaskDone() {
				c.nextPhase()
			}
			// log.Printf("没有MAP了！变身REDUCE！")
		}
		return nil
	} else if c.phase == poolStatus(PHASEREDUCE) {
		if len(c.ReduceCh) > 0 {
			task := <-c.ReduceCh
			response.Task = task
			c.TaskInfoManager.fireThetask(task.TaskID)
		} else {
			if c.checkTaskDone() {
				c.nextPhase()
				// log.Printf("没有REDUCE了，都几把完成了！")
				response.Task = &Task{
					TaskType: taskType(DONE),
				}
			}
		}
		return nil
	} else {
		response.Task = &Task{
			TaskType: taskType(DONE),
		}
	}

	return nil
}

func (j *TaskInfoManager) putTask(taskinfo *TaskInfo) bool {
	taskId := taskinfo.Task.TaskID
	meta := j.InfoMap[taskId]
	if meta != nil {
		return false
	} else {
		// fmt.Println("数据管理器已存入任务信息，id为", taskId)
		j.InfoMap[taskId] = taskinfo
	}
	return true
}

func (j *TaskInfoManager) fireThetask(taskID int) bool {
	taskinfo := j.InfoMap[taskID]
	if taskinfo.Status != StatusIdle {
		return false
	} //任务进行中
	// println("已发送map任务", taskID)
	taskinfo.Status = StatusInProgress
	taskinfo.Time = time.Now()
	return true
}

func (c *Coordinator) checkTaskDone() bool {
	reduceDoneNum := 0
	reduceUnDoneNum := 0
	mapDoneNum := 0
	mapUnDoneNum := 0
	for _, v := range c.TaskInfoManager.InfoMap {
		if v.Task.TaskType == MAP {
			if v.Status == StatusCompleted {
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else {
			if v.Status == StatusCompleted {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	if c.phase == PHASEMAP {
		return mapUnDoneNum == 0 && mapDoneNum > 0
	} else if c.phase == PHASEREDUCE {
		return reduceUnDoneNum == 0 && reduceDoneNum > 0
	}
	return false
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	if c.phase == poolStatus(PHASEDONE) {
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// log.Printf("Coordinator: 初始化开始，%d 个输入文件, %d 个 Reduce 任务\n", len(files), nReduce)
	FilesLen = len(files)
	generateJobIdFunc := func() int {
		var temp int = TaskId
		TaskId++
		return temp
	}
	c := Coordinator{
		phase:                PHASEMAP,
		nMapTasks:            len(files),
		nReduceTasks:         nReduce,
		MapCh:                make(chan *Task, len(files)),
		ReduceCh:             make(chan *Task, nReduce),
		generateJobId:        generateJobIdFunc,
		allMapTaskDispatched: false,
		TaskInfoManager: TaskInfoManager{
			InfoMap: make(map[int]*TaskInfo),
		},
	}

	c.allMapTaskDispatched = true
	c.makeMap(files, nReduce)
	// Your code here.
	go c.monirtorTasks()
	TIMETEST = time.Now()
	c.server()
	return &c
}
