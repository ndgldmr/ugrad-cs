package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "io/ioutil"
import "os"
import "sort"
import "time"

type KeyValue struct {
	Key   string
	Value string
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		for {
			arguments := RequestTaskReply{}
			reply := RequestTaskReply{}
			res := call("Coordinator.RequestTask", &arguments, &reply)
			if !res {
				break
			}
			switch reply.Task.Task {
			case Map:
				doMap(&reply, mapf)
			case Reduce:
				doReduce(&reply, reducef)
			case Wait:
				time.Sleep(1 * time.Second)
			case Exit:
				os.Exit(0)
			default:
				time.Sleep(1 * time.Second)
			}
		}
}

func doReduce(reply *RequestTaskReply, reducef func(string, []string) string) {
    intermediate := []KeyValue{}
    for _, inputFile := range reply.Task.InputFiles {
        file, err := os.Open(inputFile)
        if err != nil {
            log.Fatalf("cannot open %v", inputFile)
        }
        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err := dec.Decode(&kv); err != nil {
                break 
            }
            intermediate = append(intermediate, kv)
        }
        file.Close() 
    }
    sort.Slice(intermediate, func(i, j int) bool {
        return intermediate[i].Key < intermediate[j].Key
    })
    oname := fmt.Sprintf("mr-out-%d", reply.Task.Index)
    ofile, err := ioutil.TempFile("", oname)
    if err != nil {
        log.Fatalf("cannot create temp file for %v", oname)
    }
    for i := 0; i < len(intermediate); {
        j := i + 1
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        values := make([]string, j-i)
        for k := i; k < j; k++ {
            values[k-i] = intermediate[k].Value
        }
        output := reducef(intermediate[i].Key, values)
        fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
        i = j 
    }
    ofile.Close()
    if err := os.Rename(ofile.Name(), oname); err != nil {
        log.Fatalf("cannot rename temp file to %v", oname)
    }
    reply.Task.Status = Finished
    replyEx := RequestTaskReply{}
    call("Coordinator.NotifyComplete", reply, &replyEx)
}


func doMap(reply *RequestTaskReply, mapf func(string, string) []KeyValue) {
    file, err := os.Open(reply.Task.InputFiles[0])
    if err != nil {
        log.Fatalf("cannot open %v", reply.Task.InputFiles[0])
    }
    content, err := ioutil.ReadAll(file)
    file.Close() 
    if err != nil {
        log.Fatalf("cannot read %v", reply.Task.InputFiles[0])
    }
    kva := mapf(reply.Task.InputFiles[0], string(content))
    intermediate := make([][]KeyValue, reply.NReduce)
    for _, kv := range kva {
        r := ihash(kv.Key) % reply.NReduce
        intermediate[r] = append(intermediate[r], kv)
    }
    for r, kvs := range intermediate {
        oname := fmt.Sprintf("mr-%d-%d", reply.Task.Index, r)
        ofile, err := ioutil.TempFile("", oname)
        if err != nil {
            log.Fatalf("cannot create temp file %v", oname)
        }
        enc := json.NewEncoder(ofile)
        for _, kv := range kvs {
            if err := enc.Encode(&kv); err != nil {
                log.Fatalf("cannot encode kv: %v", err)
            }
        }
        ofile.Close()
        if err := os.Rename(ofile.Name(), oname); err != nil {
            log.Fatalf("cannot rename file %v to %v: %v", ofile.Name(), oname, err)
        }
    }
    reply.Task.Status = Finished
    replyEx := RequestTaskReply{}
    call("Coordinator.NotifyComplete", reply, &replyEx)
}

func CallExample() {
	args := ExampleArgs{}
	args.X = 99
	reply := ExampleReply{}
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func callStringReverse() {
	args := StringReverseArgs{Str: "ABCDE"}
	reply := StringReverseReply{}
	ok := call("Coordinator.StringReverse", &args, &reply)
	if !ok {
    	log.Fatalf("Failed to call Coordinator.StringReverse")
	} else {
    	fmt.Printf("Worker: %s\n", args.Str)
	}
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}