package main

import (
  "fmt"
  "os"
  "container/list"
  "math/rand"
  "time"
  "sync"
  "net"
  "net/http"
  "net/rpc"
  "flag"
)


type WorkItem struct {
  Duration  int
  Cost      int
}

type workQueue struct {
  mutex   sync.Mutex
  list    list.List
}

/* For RPC */
type ArgsNotUsed    int
type CowRPC         int

const maxWorkDuration =   31
const maxSowDuration =    11
const maxCost =           101

const port = ":23432";

var allcows = []string {
    "192.168.0.31",
    "192.168.0.32",
    "192.168.0.33",
    "192.168.0.34" }

var myip string;

var cows []string;

var herdwqmap  map[string]int;

var wq = workQueue{}

var launchSow =  flag.Bool("sow",  false,  "Start sow thread")

func main() {

  flag.Parse()

  myip = flag.Arg(0)

  if myip == "" {
    fmt.Println("You most specify IP address.  Usage:  cow <IP_Address>")
    os.Exit(1)
  }

  fmt.Printf("Initializing cow:%s ...\n", myip)

  cows = make([]string, len(allcows) - 1)

  fn := 0

  for i,j := 0, 0; i < len(allcows); i++ {

    if allcows[i] == myip {
       fn = 1
    } else if j < len(cows) {
        cows[j] = allcows[i]
        j++
     }
   }

   if fn != 1 {
     fmt.Printf("%s does not exist in the list of known cows. Enter a known IP.\n", myip)
     os.Exit(1)
   }

  herdwqmap = make(map[string]int, len(cows))
  for i := 0; i < len(cows); i++ {
    herdwqmap[cows[i]] = 0
  }

  // Launch the eat thread
  go eat()

  go moo()


  for i := 0; i < len(cows); i++ {
    go wander(cows[i])
  }

  // Launch the sow thread. TBD:  Add a flag that controls whether this thread is launched or not
  if *launchSow {
      go sow()
  }

    /* Wait for other threads to finish.  Need to call wait() equivalent here*/
  for {
    time.Sleep(time.Second)
  }

}


func  dequeue() WorkItem {
    work := WorkItem{}
    wq.mutex.Lock()
    e := wq.list.Front()
    if e == nil {
      wq.mutex.Unlock()
      go forage()
      return work
    }
    work = e.Value.(WorkItem)
    wq.list.Remove(e)
    wq.mutex.Unlock()
    return work
}

func (t *CowRPC) GetQueueLen(_ *ArgsNotUsed, reply *int) error {
  *reply = wq.list.Len()
  return nil
}

func (t *CowRPC) GetWorkItem(_ *ArgsNotUsed, reply *WorkItem) error {
  *reply = dequeue()
  return nil
}

func eat()  {
  fmt.Println("Launched eat thread for " + myip)

  for  {
    work := dequeue()
    if work == (WorkItem{}) {
          time.Sleep(time.Second)
    }
    //fmt.Printf("Processing work of Cost:%d Duration:%d for %s...\n", work.Cost, work.Duration, myip)
    fmt.Printf("[EAT:%s] Processing work of Duration:%d\n", myip, work.Duration)
    time.Sleep(time.Second * time.Duration(work.Duration))
  }

}

func sow() {
  fmt.Println("Launched sow thread for " + myip)

  for  {
     /* Sleep for a random time */
     sleep_time := rand.Intn(maxSowDuration)
     fmt.Printf("[SOW:%s] Sleeping for %d seconds\n", myip, sleep_time)
     time.Sleep(time.Second * time.Duration(sleep_time))
     Duration := rand.Intn(maxWorkDuration)
     Cost := rand.Intn(maxCost)
     work := WorkItem{Duration, Cost}
     fmt.Printf("[SOW:%s] Adding work item (Duration = %d)\n", myip, work.Duration)
     wq.mutex.Lock()
     wq.list.PushBack(work)
     wq.mutex.Unlock()
  }
}


func moo() {
  cowrpc := new(CowRPC)
  rpc.Register(cowrpc)
  rpc.HandleHTTP()
  listener, err := net.Listen("tcp", port)
  if err != nil {
      fmt.Fprintln(os.Stderr, err)
      os.Exit(3)
  }

  fmt.Println("[MOO:" + myip + " Starting HTTP Server for RPC")
  go http.Serve(listener, nil)
}


/*
 * Wander and fetch the queue len for the given cow.
 * One thread for each cow in cows[]
 */
func wander(cowip string)  {
  fmt.Println("Launched wander thread for " + myip + ":" + cowip)

  for {
      client, err := rpc.DialHTTP("tcp",cowip + port)
      if err != nil {
          time.Sleep(time.Second * 2)
          continue
      }
      qlen := 0
      notUsed := 0
      err = client.Call("CowRPC.GetQueueLen", &notUsed, &qlen)

      if qlen != 0 {
        herdwqmap[cowip] = qlen
      }

      // Ignore error
      fmt.Printf("[WANDER:%s]  Work queue len for %s is %d\n", myip, cowip, herdwqmap[cowip])
      time.Sleep(time.Second)
  }
}

func  forage() {
    var max int  =  herdwqmap[cows[0]]
    var maxcowip string = cows[0]

    for i := 1; i < len(cows); i++ {
       if  max < herdwqmap[cows[i]] {
            max  = herdwqmap[cows[i]]
            maxcowip = cows[i]
        }
    }

    client, err := rpc.DialHTTP("tcp",maxcowip + port)
    if err != nil {
        return
    }
    var work WorkItem
    notUsed := 0
    err = client.Call("CowRPC.GetWorkItem", &notUsed, &work)

    if work != (WorkItem{}) {
        fmt.Printf("[FORAGE:%s] Added work from %s\n", myip, herdwqmap[maxcowip])
        wq.list.PushBack(work)
    }
}

