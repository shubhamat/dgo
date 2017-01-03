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
  Origin    int
}

type workQueue struct {
  mutex     sync.Mutex
  list      list.List
}

/* For RPC */
type ArgsNotUsed    int
type CowRPC         int

const maxWorkDuration =   31
const maxSowDuration =    16
const maxCost =           101

const port = ":23432";

const (
        ORIGIN_LOCAL  = 1
        ORIGIN_REMOTE = 2
)

var allcows = []string {
    "192.168.0.30",
    "192.168.0.31",
    "192.168.0.32",
    "192.168.0.33",
    "192.168.0.34" }

var myip string
var myipaddr *net.IPNet
var broadcast string

var cows []string
var herdwqmap  map[string]int
var wq = workQueue{}

var launchSow =   flag.Bool("sow",  false,  "Start sow thread")
var iface     =   flag.String("iface", "wlan0",  "Interface used for sending data")

func main() {

  initAll()

  // Launch the eat thread
  go eat()

  go moo()


  for i := 0; i < len(cows); i++ {
    go wander(cows[i])
  }

  go discover()
  go beDiscovered()

  // Launch the sow thread. TBD:  Add a flag that controls whether this thread is launched or not
  if *launchSow {
      go sow()
  }

    /* Wait for other threads to finish.  Need to call wait() equivalent here*/
  for {
    time.Sleep(time.Second)
  }

}

func initAll() {
  flag.Parse()

  myiface, err := net.InterfaceByName(*iface)

  if err != nil {
    fmt.Fprintf(os.Stderr, "Error in locating interface:%s\n%s\n", *iface, err)
    os.Exit(1)
  }

  addresses, err := myiface.Addrs()
  if err != nil {
    fmt.Fprintf(os.Stderr, "Error in getting address for interface:%s\n%s\n", *iface, err)
    os.Exit(1)
  }

  for _, addr := range addresses {
   if ipaddr, ok := addr.(*net.IPNet); ok && !ipaddr.IP.IsLoopback() {
     if ipaddr.IP.To4() != nil {
       myip = ipaddr.IP.String()
       myipaddr = ipaddr
     }
   }
  }

  ip := myipaddr.IP.To4()
  mask := myipaddr.Mask
  bcast := make(net.IP, len(ip))
  for i := range bcast {
    bcast[i] = ip[i] | ^mask[i]
  }

  broadcast  = fmt.Sprintf("%s", bcast)

  if myip == "" {
    fmt.Println("You most specify an interface.  Usage:  cow -iface <InterfaceName>")
    os.Exit(1)
  }

  fmt.Printf("Initializing cow:%s..., Looking for other cows on:%s\n", myip, broadcast)

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

}

func discover() {
  fmt.Println("[DISCOVER:" + myip + "] Launched thread")
  svc := myip + port
  addr, err := net.ResolveUDPAddr("udp4", svc)
  if err != nil {
      fmt.Fprintln(os.Stderr, err)
      os.Exit(1)
  }
  conn, err := net.ListenUDP("udp", addr)
  if err != nil {
      fmt.Fprintln(os.Stderr, err)
      os.Exit(1)
  }

  for {
      var buf [64]byte
      _, cowaddr, err := conn.ReadFromUDP(buf[0:])
      if err != nil {
        time.Sleep(time.Second)
        continue
      }

     fmt.Println("Got ping from " + cowaddr.String())
  }
}

func beDiscovered() {
  fmt.Println("[BEDISCOVERED:" + myip + ":"  + broadcast + "] Launched thread")
  for {
      svc := broadcast + port
      addr, err := net.ResolveUDPAddr("udp4", svc)
      if err != nil {
        time.Sleep(time.Second)
        continue
      }
      svc = myip + port
      localaddr, err := net.ResolveUDPAddr("udp4", svc)
      if err != nil {
        time.Sleep(time.Second)
        continue
      }
      conn, err := net.DialUDP("udp", localaddr, addr)
      if err != nil {
        time.Sleep(time.Second)
        continue
      }
      conn.Write([]byte("cow"))
      conn.Close()
      time.Sleep(time.Second)
  }
}

func  dequeue() WorkItem {
    wq.mutex.Lock()
    e := wq.list.Front()
    if e == nil {
      wq.mutex.Unlock()
      go forage()
      return WorkItem{}
    }
    work := e.Value.(WorkItem)
    wq.list.Remove(e)
    wq.mutex.Unlock()
    return work
}

func  dequeueLocal() WorkItem {
    wq.mutex.Lock()
    e := wq.list.Front()
    if e == nil {
      wq.mutex.Unlock()
      return WorkItem{}
    }
    work := e.Value.(WorkItem)
    if work.Origin == ORIGIN_LOCAL {
        wq.list.Remove(e)
    } else {
        work = WorkItem{}
    }
    wq.mutex.Unlock()
    return work
}


func (t *CowRPC) GetQueueLen(_ *ArgsNotUsed, reply *int) error {
  *reply = wq.list.Len()
  return nil
}

func (t *CowRPC) GetWorkItem(_ *ArgsNotUsed, reply *WorkItem) error {
  *reply = dequeueLocal()
  return nil
}

func eat()  {
  fmt.Println("[EAT:" + myip + "] Launched thread")

  for  {
    work := dequeue()
    if work == (WorkItem{}) {
          time.Sleep(time.Second)
      } else {
          //fmt.Printf("Processing work of Cost:%d Duration:%d for %s...\n", work.Cost, work.Duration, myip)
          fmt.Printf("[EAT:%s qlen:%d] Processing work of Duration:%d\n", myip, wq.list.Len(), work.Duration)
          time.Sleep(time.Second * time.Duration(work.Duration))
      }
  }

}

func sow() {
  fmt.Println("[SOW:" + myip + "] Launched thread")

  for  {
     /* Sleep for a random time */
     sleep_time := rand.Intn(maxSowDuration)
     //fmt.Printf("[SOW:%s] Sleeping for %d seconds\n", myip, sleep_time)
     time.Sleep(time.Second * time.Duration(sleep_time))
     Duration := rand.Intn(maxWorkDuration)
     Cost := rand.Intn(maxCost)
     work := WorkItem{Duration, Cost, ORIGIN_LOCAL}
     wq.mutex.Lock()
     wq.list.PushBack(work)
     wq.mutex.Unlock()
     fmt.Printf("[SOW:%s qlen:%d] Added work item (Duration = %d)\n", myip, wq.list.Len(), work.Duration)
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

  fmt.Println("[MOO:" + myip + "] Starting HTTP Server for RPC")
  go http.Serve(listener, nil)
}


/*
 * Wander and fetch the queue len for the given cow.
 * One thread for each cow in cows[]
 */
func wander(cowip string)  {
  fmt.Println("[WANDER:" + myip + "] Launched thread for " + cowip)

  for {
      client, err := rpc.DialHTTP("tcp",cowip + port)
      if err != nil {
          time.Sleep(time.Second * 2)
          continue
      }
      qlen := 0
      notUsed := 0
      err = client.Call("CowRPC.GetQueueLen", &notUsed, &qlen)
      herdwqmap[cowip] = qlen

      // Ignore error
      // fmt.Printf("[WANDER:%s]  Work queue len for %s is %d\n", myip, cowip, herdwqmap[cowip])
      time.Sleep(time.Second)
  }
}

func  forage() {

    if len(cows) < 1 {
      return
    }

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
        fmt.Printf("[FORAGE:%s] Added work from %s, qlen:%d\n", myip, maxcowip, max)
        work.Origin = ORIGIN_REMOTE
        wq.list.PushBack(work)
    }
}
