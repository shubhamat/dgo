/* (c) 2017  Shubham Mankhand  <shubham.mankhand@gmail.com> */
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

  go eat()

  go moo()

  go discover()

  go beDiscovered()

  if *launchSow {
      go sow()
  }

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

  herdwqmap = make(map[string]int, len(cows))
}

/*
 * Discover new cows.
 */
func discover() {
  fmt.Println("[DISCOVER:" + myip + "] Launched thread")
  svc := "0.0.0.0" + port
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
        fmt.Println("DISCOVER read error")
        time.Sleep(time.Second)
        continue
      }
      newcowaddr := cowaddr.IP.String()
      if newcowaddr == myip {
          continue
      }

      found := false
      for i := 0; i < len(cows); i++ {
        if cows[i] == newcowaddr {
          found = true
          break;
        }
      }

      if !found {
        cows = append(cows, newcowaddr)
        fmt.Printf("[DISCOVER:%s] Adding new cow %s. Total cows in herd %d\n", myip, newcowaddr, 1 + len(cows))
        herdwqmap[newcowaddr] = 0
        go wander(newcowaddr)
      }
  }
}

/*
 * Let other cows know you exist
 */
func beDiscovered() {
  fmt.Println("[BEDISCOVERED:" + myip + ":"  + broadcast + "] Launched thread")
  for {
      svc := broadcast + port
      addr, err := net.ResolveUDPAddr("udp4", svc)
      if err != nil {
        fmt.Println("BEDISCOVERED resolve error")
        time.Sleep(time.Second)
        continue
      }

      conn, err := net.DialUDP("udp", nil, addr)
      if err != nil {
        fmt.Println("BEDISCOVERED dial error")
        time.Sleep(time.Second)
        continue
      }
      conn.Write([]byte("cow"))
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
      /* Ignore error for now */
      err = client.Call("CowRPC.GetQueueLen", &notUsed, &qlen)
      herdwqmap[cowip] = qlen
      time.Sleep(time.Second)
  }
}

/*
 * Get work off another cow's queue
 */
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
