package main

import (
  "fmt"
  "os"
  "strings"
  "container/list"
  "math/rand"
  "time"
  "sync"
  "net"
)


type workItem struct {
  duration  int
  cost      int
}

type workQueue struct {
  mutex   sync.Mutex
  list    list.List
}



const maxWorkDuration = 31
const maxSowDuration = 11
const maxCost = 101

const port = ":23432";

var allcows = []string {
    "192.168.0.31",
    "192.168.0.32",
    "192.168.0.33",
    "192.168.0.34" }

var myip string;

var cows []string;

var wq = workQueue{}

var launchSow int = 1

func main() {

  myip = strings.Join(os.Args[1:], "")

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


  fmt.Println("Other cows in herd:")
  for i := 0; i < len(cows); i++ {
    fmt.Println(cows[i])
  }

  // Launch the eat thread
  go eat()

  go moo()

  // Launch the sow thread. TBD:  Add a flag that controls whether this thread is launched or not
  if launchSow == 1 {
      go sow()
  }

    /* Wait for other threads to finish.  Need to call wait() equivalent here*/
  for {
    time.Sleep(time.Second)
  }

}

func eat()  {
  fmt.Println("Launched eat thread for " + myip)

  for  {
    wq.mutex.Lock()
    e := wq.list.Front()
    if e == nil {
      wq.mutex.Unlock()
      time.Sleep(time.Second)
      continue
    }
    work := e.Value.(workItem)
    wq.list.Remove(e)
    wq.mutex.Unlock()

    if work == (workItem{}) {
       fmt.Println ("Got nil work in queue for " + myip)
       continue
    }

    //fmt.Printf("Processing work of cost:%d duration:%d for %s...\n", work.cost, work.duration, myip)
    fmt.Printf("[EAT:%s] Processing work of duration:%d\n", myip, work.duration)
    time.Sleep(time.Second * time.Duration(work.duration))
  }

}

func sow() {
  fmt.Println("Launched sow thread for " + myip)

  for  {
     /* Sleep for a random time */
     sleep_time := rand.Intn(maxSowDuration)
     fmt.Printf("[SOW:%s] Sleeping for %d seconds\n", myip, sleep_time)
     time.Sleep(time.Second * time.Duration(sleep_time))
     duration := rand.Intn(maxWorkDuration)
     cost := rand.Intn(maxCost)
     work := workItem{duration, cost}
     fmt.Printf("[SOW:%s] Adding work item (duration = %d)\n", myip, work.duration)
     wq.mutex.Lock()
     wq.list.PushBack(work)
     wq.mutex.Unlock()
  }
}

func moo() {
  listner, err := net.Listen("tcp", port)
  if err != nil {
      fmt.Fprintln(os.Stderr, err)
      os.Exit(3)
  }

  for {
      conn, err := listner.Accept()
      if err != nil {
        fmt.Println(err)
        continue
      }
      go handleWander(conn)
  }
}

func handleWander(conn net.Conn) {
  fmt.Printf("Cow %v wandered.\n", conn.RemoteAddr())
  fmt.Printf("Client %v strayed.\n", conn.RemoteAddr())
}
