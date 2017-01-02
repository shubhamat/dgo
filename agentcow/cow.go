package main

import (
  "fmt"
  "os"
  "strings"
  "container/list"
  "math/rand"
  "time"
)


type workItem struct {
  duration  int
  cost      int
}

var workQueue list.List

const port = 23432;

var allcows = []string {
    "192.168.0.31",
    "192.168.0.32",
    "192.168.0.33",
    "192.168.0.34" }

var myip string;

var cows []string;

var launch_sow int = 1

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

  // Launch the sow thread. TBD:  Add a flag that controls whether this thread is launched or not
  if launch_sow == 1 {
      go sow()
  }

  /* Wait for other threads to finish.  Need to call wait() equivalent here*/
  for {
    time.Sleep(time.Duration(2))
  }

}

func sow() {
  fmt.Println("Launched sow thread for " + myip)
  for  {
     /* Sleep for a random time */
     sleep_time := rand.Intn(11)
     time.Sleep(time.Second * time.Duration(sleep_time))
     fmt.Printf("Adding work item on %s's queue after %d seconds\n", myip, sleep_time)
     duration := rand.Intn(31)
     cost := rand.Intn(101)
     work := workItem{duration, cost}
     // TBD: Lock
     workQueue.PushBack(work)
  }
}
