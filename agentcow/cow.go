package main

import (
  "fmt"
  "os"
  "strings"
)

const port = 23432;

var allcows = []string {
    "192.168.0.31",
    "192.168.0.32",
    "192.168.0.33",
    "192.168.0.34" }

var myip string;

var cows []string;

func main() {

  myip = strings.Join(os.Args[1:], "")

  if myip == "" {
    fmt.Println("You most specify IP address.  Usage:  cow <IP_Address>")
    os.Exit(1)
  }

  fmt.Printf("Initializing cow:%s ...\n", myip)

  cows = make([]string, len(allcows) - 1)

  for i,j := 0, 0; i < len(allcows); i++ {
     if (allcows[i] != myip) {
        cows[j] = allcows[i]
        j++
     }

     if (j >= len(cows)) {
       fmt.Printf("%s does not exist in the list of known cows. Enter a known IP.\n", myip)
       os.Exit(1)
     }

   }

  for i := 0; i < len(cows); i++ {
    fmt.Println(cows[i])
  }


}
