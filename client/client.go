package main

import (
 "fmt"
 "log"
 "net"
)


func main() {
  fmt.Println("Hello! from Client")

  conn, err := net.Dial("tcp", "192.168.0.31:6666")
  if err != nil {
      log.Fatal(err)
  }

  fmt.Printf("Remote Address : %s \n", conn.RemoteAddr().String())
  fmt.Printf("Local Address : %s \n", conn.LocalAddr().String())
}
