package main
import (
 "fmt"
 "log"
 "net"
)


func  handleConnection(c net.Conn) {
    log.Printf("Client %v connected.\n", c.RemoteAddr())
    log.Printf("Client %v closed.\n", c.RemoteAddr())
}

func main() {
    fmt.Println("Hello! from Server")

    ln, err := net.Listen("tcp", ":6666")
    if err != nil {
        log.Fatal(err)
    }

    for {
        conn, err := ln.Accept()
        if err != nil {
          log.Println(err)
          continue
        }
        go handleConnection(conn)
    }

}
