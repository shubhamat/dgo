package main

import (
	"flag"
	"fmt"
	"os"
)

type node struct {
	id  string
	gps string
	ip  string
}

type piece struct {
	hash   string
	start  int
	length int
}

var daemon = flag.Bool("daemon", false, "Launch daemon")
var split = flag.Bool("split", false, "Split the file")
var join = flag.Bool("join", false, "Join the file")
var list = flag.Bool("listnodes", false, "List nodes in the viccinity")
var mode = flag.String("mode", "GPS", "mode to determine what determines the viccinity. GPS coords or Bluetooth connection")
var dist = flag.Int("distance", 100, "distance in meters to determine how close the nodes should be for a file to be join")
var nodes = flag.String("nodes", "", "node id's of nodes where the pieces of a split file will be stored")

var op string
var fname string

func main() {
	parseArgs()
}

func parseArgs() {
	flag.Parse()

	allops := 0
	if *split {
		op = "split"
		allops++
	}
	if *join {
		op = "join"
		allops++
	}
	if *list {
		op = "list"
		allops++
	}
	if *daemon {
		op = "daemon"
		allops++
	}
	if allops != 1 {
		fmt.Println("One and only one operation among --daemon, --listnodes, --split or --join should be specified")
		usage()
	}

	fname = flag.Arg(0)
	if (op == "split" || op == "join") && fname == "" {
		fmt.Println("filename should be provided with a --split or --join operation")
		usage()
	}

	switch op {
	case "daemon":
		daemonize()
	case "split":
		splitFile()
	case "join":
		joinFile()
	case "list":
		listNodes()
	}
}

func daemonize() {
	fmt.Printf("Launching hcrux daemon...\n")
}

func splitFile() {
	fmt.Printf("Splitting file %q...\n", fname)
}

func joinFile() {
	fmt.Printf("Searching and joining file %q...\n", fname)
}

func listNodes() {
	fmt.Printf("Searching for nodes...\n")
}

func calculateHash() {
	/*md5 hash of file contents*/
}

func usage() {
	fmt.Println("\n\nUsage: hcrux [OPTIONS] [filename]")
	fmt.Println("OPTIONS:")
	fmt.Printf("--daemon\n\tLaunch the hcrux daemon. Each node should have a daemon running.\n")
	fmt.Printf("--split\n\tsplit the file into multiple pieces\n")
	fmt.Printf("--join\n\tsearch and build the files if all the pieces are viccinity\n")
	fmt.Printf("--mode=GPS|BT\n\tmode to determine what determines the viccinity. GPS coords or Bluetooth connection\n")
	fmt.Printf("--distance=<meters>\n\tdistance in meters to determine how close the nodes should be for a file to be joined\n")
	fmt.Printf("--listnodes\n\tlist nodes in the proximity as determined by --mode and/or --distance\n")
	fmt.Printf("--nodes=nodeid1[,nodeid2..]\n\tnode id's of nodes where the pieces of a split file will be stored\n")
	os.Exit(1)
}
