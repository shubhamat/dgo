package main

import (
	"flag"
	"fmt"
	"os"
)

var split = flag.Bool("split", false, "Split the file")
var join = flag.Bool("join", false, "Join the file")
var list = flag.Bool("listnodes", false, "List nodes in the viccinity")
var mode = flag.String("mode", "GPS", "mode to determine what determines the viccinity. GPS coords or Bluetooth connection")
var dist = flag.Int("distance", 100, "distance in meters to determine how close the nodes should be for a file to be join")

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
	if allops != 1 {
		fmt.Println("One and only one operation among --listnodes, --split or --join should be specified")
		usage()
	}

	fname = flag.Arg(0)
	if op != "list" && fname == "" {
		fmt.Println("filename should be provided with a --split or --join operation")
		usage()
	}
}

func usage() {
	fmt.Println("\n\nUsage: hcrux [OPTIONS] [filename]")
	fmt.Println("OPTIONS:")
	fmt.Printf("--split\n\tsplit the file into multiple pieces\n")
	fmt.Printf("--join\n\tsearch and build the files if all the pieces are viccinity\n")
	fmt.Printf("--mode=GPS|BT\n\tmode to determine what determines the viccinity. GPS coords or Bluetooth connection\n")
	fmt.Printf("--distance=<meters>\n\tdistance in meters to determine how close the nodes should be for a file to be joined\n")
	fmt.Printf("--listnodes\n\t list nodes in the proximity as determined by --mode and/or --distance\n")
	os.Exit(1)
}
