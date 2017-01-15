package main

import (
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
)

type node struct {
	id  string
	gps string
	ip  string
}

/*If these fields are not exported, they won't be encoded properly*/
type Piece struct {
	Name        string /* used for joining*/
	Mode        string
	Contenthash string /* hash of the entire file */
	Start       int64
	Length      int64
	Data        []byte
}

const piecedb = "piecedb"

var daemon = flag.Bool("daemon", false, "Launch daemon")
var split = flag.Bool("split", false, "Split the file")
var join = flag.Bool("join", false, "Join the file")
var rm = flag.Bool("rm", false, "Remove file after splitting")
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

	if *rm && op != "split" {
		fmt.Println("--rm can only used with --split operation")
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
	/*
	 * Splitting logic:
	 * 1. Get the nodes where the pieces will be stored These nodes can be:
	 *    a. List of nodes in the same bluetooth ad hoc network
	 *    b. Nodes within --distance of each other
	 *    c. Nodes specified with --nodes flag
	 *    d. A combination of above
	 *
	 * 2. Shuffle the nodes to determine an order
	 *
	 * 3. Split the file into as many pieces as the nodes in step 1. Each piece can be encrypted
	 *
	 * 4. Transfer the pieces to each node.
	 *
	 * 5. Return success if all nodes were able to store the pieces.
	 */

	file, err := os.Open(fname)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	fstat, _ := file.Stat()
	fsize := fstat.Size()
	fmt.Printf("%q's size is %d bytes\n", fname, fsize)

	nodes := getNodes()
	numnodes := len(nodes)
	if numnodes == 0 {
		fmt.Printf("No nodes found\n")
		os.Exit(1)
	}

	fmt.Printf("Found %d nodes\n", numnodes)

	fmt.Printf("Calculating hash...\n")
	filehash, err := calculateHash(file)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("hash:%s\n", filehash)

	pieces := make([]Piece, numnodes)
	off := int64(0)
	plen := fsize / int64(numnodes)
	for i := 0; i < len(pieces); i++ {
		pieces[i].Name = path.Base(fname)
		pieces[i].Contenthash = filehash
		pieces[i].Start = off
		pieces[i].Length = plen
		if i == numnodes-1 {
			pieces[i].Length += fsize - plen*int64(numnodes)
		}
		pieces[i].Data = make([]byte, pieces[i].Length)
		file.Seek(pieces[i].Start, 0)
		_, err = io.ReadFull(file, pieces[i].Data)
		if err != nil {
			fmt.Printf("Error reading pieces[i] %d\n")
			os.Exit(1)
		}
		off += pieces[i].Length
		fmt.Printf("pieces %d: start:%d data.Length:%d\n", i, pieces[i].Start, len(pieces[i].Data))
	}

	/* Send each piece to a node, for now save it locally */
	for i, piece := range pieces {
		err := sendPieceToNode(piece, nodes[i])
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
	}
}

func joinFile() {
	fmt.Printf("Searching and joining file %q...\n", fname)
}

func sendPieceToNode(p Piece, n node) (err error) {
	/* Send the piece p to node n*/

	/* for now call the receiver directly*/
	receivePiece(p)

	return
}

/* Handled by Daemon */
/* RPC handler that receives the piece*/
func receivePiece(p Piece) {
	savePiece(p)
}

func savePiece(p Piece) {
	fmt.Printf("Saving piece. name:%s start:%d\n", p.Name, p.Start)
	file, err := os.OpenFile(piecedb, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	enc := gob.NewEncoder(file)
	enc.Encode(p)
}

/*
 * Return nodes where the pieces will be stored, including self
 */
func getNodes() []node {
	nodes := make([]node, 4)
	return nodes
}

func listNodes() {
	fmt.Printf("Searching for nodes...\n")
}

/* calculate sha1 hash of file contents*/
func calculateHash(file *os.File) (filehash string, err error) {
	hash := sha1.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		return
	}
	filehash = hex.EncodeToString(hash.Sum(nil))
	return
}

func usage() {
	fmt.Println("\n\nUsage: hcrux [OPTIONS] [filename]")
	fmt.Println("OPTIONS:")
	fmt.Printf("--daemon\n\tLaunch the hcrux daemon. Each node should have a daemon running.\n")
	fmt.Printf("--split\n\tsplit the file into multiple pieces\n")
	fmt.Printf("--join\n\tsearch and build the files if all the pieces are viccinity\n")
	fmt.Printf("--rm\n\tRemove file after splitting\n")
	fmt.Printf("--mode=GPS|BT\n\tmode to determine what determines the viccinity. GPS coords or Bluetooth connection\n")
	fmt.Printf("--distance=<meters>\n\tdistance in meters to determine how close the nodes should be for a file to be joined\n")
	fmt.Printf("--listnodes\n\tlist nodes in the proximity as determined by --mode and/or --distance\n")
	fmt.Printf("--nodes=nodeid1[,nodeid2..]\n\tnode id's of nodes where the pieces of a split file will be stored\n")
	os.Exit(1)
}
