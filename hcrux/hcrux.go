package main

import (
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
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

type PiecePS []*Piece

const piecedb = "piecedb"

var server = flag.Bool("server", false, "Launch the server")
var split = flag.Bool("split", false, "Split the file")
var join = flag.Bool("join", false, "Join the file")
var rm = flag.Bool("rm", false, "Remove file after splitting")
var list = flag.Bool("listnodes", false, "List nodes in the viccinity")
var numpieces = flag.Int("numpieces", 3, "Number of pieces, a file should be split into")
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
	if *server {
		op = "server"
		allops++
	}
	if allops != 1 {
		fmt.Println("One and only one operation among --server, --listnodes, --split or --join should be specified")
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
	case "server":
		launchServer()
	case "split":
		splitFile()
	case "join":
		joinFile()
	case "list":
		listNodes()
	}
}

func splitFile() {
	fmt.Printf("Splitting file %q...\n", fname)
	/*
	 * Splitting logic:
	 * 1. Get the nodes where the pieces will be stored These nodes can be:
	 *    a. Nodes specified with --nodes flag
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

	np := *numpieces

	fmt.Printf("Calculating hash...\n")
	filehash, err := calculateHash(file)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	fmt.Printf("hash:%s\n", filehash)

	pieces := make([]Piece, np)
	off := int64(0)
	/* TBD: Make sure plen does not exceed 64k */
	plen := fsize / int64(np)
	for i := 0; i < len(pieces); i++ {
		pieces[i].Name = path.Base(fname)
		pieces[i].Contenthash = filehash
		pieces[i].Start = off
		pieces[i].Length = plen
		if i == np-1 {
			pieces[i].Length += fsize - plen*int64(np)
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
	nodes := getNodes()
	for i, piece := range pieces {
		err := sendPieceToNode(piece, nodes[i])
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
	}
}

func joinFile() {

	/*
	 * join logic:
	 * 1. local node (the one which issued --join) searches the local pieces to find
	 *    the content hash for the file we wish to join. This implies that unless
	 *    the local node does not have atleast one piece of a file, it cannot fetch
	 *    other pieces.
	 * 2. ask other nodes to send the pieces with matching hash
	 *      - a remote node will only send a piece if it matches the hash and meets
	 *      - the activation criteria: (node is at a given location and/or the time is
	 *      - within a given range)
	 * 3. local node orders the incoming pieces by their Start field
	 * 4. After a timeout, it assembles the file together and calculates the hash
	 * 5. If the hash matches it exposes(creates) the file
	 */
	found := false
	bfname := path.Base(fname)
	piece := Piece{}
	fmt.Printf("Searching file %q's piece locally...\n", bfname)
	piecefiles, _ := filepath.Glob("piece*")
	for _, piecefile := range piecefiles {
		file, err := os.Open(piecefile)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		dec := gob.NewDecoder(file)
		dec.Decode(&piece)
		if piece.Name == bfname {
			found = true
			file.Close()
			break
		}
		file.Close()
	}

	if !found {
		fmt.Printf("could not find a local piece for %q\n", fname)
		os.Exit(0)
	}
	/* Request pieces from all nodes with matching hash, including self*/
	fmt.Printf("found local piece start:%d data.Length:%d hash:%s\n",
		piece.Start, len(piece.Data), piece.Contenthash)
	pieces := []*Piece{}
	/*TBD: use a channel to assemble pieces received from other nodes */
	pieces = append(pieces, fetchRemotePieces(piece.Contenthash)...)
	pieces = append(pieces, fetchLocalPieces(piece.Contenthash)...)
	joinPieces(pieces)
}

func joinPieces(pieces []*Piece) {

	if len(pieces) == 0 {
		fmt.Printf("No pieces to join!\n")
		os.Exit(1)
	}

	/*
	 * Create the file which will contain the pieces.
	 */
	file, err := os.Create(path.Base(fname) + ".join")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	sort.Sort(PiecePS(pieces))

	/* Write piece Data to temp file */
	for _, pp := range pieces {
		p := *pp
		fmt.Printf("local piece start:%d data.Length:%d hash:%s\n",
			p.Start, len(p.Data), p.Contenthash)
		file.Write(p.Data)
	}
}

func fetchRemotePieces(hash string) (p []*Piece) {
	/*<<<< connect to cloud server here >>>*/
	return
}

func fetchLocalPieces(hash string) (p []*Piece) {
	piecefiles, _ := filepath.Glob("piece*")
	for _, piecefile := range piecefiles {
		piece := Piece{}
		file, err := os.Open(piecefile)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(1)
		}
		dec := gob.NewDecoder(file)
		dec.Decode(&piece)
		if piece.Contenthash == hash && piece.Name == path.Base(fname) {
			p = append(p, &piece)
		}
		file.Close()
	}
	return
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
	file, err := ioutil.TempFile("./", "piece")
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
	nodes := make([]node, 2)
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

/* Sort interface */
func (p PiecePS) Len() int           { return len(p) }
func (p PiecePS) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PiecePS) Less(i, j int) bool { return (*p[i]).Start < (*p[j]).Start }

func usage() {
	fmt.Println("\n\nUsage: hcrux [OPTIONS] [filename]")
	fmt.Println("OPTIONS:")
	fmt.Printf("--server\n\tLaunch the hcrux  aws server. Each node should have this server running.\n")
	fmt.Printf("--split\n\tsplit the file into multiple pieces\n")
	fmt.Printf("--join\n\tsearch and build the files if all the pieces are viccinity\n")
	fmt.Printf("--rm\n\tRemove file after splitting\n")
	fmt.Printf("--mode=GPS|BT\n\tmode to determine what determines the viccinity. GPS coords or Bluetooth connection\n")
	fmt.Printf("--distance=<meters>\n\tdistance in meters to determine how close the nodes should be for a file to be joined\n")
	fmt.Printf("--listnodes\n\tlist nodes in the proximity as determined by --mode and/or --distance\n")
	fmt.Printf("--nodes=nodeid1[,nodeid2..]\n\tnode id's of nodes where the pieces of a split file will be stored\n")
	os.Exit(1)
}

/* THIS SECTION WILL BE MOVED TO A SEPARATE FILE */
/*********************  AWS STUFF  *************************************/
var sess *session.Session
var qsvc *sqs.SQS
var qname, qpath, qurl string

/* Store other nodes QueueUrl(as a key) and map it to queue's ARN */
var nodequeues map[string]string

var wg sync.WaitGroup

/*
 * Launch the server that connects to AWS
 */
func launchServer() {
	fmt.Printf("Launching hcrux aws server...\n")

	wg.Add(1)
	go doSignals()

	/*
	 * TO DO
	 * -  Subscribe queue to notifications on a preknown Topic: SNSHCRUXSQS
	 * -  Push a notification on SNSHCRUXSQS, indicating the queue name
	 *    This will let others know that this node is up.
	 * -  Other nodes can then request pieces by sending a request on this queue.
	 * -  On exit, send notification on ControlTopic that this queue is no longer
	 *    available
	 */
	initAWS()
	/*DO the DO*/
	wg.Wait()
}

func initAWS() {
	nodequeues = make(map[string]string)
	fmt.Printf("Creating aws session...\n")
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	qsvc = sqs.New(sess)

	/* Get list of other node queues */
	qlistr, err := qsvc.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: aws.String("SQS")})
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	/* Store other node's queues */
	for _, u := range qlistr.QueueUrls {
		if u == nil {
			continue
		}
		nodequeues[*u] = "ARN_OF_THIS_QUEUE"
		fmt.Printf("found queue:%s\n", *u)
	}

	/* Create a temp file, this will indicate that the server has a queue created */
	file, err := ioutil.TempFile("./", "SQS")
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	qpath = file.Name()
	qname = path.Base(file.Name())
	file.Close()
	/*
	 *
	 * FIX ME:  There is no guarantee the queue name is not already taken.
	 * The CreateQueue API returns an error only if the Attributes are
	 * different.
	 */
	fmt.Printf("Creating queue...\n")
	r, err := qsvc.CreateQueue(&sqs.CreateQueueInput{QueueName: &qname})
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
		return
	}
	qurl = *r.QueueUrl
	fmt.Println("New Queue: ", qurl)
}

func cleanupAWS() {
	/* This gets call from signal handler */
	fmt.Printf("Deleting queue...\n")
	/*
	 * Delete the Queue.  This will be moved to signal handler
	 */
	_, err := qsvc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: &qurl})
	if err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
		return
	}
	os.Remove(qpath)
	fmt.Printf("queue removed\n.")
}

func doSignals() {
	defer wg.Done()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-sigchan
	cleanupAWS()
}
