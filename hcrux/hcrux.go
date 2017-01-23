package main

import (
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

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
type node struct {
	dummy int
}

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
type nodequeue struct {
	url string
	arn string
}

type MessageBody struct {
	Subject string
	Message string
}

const topicname = "SNSHCRUXSQS"

var sess *session.Session
var accountid string
var qsvc *sqs.SQS
var nsvc *sns.SNS
var qname, qpath, qurl, qarn, qsubarn string
var tarn string

/* Store other nodes Queue Name(as a key) and map it to nodequeue struct */
var nodequeues map[string]nodequeue

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

	/* Start the receiver */
	wg.Add(1)
	go receiveQueueMessages()

	wg.Wait()
}

func receiveQueueMessages() {
	defer wg.Done()
	params := &sqs.ReceiveMessageInput{
		QueueUrl:            &qurl,
		MaxNumberOfMessages: aws.Int64(1),
	}

	for {
		/* Ignore the error(for now) */
		rcvr, err := qsvc.ReceiveMessage(params)
		if err != nil {
			continue
		}

		if len(rcvr.Messages) != 0 {
			processMessage(rcvr.Messages[0])
			params2 := &sqs.DeleteMessageInput{
				QueueUrl:      &qurl,
				ReceiptHandle: rcvr.Messages[0].ReceiptHandle,
			}

			_, err = qsvc.DeleteMessage(params2)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
		}
		time.Sleep(time.Second)
	}

}

func processMessage(msg *sqs.Message) {
	var body MessageBody
	err := json.Unmarshal([]byte(*msg.Body), &body)
	if err != nil {
		fmt.Printf("Error in parsing message:%v\n", err)
		return
	}

	switch body.Subject {
	case "NODE_UP":
		process_NODE_UP(body.Message)
	case "NODE_DOWN":
		process_NODE_DOWN(body.Message)
	case "PING":
		process_PING(body.Message)
	default:
		fmt.Printf("Unknown message received\n")
	}
}

func process_NODE_UP(msg string) {
	nodequrl := msg

	if nodequrl == qurl {
		return
	}
	fmt.Printf("Found new node %s\n", nodequrl)
	/* Fetch list of all new queues */
	addNodeQueues("SQS")

	/* Send PING to the newly added queue */
	go sendPING(nodequrl)
}

func process_NODE_DOWN(msg string) {
	nodequrl := msg

	if nodequrl == qurl {
		return
	}
	delNodeQueues(nodequrl)
	fmt.Printf("Node %s is gone!\n", nodequrl)
}

func process_PING(msg string) {
	fmt.Printf("PING %s\n", msg)
}

func initAWS() {
	fmt.Printf("Creating aws session...\n")
	sess = session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	initNotifications()
	initQueues()
	notifyNodeUp()
}

func initNotifications() {
	nsvc = sns.New(sess)
	/* We assume our topic is in the first 100, hence NextToken is not set */
	nlistr, err := nsvc.ListTopics(&sns.ListTopicsInput{})
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	/* Get the ARN of the Topic by scanning the list*/
	for _, topic := range nlistr.Topics {
		if strings.HasSuffix(*topic.TopicArn, topicname) {
			tarn = *topic.TopicArn
			break
		}
	}

	if tarn == "" {
		fmt.Printf("Error: Cannot find control topic\n")
		os.Exit(1)
	}
	fmt.Printf("topic arn: %s\n", tarn)
}

func initQueues() {
	nodequeues = make(map[string]nodequeue)
	qsvc = sqs.New(sess)

	addNodeQueues("SQS")

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
	params := &sqs.GetQueueAttributesInput{
		QueueUrl: &qurl,
		AttributeNames: []*string{
			aws.String("QueueArn"),
		},
	}
	arnr, err := qsvc.GetQueueAttributes(params)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	qarn = *arnr.Attributes["QueueArn"]
	fmt.Printf("New Queue url:%s arn:%s\n", qurl, qarn)

	subscribeToTopic()
}

func addNodeQueues(qnameprefix string) {
	/* Get list of other node queues */
	qlistr, err := qsvc.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: &qnameprefix})
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	/* Store other node's queues */
	for _, u := range qlistr.QueueUrls {
		if u == nil {
			continue
		}
		nq := nodequeue{}
		params := &sqs.GetQueueAttributesInput{QueueUrl: u, AttributeNames: []*string{aws.String("QueueArn")}}
		arnr, err := qsvc.GetQueueAttributes(params)
		if err == nil {
			nq.url = *u
			nq.arn = *arnr.Attributes["QueueArn"]
			nodequeues[*u] = nq
			fmt.Printf("found queue url:%s arn:%s\n", nq.url, nq.arn)
		}
	}
}

func delNodeQueues(queueurl string) {
	fmt.Printf("removing queue url:%s\n", queueurl)
	delete(nodequeues, queueurl)
}

func subscribeToTopic() {

	/* Extract Account ID from queue arn */
	accountid = strings.Split(qarn, ":")[4]
	fmt.Printf("Account ID:%s\n", accountid)

	/* Set permissions on the Queue to receive notifications */
	params := &sqs.AddPermissionInput{
		AWSAccountIds: []*string{aws.String(accountid)},
		Actions:       []*string{aws.String("SendMessage")},
		Label:         aws.String(qname),
		QueueUrl:      aws.String(qurl),
	}

	/* TBD: Check if we should instead call SetAttribute API, is this needed? */
	_, err := qsvc.AddPermission(params)
	if err != nil {
		fmt.Printf("%v\n", err)
		cleanupAWS()
		os.Exit(1)
	}

	setQueueAttributes()

	params2 := &sns.SubscribeInput{
		Protocol: aws.String("sqs"),
		TopicArn: aws.String(tarn),
		Endpoint: aws.String(qarn),
	}

	subr, err := nsvc.Subscribe(params2)
	if err != nil {
		fmt.Printf("%v\n", err)
		cleanupAWS()
		os.Exit(1)
	}

	qsubarn = *subr.SubscriptionArn
}

func setQueueAttributes() {
	policy := `{
    "Version":"2012-10-17",
    "Id":"SNS-To-SQS-Policy",
    "Statement" :[
    {
      "Sid":"Allow-SNS-SendMessage",
      "Effect":"Allow",
      "Principal" :"*",
      "Action":["sqs:SendMessage"],
      "Resource": "` + qarn + `",
      "Condition" :{
        "ArnEquals" :{
          "aws:SourceArn":"` + tarn + `"
        }
      }
    }
    ]
  }`
	fmt.Printf("Setting policy:\n%s\n", policy)

	/* Set Policy and Long Polling */
	params := &sqs.SetQueueAttributesInput{
		QueueUrl: &qurl,
		Attributes: map[string]*string{
			"Policy":                        &policy,
			"ReceiveMessageWaitTimeSeconds": aws.String("20"),
		},
	}

	_, err := qsvc.SetQueueAttributes(params)

	if err != nil {
		fmt.Printf("%v\n", err)
		cleanupAWS()
		os.Exit(1)
	}
}

/* Send a message indicating this node is up */
func notifyNodeUp() {
	fmt.Printf("Sending NODE_UP...\n")
	params := &sns.PublishInput{
		Subject:  aws.String("NODE_UP"),
		Message:  &qurl,
		TopicArn: &tarn,
	}

	_, err := nsvc.Publish(params)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
}

func cleanupAWS() {
	notifyNodeDown()
	cleanupNotifications()
	cleanupQueues()
	os.Exit(0)
}

func cleanupQueues() {
	/* This gets call from signal handler */
	fmt.Printf("Deleting queue...\n")
	/* UnSubscribe*/
	params := &sns.UnsubscribeInput{SubscriptionArn: &qsubarn}
	_, err := nsvc.Unsubscribe(params)
	if err != nil {
		fmt.Printf("%v", err)
	}
	/*
	 * Delete the Queue.  This will be moved to signal handler
	 */
	_, err = qsvc.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: &qurl})
	if err != nil {
		fmt.Printf("%v", err)
	}
	os.Remove(qpath)
	fmt.Printf("queue removed\n.")
}

func cleanupNotifications() {

}

/* Send a message indicating this node is down */
func notifyNodeDown() {
	fmt.Printf("Sending NODE_DOWN...\n")
	params := &sns.PublishInput{
		Subject:  aws.String("NODE_DOWN"),
		Message:  &qurl,
		TopicArn: &tarn,
	}
	/* Probably no point sending this since queue will be deleted in a jiffy */
	_, err := nsvc.Publish(params)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
}

/* Send a PING to a newly added node */
func sendPING(queueurl string) {
	fmt.Printf("Sending ping to %s\n", queueurl)
	body := MessageBody{
		Subject: "PING",
		Message: qurl,
	}

	/* Marshal it so that it is of the same format as sent by SNS */
	bodyjson, _ := json.Marshal(body)
	bodystr := string(bodyjson)
	params := &sqs.SendMessageInput{
		MessageBody: &bodystr,
		QueueUrl:    &queueurl,
	}

	_, err := qsvc.SendMessage(params)
	if err != nil {
		fmt.Printf("%v\n", err)
	}
}

func doSignals() {
	defer wg.Done()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-sigchan
	cleanupAWS()
}
