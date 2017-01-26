package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"
)

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
func LaunchServer() {
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
