# dgo/agentcow

(c) 2017  Shubham Mankhand <shubham.mankhand@gmail.com>

1. ABOUT

    The agentcow project is an exploration in self-orgainzing load balancing.
    
    Each participating node in the cluster (herd!), called a cow,  follows the
    same procedure to process items off a work queue. 
    
    All cows are equal.


2. ASSUMPTIONS

    1. Each cow knows and can talk to some or all of the other cows in the same herd.
    2. Each cow has a work queue that can be filled independently of other cows.

3. ACTORS

    1. cow:     A cow is an entity that processes work.   It takes the work off it's work
                queue, processes it and moves on to the next item.

                A cow has the following threads of execution:

                thread_wander:  A thread that periodically requests the queue size
                                (number of work items in the work queue) of all other cows in the herd.

                thread_eat:     A thread that processes items off work queue. If the work queue is empty,
                                it picks an item from the queue of a cow with maximum items.

                thread_moo:     A thread that processes incoming requests for returing the queue size and
                                handing off a work item from its queue to the requester.

                thread_forage:  A thread that will periodically pick items off other cow's
                                work queue based on some heuristic (such as items in queue).

                thread_discover:  A thread that waits for a broadcast message sent from other cows.
                                When a message from a new cow is received, the new cow is added to
                                the herd.

                thread_bediscovered: A thread that periodically sends broadcast messages.

    2.  herd:   A herd is a group of cows that can talk to each other and know about each other.

    3.  sower:  A sower is an entity that randomly assigns works to all the cows in the herd.
                This is what creates the "load"

                A sower has the following thread of execution:

                thread_sow:     A thread that adds an item to a cow's work queue every
                                few seconds(A random number between [0,N]).

4.  DATA STRUCTURES

    1. work_item:   A work_item tracks the indvidual work each cow has to process.
                    It consisits of the following fields:

                    duration:   The time in msecs it takes to complete this work.
                                A cow processing a work_item, will hold on to it
                                for 'duration' ms before moving on to next one.

                    cost:       cost is a metric to denote the resources a work needs.
                                This will be useful in future.

                    origin:     origin denotes whether the work item is local or came from
                                another cow. It can be used to prevent a work item from being
                                bounced around from one cow to another.

    2. work_queue:  A work_queue is queue of of work_items. Each cow has a work_queue.

    3. cow:         A cow contains a work_queue, it's IP address and ports and a herdmap.

    4. herdwqmap:   A herdwqmap maps the IP Address of a cow in the herd to the ports it uses
                    and it's most recent  queue size.

    5. cows:        An array of IP addresses of other cows in the herd.

# dgo/dht

The dht project is an implementation of a light weight Dynamic Hash Table.
