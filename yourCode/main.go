package main

import (
	"context"
	"cuhk/asgn/raft"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
)

func main() {
	ports := os.Args[2]
	myport, _ := strconv.Atoi(os.Args[1])
	nodeID, _ := strconv.Atoi(os.Args[3])
	heartBeatInterval, _ := strconv.Atoi(os.Args[4])
	electionTimeout, _ := strconv.Atoi(os.Args[5])

	portStrings := strings.Split(ports, ",")

	// A map where
	// 		the key is the node id
	//		the value is the {hostname:port}
	nodeidPortMap := make(map[int]int)
	for i, portStr := range portStrings {
		port, _ := strconv.Atoi(portStr)
		nodeidPortMap[i] = port
	}

	// Create and start the Raft Node.
	_, err := NewRaftNode(myport, nodeidPortMap,
		nodeID, heartBeatInterval, electionTimeout)

	if err != nil {
		log.Fatalln("Failed to create raft node:", err)
	}

	// Run the raft node forever.
	select {}
}

type raftNode struct {
	// TODO: Implement this! Done
	log               []*raft.LogEntry
	hashMap           map[string]int32
	serverState       raft.Role
	nodeId            int32
	electionTimeout   int32
	heartBeatInterval int32
	currentTerm       int32
	votedFor          int32
	commitIndex       int32
	nextIndex         map[int32]int32
	matchIndex        map[int32]int32
	commitChan        chan bool
	resetChan         chan bool
	mutex             sync.Mutex
	majoritySize      int
}

// Desc:
// NewRaftNode creates a new RaftNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes.
//
// Params:
// myport: the port of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
// nodeidPortMap: a map from all node IDs to their ports.
// nodeId: the id of this node
// heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
// electionTimeout: The election timeout for this node. In millisecond.
func NewRaftNode(myport int, nodeidPortMap map[int]int, nodeId, heartBeatInterval,
	electionTimeout int) (raft.RaftNodeServer, error) {
	// TODO: Implement this!

	//remove myself in the hostmap
	delete(nodeidPortMap, nodeId)

	//a map for {node id, gRPCClient}
	hostConnectionMap := make(map[int32]raft.RaftNodeClient)

	rn := raftNode{
		log:               nil,
		hashMap:           make(map[string]int32),
		serverState:       raft.Role_Follower,
		heartBeatInterval: int32(heartBeatInterval),
		electionTimeout:   int32(electionTimeout),
		currentTerm:       0,
		votedFor:          -1,
		nextIndex:         make(map[int32]int32),
		matchIndex:        make(map[int32]int32),
		resetChan:         make(chan bool, 1),
		commitChan:        make(chan bool, 1),
		majoritySize:      (len(nodeidPortMap)+1)/2 + 1,
		commitIndex:       0,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", myport))

	if err != nil {
		log.Println("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, &rn)

	log.Printf("Start listening to port: %d", myport)
	go s.Serve(l)

	//Try to connect nodes
	for tmpHostId, hostPorts := range nodeidPortMap {
		hostId := int32(tmpHostId)
		numTry := 0
		for {
			numTry++

			conn, err := grpc.Dial(fmt.Sprintf("127.0.0.1:%d", hostPorts), grpc.WithInsecure(), grpc.WithBlock())
			//defer conn.Close()
			client := raft.NewRaftNodeClient(conn)
			if err != nil {
				log.Println("Fail to connect other nodes. ", err)
				time.Sleep(1 * time.Second)
			} else {
				hostConnectionMap[hostId] = client
				break
			}
		}
	}
	log.Printf("Successfully connect all nodes")

	//TODO: kick off leader election here ! Done 240408
	ctx := context.Background()
	go func() {
		for {
			switch rn.serverState {
			case raft.Role_Leader:
				for hostID, client := range hostConnectionMap {
					go func(hostId int32, client raft.RaftNodeClient) {
						rn.mutex.Lock()
						var nextLogIndex = rn.nextIndex[hostId]
						var prevLogIndex = 0
						if nextLogIndex > 1 {
							prevLogIndex = int(nextLogIndex - 1)
						}
						var prevLogTerm = 0
						if prevLogIndex > 0 {
							prevLogTerm = int(rn.log[prevLogIndex-1].Term)
						}

						sendLog := make([]*raft.LogEntry, 0)
						if nextLogIndex > 0 {
							sendLog = append(sendLog, rn.log[nextLogIndex-1:]...)
						}
						rn.mutex.Unlock()
						reply, err := client.AppendEntries(ctx, &raft.AppendEntriesArgs{
							From:         int32(nodeId),
							To:           hostId,
							Term:         rn.currentTerm,
							LeaderId:     int32(nodeId),
							PrevLogIndex: int32(prevLogIndex),
							PrevLogTerm:  int32(prevLogTerm),
							Entries:      sendLog,
							LeaderCommit: rn.commitIndex,
						})

						if err == nil {
							rn.mutex.Lock()
							if reply.Success {
								rn.matchIndex[hostId] = reply.MatchIndex
								rn.nextIndex[hostId] = reply.MatchIndex + 1
								flag := true
								for flag {
									count := 1
									for i := range rn.matchIndex {
										if rn.matchIndex[i] >= rn.commitIndex+1 {
											count++
										}
									}
									if count >= rn.majoritySize {
										rn.commitIndex = rn.commitIndex + 1
										rn.commitChan <- true
									} else if count < rn.majoritySize {
										flag = false
									}
								}
							} else {
								rn.nextIndex[hostId]--
							}
							rn.mutex.Unlock()
						}
					}(hostID, client)
				}
				select {
				case <-time.After(time.Duration(rn.heartBeatInterval) * time.Millisecond):
				case <-rn.resetChan:
				}

			case raft.Role_Follower:
				select {
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
					rn.serverState = raft.Role_Candidate
				case <-rn.resetChan:
				}

			case raft.Role_Candidate:
				rn.mutex.Lock()
				rn.currentTerm++
				rn.votedFor = int32(nodeId)
				lastLogIndex := len(rn.log)
				lastLogTerm := 0
				if lastLogIndex > 0 {
					lastLogTerm = int(rn.log[lastLogIndex-1].Term)
				}
				rn.mutex.Unlock()

				voteNum := 1
				for hostID, client := range hostConnectionMap {
					go func(hostId int32, c raft.RaftNodeClient) {
						reply, err := c.RequestVote(ctx, &raft.RequestVoteArgs{
							From:         int32(nodeId),
							To:           hostId,
							Term:         rn.currentTerm,
							LastLogIndex: int32(lastLogIndex),
							LastLogTerm:  int32(lastLogTerm),
						})
						if err == nil {
							if reply.VoteGranted && reply.Term == rn.currentTerm {
								rn.mutex.Lock()
								voteNum++
								rn.mutex.Unlock()
								if voteNum >= rn.majoritySize && rn.serverState == raft.Role_Candidate {
									rn.serverState = raft.Role_Leader
									for i := range rn.nextIndex {
										rn.nextIndex[i] = int32(len(rn.log))
									}
									for i := range rn.matchIndex {
										rn.matchIndex[i] = 0
									}
									rn.resetChan <- true
								}
							} else if reply.Term > rn.currentTerm {
								rn.serverState = raft.Role_Follower
							}
						}
					}(hostID, client)
				}
				select {
				case <-time.After(time.Duration(rn.electionTimeout) * time.Millisecond):
				case <-rn.resetChan:
				}
			}
		}
	}()

	return &rn, nil
}

// Desc:
// Propose initializes proposing a new operation, and replies with the
// result of committing this operation. Propose should not return until
// this operation has been committed, or this node is not leader now.
//
// If the we put a new <k, v> pair or deleted an existing <k, v> pair
// successfully, it should return OK; If it tries to delete an non-existing
// key, a KeyNotFound should be returned; If this node is not leader now,
// it should return WrongNode as well as the currentLeader id.
//
// Params:
// args: the operation to propose
// reply: as specified in Desc
func (rn *raftNode) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	// TODO: Implement this! Done 240403
	log.Printf("Receive propose from client")
	var ret raft.ProposeReply

	switch rn.serverState {
	case raft.Role_Leader:
		rn.log = append(rn.log, &raft.LogEntry{
			Term:  rn.currentTerm,
			Op:    args.GetOp(),
			Key:   args.GetKey(),
			Value: args.GetV(),
		})
		<-rn.commitChan

		ret.CurrentLeader = rn.nodeId
		switch args.GetOp() {
		case raft.Operation_Put:
			ret.Status = raft.Status_OK
			rn.hashMap[args.Key] = args.GetV()
		case raft.Operation_Delete:
			_, ok := rn.hashMap[args.GetKey()]
			if ok {
				ret.Status = raft.Status_OK
				delete(rn.hashMap, args.GetKey())
			} else {
				ret.Status = raft.Status_KeyNotFound
			}
		}
	case raft.Role_Follower:
		ret.Status = raft.Status_WrongNode
		ret.CurrentLeader = rn.votedFor
	case raft.Role_Candidate:
		ret.Status = raft.Status_WrongNode
		ret.CurrentLeader = rn.votedFor
	}

	return &ret, nil
}

// Desc:GetValue
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (rn *raftNode) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	// TODO: Implement this! Done 240320
	var ret raft.GetValueReply
	tmpValue, ok := rn.hashMap[args.GetKey()]
	if ok {
		ret.Status = raft.Status_KeyFound
		ret.V = tmpValue
	} else {
		ret.Status = raft.Status_KeyNotFound
	}
	return &ret, nil
}

// Desc:
// Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the RequestVote Reply Message
func (rn *raftNode) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	// TODO: Implement this! Done 240330
	var reply raft.RequestVoteReply
	reply.From = args.GetTo()
	reply.To = args.GetFrom()

	if args.GetTerm() < rn.currentTerm {
		reply.VoteGranted = false
	} else if args.GetTerm() == rn.currentTerm {
		if rn.votedFor != -1 {
			reply.VoteGranted = false
		} else {
			if rn.votedFor != args.GetCandidateId() {
				reply.VoteGranted = false
			} else {
				reply.VoteGranted = true
			}
		}
	} else {
		reply.VoteGranted = true
	}

	if reply.VoteGranted {
		rn.votedFor = args.GetFrom()
		rn.currentTerm = args.GetTerm()
		rn.serverState = raft.Role_Follower
		rn.resetChan <- true
	}

	reply.Term = rn.currentTerm
	return &reply, nil
}

// Desc:
// Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
//
// Params:
// args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
// you call this API
// reply: the AppendEntries Reply Message
func (rn *raftNode) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	// TODO: Implement this! Done 240401
	var reply raft.AppendEntriesReply

	reply.From = args.GetTo()
	reply.To = args.GetFrom()
	reply.Success = true

	if args.GetTerm() < rn.currentTerm {
		reply.Success = false
	} else {
		if args.GetPrevLogIndex() > 0 {
			if args.GetPrevLogIndex() > int32(len(rn.log)) {
				reply.Success = false
			}
			if rn.log[args.GetPrevLogIndex()-1].Term != args.GetPrevLogTerm() {
				reply.Success = false
				rn.log = rn.log[:args.GetPrevLogIndex()]
			}
		}
	}

	if reply.Success {
		rn.currentTerm = args.GetTerm()
		rn.votedFor = args.GetFrom()
		rn.log = append(rn.log, args.GetEntries()...)
		reply.MatchIndex = int32(int(args.GetPrevLogIndex()) + len(args.GetEntries()))

		if args.GetLeaderCommit() > rn.commitIndex {
			var start int32
			if rn.commitIndex > 0 {
				start = rn.commitIndex - 1
			} else {
				start = 0
			}
			for _, entry := range rn.log[start:args.GetLeaderCommit()] {
				if entry.Op == raft.Operation_Put {
					rn.hashMap[entry.Key] = entry.Value
				}
				if entry.Op == raft.Operation_Delete {
					_, ok := rn.hashMap[entry.Key]
					if ok {
						delete(rn.hashMap, entry.Key)
					}
				}
			}
			rn.commitIndex = args.GetLeaderCommit()
		}
		rn.serverState = raft.Role_Follower
		rn.resetChan <- true
	}

	reply.Term = rn.currentTerm

	return &reply, nil
}

// Desc:
// Set electionTimeOut as args.Timeout milliseconds.
// You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	// TODO: Implement this! Done 240320
	var reply raft.SetElectionTimeoutReply
	rn.electionTimeout = args.GetTimeout()
	rn.resetChan <- true
	return &reply, nil
}

// Desc:
// Set heartBeatInterval as args.Interval milliseconds.
// You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
//
// Params:
// args: the heartbeat duration
// reply: no use
func (rn *raftNode) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	// TODO: Implement this! Done 240320
	var reply raft.SetHeartBeatIntervalReply
	rn.heartBeatInterval = args.GetInterval()
	rn.resetChan <- true
	return &reply, nil
}

//NO NEED TO TOUCH THIS FUNCTION
func (rn *raftNode) CheckEvents(context.Context, *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	return nil, nil
}
