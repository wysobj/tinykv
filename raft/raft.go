// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"log"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send, mail box
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// randomized election timeout
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	raftInstance := &Raft{
		id:                    c.ID,
		State:                 StateFollower,
		electionTimeout:       c.ElectionTick,
		heartbeatTimeout:      c.HeartbeatTick,
		randomElectionTimeout: generateElectionTimeout(c.ElectionTick),
		RaftLog:               newLog(c.Storage),
	}

	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}

	raftInstance.Term = hardState.Term
	raftInstance.Vote = hardState.Vote
	raftInstance.RaftLog.committed = hardState.Commit
	raftInstance.RaftLog.applied = c.Applied

	raftInstance.Prs = make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		raftInstance.Prs[uint64(peer)] = nil
	}

	raftInstance.resetProgress()

	raftInstance.clearVotes()

	return raftInstance
}

func (r *Raft) resetProgress() {
	for peer := range r.Prs {
		progress := new(Progress)
		if peer == r.id {
			progress.Match = r.RaftLog.LastIndex()
			progress.Next = r.RaftLog.LastIndex() + 1
		} else {
			progress.Match = 0
			progress.Next = r.RaftLog.LastIndex() + 1
		}
		r.Prs[peer] = progress
	}
}

func generateElectionTimeout(electionTimeout int) int {
	return electionTimeout + rand.Intn(electionTimeout)
}

func (r *Raft) sendMsg(msg pb.Message) {
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	hbMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
	}
	r.sendMsg(hbMsg)
}

func (r *Raft) tickLeader() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		beatMsg := pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			From:    r.id,
			To:      r.id,
		}
		r.Step(beatMsg)
		r.heartbeatElapsed = 0
	}
}

func (r *Raft) tickNonLeader() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		upMsg := pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
			To:      r.id,
		}
		r.Step(upMsg)
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	switch r.State {
	case StateLeader:
		r.tickLeader()
	case StateCandidate:
		r.tickNonLeader()
	case StateFollower:
		r.tickNonLeader()
	}
}

func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomElectionTimeout = generateElectionTimeout(r.electionTimeout)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.resetElectionTimer()
	r.Term = term
	r.Lead = lead
	r.setNotVoted()
	r.clearVotes()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term++
	r.resetElectionTimer()
	r.setNotVoted()
	r.clearVotes()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	r.State = StateLeader
	r.Lead = r.id
	r.resetProgress()
	r.appendNoopEntry()
	r.broadcastAppend()
}

func (r *Raft) stepLeader(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgBeat:
		r.broadCastHearbeat()
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeadBeatResp(msg)
	case pb.MessageType_MsgPropose:
		r.appendEntry(msg.Entries)
		r.broadcastAppend()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendRsp(msg)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteReq(msg)
	case pb.MessageType_MsgTimeoutNow:
		r.handleLeaderTimeout(msg)
	}
}

func (r *Raft) broadCastHearbeat() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		hbMsg := pb.Message{
			MsgType: pb.MessageType_MsgHeartbeat,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
		}
		r.sendMsg(hbMsg)
	}
}

func (r *Raft) handleHeadBeatResp(msg pb.Message) {
	if msg.Term > r.Term {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) handleLeaderTimeout(msg pb.Message) {
	// TODO 2A
}

func (r *Raft) stepCandidate(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.compaign()
	case pb.MessageType_MsgPropose:
		r.candidateDropPropose(msg)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteReq(msg)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteRsp(msg)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(msg)
	}
}

func (r *Raft) candidateDropPropose(msg pb.Message) {
}

func (r *Raft) stepFollower(msg pb.Message) {
	switch msg.MsgType {
	case pb.MessageType_MsgHup:
		r.compaign()
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteReq(msg)
	case pb.MessageType_MsgPropose:
		r.followerHandlePropose(msg)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(msg)
	}
}

func (r *Raft) followerHandlePropose(msg pb.Message) {
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) compaign() {
	r.becomeCandidate()
	r.clearVotes()
	r.voteForSelf()

	if r.statisticVotes() == compainResultWin {
		r.becomeLeader()
		return
	}

	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		voteReq := pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastLogTerm(),
			Index:   r.RaftLog.LastIndex(),
		}
		r.sendMsg(voteReq)
	}
}

func (r *Raft) handleVoteReq(msg pb.Message) {
	if msg.Term < r.Term {
		r.rejectVote(msg.From)
		return
	}

	if msg.Term > r.Term {
		r.becomeFollower(msg.Term, None)
	}

	if !r.moreUpToDate(msg) {
		r.rejectVote(msg.From)
		return
	}

	if msg.Term == r.Term && r.alreadyVoted() && r.Vote != msg.From {
		r.rejectVote(msg.From)
		return
	}

	r.becomeFollower(msg.Term, None)
	r.sendVoteRsp(msg.From)
}

func (r *Raft) moreUpToDate(msg pb.Message) bool {
	if msg.LogTerm > r.RaftLog.LastLogTerm() {
		return true
	}

	if msg.LogTerm == r.RaftLog.LastLogTerm() && msg.Index >= r.RaftLog.LastIndex() {
		return true
	}

	return false
}

func (r *Raft) alreadyVoted() bool {
	return r.Vote != None
}

func (r *Raft) setNotVoted() {
	r.Vote = None
}

func (r *Raft) voteForSelf() {
	r.Vote = r.id
	r.votes[r.id] = true
}

func (r *Raft) getVoteFrom(id uint64) {
	r.votes[id] = true
}

func (r *Raft) getRejectVoteFrom(id uint64) {
	r.votes[id] = false
}

const (
	compainResultWin = iota
	compainResultLose
	compainResultUndetermin
)

func (r *Raft) statisticVotes() uint8 {
	qorum := len(r.Prs) / 2
	win := 0
	lose := 0
	for _, v := range r.votes {
		if v {
			win++
		} else {
			lose++
		}
	}

	switch {
	case win > qorum:
		return compainResultWin
	case lose > qorum:
		return compainResultLose
	default:
		return compainResultUndetermin
	}
}

func (r *Raft) clearVotes() {
	r.votes = make(map[uint64]bool, 0)
}

func (r *Raft) sendVoteRsp(id uint64) {
	r.Vote = id

	voteRspMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      id,
		Term:    r.Term,
		Reject:  false,
	}

	r.sendMsg(voteRspMsg)
}

func (r *Raft) rejectVote(candidate uint64) {
	rejectMsg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      candidate,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastLogTerm(),
		Reject:  true,
	}

	r.sendMsg(rejectMsg)
}

func (r *Raft) handleVoteRsp(msg pb.Message) {
	if msg.Term > r.Term {
		r.becomeFollower(msg.Term, None)
		return
	}

	if msg.Term < r.Term {
		return
	}

	if msg.Reject {
		r.getRejectVoteFrom(msg.From)
	} else {
		r.getVoteFrom(msg.From)
	}

	switch r.statisticVotes() {
	case compainResultWin:
		r.becomeLeader()
	case compainResultLose:
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) appendNoopEntry() {
	entries := make([]*pb.Entry, 0)
	entries = append(entries, &pb.Entry{})
	r.appendEntry(entries)
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	for _, entry := range entries {
		entry.Term = r.Term
	}

	r.RaftLog.appendEntriesAndSetIndex(entries)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
}

func (r *Raft) broadcastAppend() {
	if len(r.Prs) > 1 {
		for peer := range r.Prs {
			if peer == r.id {
				continue
			}
			r.sendAppend(peer)
		}
	} else {
		r.updateCommit()
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	progress := r.Prs[to]
	if progress == nil {
		return false
	}

	if progress.Next > r.RaftLog.LastIndex() {
		return false
	}

	entries, err := r.RaftLog.getEntries(progress.Next, r.RaftLog.LastIndex()+1)
	if err != nil && err != ErrCompacted {
		log.Panicf("can not find entry(%d) lastIdx(%d), impossible", progress.Next, r.RaftLog.LastIndex())
	}

	prevIdx := progress.Next - 1
	prevTerm, err := r.RaftLog.Term(prevIdx)
	if err != nil {
		return false
	}
	appendMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   prevIdx,
		LogTerm: prevTerm,
		Commit:  r.RaftLog.committed,
		Entries: make([]*pb.Entry, 0),
	}

	for _, entry := range entries {
		appendEntry := entry
		appendMsg.Entries = append(appendMsg.Entries, &appendEntry)
	}

	r.sendMsg(appendMsg)

	return true
}

func (r *Raft) checkAppendEntriesAccept(msg pb.Message) bool {
	prevIndex := msg.Index
	if r.RaftLog.LastIndex() < prevIndex {
		return false
	}

	prevTerm := msg.LogTerm
	entryTerm, err := r.RaftLog.Term(prevIndex)
	if err != nil || entryTerm != prevTerm {
		return false
	}

	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(msg pb.Message) {
	if msg.Term < r.Term {
		r.rejectAppend(msg)
		return
	}

	r.becomeFollower(msg.Term, msg.From)

	if !r.checkAppendEntriesAccept(msg) {
		r.rejectAppend(msg)
		return
	}

	if msg.Entries == nil && r.RaftLog.committed < msg.Commit {
		// if msg.Entries == nil, this msg used to update follower commit index.
		r.updateCommitIndex(msg)
		r.Lead = msg.From
		r.sendAppendRsp(msg, r.RaftLog.LastIndex())
		return
	}

	r.RaftLog.appendEntries(msg.Entries)
	r.updateCommitIndex(msg)

	r.sendAppendRsp(msg, r.RaftLog.LastIndex())
}

func (r *Raft) updateCommitIndex(msg pb.Message) {
	lastNewIndex := msg.Index
	if msg.Entries == nil || len(msg.Entries) == 0 {
		r.RaftLog.committed = lastNewIndex
		return
	}

	for _, entry := range msg.Entries {
		if entry.Index > lastNewIndex {
			lastNewIndex = entry.Index
		}
	}

	r.RaftLog.committed = msg.Commit
	if lastNewIndex < msg.Commit {
		r.RaftLog.committed = lastNewIndex
	}
}

func (r *Raft) sendAppendRsp(msg pb.Message, index uint64) {
	appendRspMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      msg.From,
		Index:   index,
		Term:    r.Term,
		Reject:  false,
	}

	r.sendMsg(appendRspMsg)
}

func (r *Raft) rejectAppend(msg pb.Message) {
	rejectAppendMsg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      msg.From,
		Term:    r.Term,
		Reject:  true,
	}

	r.sendMsg(rejectAppendMsg)
}

func (r *Raft) handleAppendRsp(msg pb.Message) {
	if msg.Term > r.Term {
		r.becomeFollower(r.Term, None)
		return
	}

	progress := r.Prs[msg.From]
	if msg.Reject {
		progress.Next--
		r.broadcastAppend()
		return
	}

	if msg.Index > progress.Match {
		progress.Match = msg.Index
		progress.Next = progress.Match + 1
	}

	r.updateCommit()
}

func (r *Raft) updateCommit() {
	matchIdxes := make([]uint64, 0)
	for peer := range r.Prs {
		progress := r.Prs[peer]
		matchIdxes = append(matchIdxes, progress.Match)
	}

	sort.Slice(matchIdxes, func(i, j int) bool {
		// reverse order
		return matchIdxes[i] > matchIdxes[j]
	})

	minCommitIdx := matchIdxes[len(matchIdxes)/2]
	commitLogTerm, err := r.RaftLog.Term(minCommitIdx)
	if err != nil {
		return
	}

	if minCommitIdx > r.RaftLog.committed && commitLogTerm == r.Term {
		r.RaftLog.committed = minCommitIdx
		r.broadcastUpdateCommit()
	}
}

func (r *Raft) broadcastUpdateCommit() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		updateCommitMsg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      peer,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: r.RaftLog.LastLogTerm(),
			Commit:  r.RaftLog.committed,
		}
		r.sendMsg(updateCommitMsg)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(msg pb.Message) {
	if msg.Term >= r.Term {
		r.becomeFollower(msg.Term, msg.From)
	}

	r.sendHearbeatRsp(msg)
}

func (r *Raft) sendHearbeatRsp(msg pb.Message) {
	hbRspMsg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      msg.From,
		Term:    r.Term,
	}

	r.sendMsg(hbRspMsg)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
