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
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	raftLog := &RaftLog{
		storage: storage,
		entries: make([]pb.Entry, 0),
	}

	firstIdx, err := storage.FirstIndex()
	if err != nil {
		log.Panicf("recover first log index failed, err: %v", err)
	}

	lastIdx, err := storage.LastIndex()
	if err != nil {
		log.Panicf("recover last log index failed, err: %v", err)
	}

	raftLog.entries, err = storage.Entries(firstIdx, lastIdx+1)
	if err != nil {
		log.Panicf("recover log entries index failed, first: %d, last: %d, err: %v", firstIdx, lastIdx, err)
	}

	raftLog.stabled = lastIdx
	raftLog.committed = lastIdx

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return l.entries
	}

	firstIdx := l.entries[0].Index
	if l.stabled < firstIdx {
		return l.entries
	}

	offset := l.stabled + 1 - firstIdx
	return l.entries[offset:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if l.committed == l.applied {
		return make([]pb.Entry, 0)
	}

	entries, err := l.getEntries(l.applied+1, l.committed+1)
	if err != nil {
		return nil
	}

	return entries
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if len(l.entries) == 0 {
		return l.storage.Term(i)
	}

	firstIdx := l.entries[0].Index
	if i < firstIdx {
		return l.storage.Term(i)
	}

	offset := i - firstIdx
	if offset >= uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}

	return l.entries[offset].Term, nil
}

func (l *RaftLog) LastLogTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Term
}

func (l *RaftLog) getEntries(lo, hi uint64) ([]pb.Entry, error) {
	if lo > l.LastIndex() || lo >= hi {
		return nil, ErrUnavailable
	}

	entries := make([]pb.Entry, 0)
	firstIdx := l.entries[0].Index
	if lo < firstIdx {
		return make([]pb.Entry, 0), ErrCompacted
	}

	offset := lo - firstIdx
	end := hi - firstIdx
	entries = append(entries, l.entries[offset:end]...)

	return entries, nil
}

// leader append operation, will assign indexes
func (l *RaftLog) appendEntriesAndSetIndex(entries []*pb.Entry) {
	for _, entry := range entries {
		entry.Index = l.LastIndex() + 1
		l.entries = append(l.entries, *entry)
	}
}

func (l *RaftLog) appendEntries(entries []*pb.Entry) {
	if len(entries) == 0 {
		return
	}

	for _, entry := range entries {
		if len(l.entries) == 0 {
			l.entries = append(l.entries, *entry)
			continue
		}

		lastIdx := l.entries[len(l.entries)-1].Index
		if entry.Index > lastIdx+1 {
			log.Panicf("missing entry, lastIdx:%d append:%d", lastIdx, entry.Index)
		} else if entry.Index == lastIdx+1 {
			l.entries = append(l.entries, *entry)
		} else {
			offset := entry.Index - l.entries[0].Index
			if l.entries[offset].Index == entry.Index && l.entries[offset].Term == entry.Term {
				continue
			}

			// meet entry conflict, truncate remains
			l.entries = l.entries[:offset]
			l.entries = append(l.entries, *entry)
			l.stabled = offset
		}
	}
}

func (l *RaftLog) getEntryNum() uint64 {
	return uint64(len(l.entries))
}

func (l *RaftLog) truncateEntries(index uint64) {
	if len(l.entries) == 0 {
		return
	}

	firstIdx := l.entries[0].Index
	if firstIdx > index {
		return
	}

	offset := index - firstIdx
	if offset > uint64(len(l.entries)) {
		return
	}

	l.entries = l.entries[:offset]
}
