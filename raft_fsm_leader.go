// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
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
	"fmt"
	"sort"
	"time"

	"github.com/yixinin/raft/logger"
	"github.com/yixinin/raft/proto"
	"github.com/yixinin/raft/util"
)

func (r *raftFsm) becomeLeader() {
	if r.state == stateFollower {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeLeader][%v] invalid transition [follower -> leader].", r.id)))
	}
	r.recoverCommit()
	lasti := r.raftLog.lastIndex()
	r.step = stepLeader
	r.reset(r.term, lasti, true)
	r.tick = r.tickHeartbeat
	r.leader = r.config.NodeID
	r.state = stateLeader
	r.acks = nil

	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		errMsg := fmt.Sprintf("[raft->becomeLeader][%v] unexpected error getting uncommitted entries (%v).", r.id, err)
		logger.Error(errMsg)
		panic(AppPanicError(errMsg))
	}
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeLeader][%v] unexpected double uncommitted config entry.", r.id)))
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	r.appendEntry(&proto.Entry{Term: r.term, Index: lasti + 1, Data: nil})
	if logger.IsEnableDebug() {
		logger.Debug("raft[%v] became leader at term %d.", r.id, r.term)
	}
}

func stepLeader(r *raftFsm, m *proto.Message) {
	// These message types do not require any progress for m.From.
	switch m.Type {
	case proto.LocalMsgProp:
		if _, ok := r.replicas[r.config.NodeID]; !ok || len(m.Entries) == 0 {
			return
		}

		for i, e := range m.Entries {
			if e.Type == proto.EntryConfChange {
				if r.pendingConf {
					m.Entries[i] = &proto.Entry{Term: e.Term, Index: e.Index, Type: proto.EntryNormal}
				}
				r.pendingConf = true
			}
		}
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		if logger.IsEnableDebug() {
			logger.Debug("[raft->stepLeader][%v logterm: %d, index: %d, vote: %v] rejected vote from %v [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		r.send(nmsg)
		proto.ReturnMessage(m)
		return
	}

	// All other message types require a progress for m.From (pr).
	pr, prOk := r.replicas[m.From]
	if !prOk {
		if logger.IsEnableDebug() {
			logger.Debug("[raft->stepLeader][%v] no progress available for %v.", r.id, m.From)
		}
		return
	}
	switch m.Type {
	case proto.RespMsgAppend:
		pr.active = true
		pr.lastActive = time.Now()

		if m.Reject {
			if logger.IsEnableDebug() {
				logger.Debug("raft[%v] received msgApp rejection(lastindex: %d) from %v for index %d.", r.id, m.RejectIndex, m.From, m.Index)
			}
			if pr.maybeDecrTo(m.Index, m.RejectIndex, m.Commit) {
				if pr.state == replicaStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
		} else {
			oldPaused := pr.isPaused()
			if pr.maybeUpdate(m.Index, m.Commit) {
				switch {
				case pr.state == replicaStateProbe:
					pr.becomeReplicate()
				case pr.state == replicaStateSnapshot && pr.needSnapshotAbort():
					if logger.IsEnableWarn() {
						logger.Warn("raft[%v] snapshot aborted, resumed sending replication messages to %v.", r.id, m.From)
					}
					pr.becomeProbe()
				case pr.state == replicaStateReplicate:
					pr.inflight.freeTo(m.Index)
				}

				if r.maybeCommit() {
					r.bcastAppend()
				} else if oldPaused {
					r.sendAppend(m.From)
				}
			}
		}
		proto.ReturnMessage(m)
		return

	case proto.RespMsgHeartBeat:
		if pr.state == replicaStateReplicate && pr.inflight.full() {
			pr.inflight.freeFirstOne()
		}
		if !pr.pending && (pr.match < r.raftLog.lastIndex() || pr.committed < r.raftLog.committed) {
			r.sendAppend(m.From)
		}

		pr.active = true
		pr.lastActive = time.Now()
		if pr.state != replicaStateSnapshot {
			pr.pending = false
		}
		return

	case proto.LeaseMsgOffline:
		for id := range r.replicas {
			if id == r.config.NodeID {
				continue
			}
			nmsg := proto.GetMessage()
			nmsg.Type = proto.LeaseMsgTimeout
			nmsg.To = id
			r.send(nmsg)
		}
		logger.Debug("[raft][%v] LeaseMsgOffline at term[%d] leader[%d].", r.id, r.term, r.leader)
		r.becomeFollower(r.term, NoLeader)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgSnapShot:
		if pr.state != replicaStateSnapshot {
			return
		}

		if m.Reject {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] send snapshot to [%v] failed.", r.id, m.From)
			}
			pr.snapshotFailure()
			pr.becomeProbe()
		} else {
			pr.active = true
			pr.lastActive = time.Now()
			pr.becomeProbe()
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] send snapshot to [%v] succeeded, resumed replication [%s]", r.id, m.From, pr)
			}
		}

		// If snapshot finish, wait for the RespMsgAppend from the remote node before sending out the next ReqMsgAppend.
		// If snapshot failure, wait for a heartbeat interval before next try.
		pr.pause()
		proto.ReturnMessage(m)
		return

	case proto.RespCheckQuorum:
		// TODO: remove this when stable
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum resp from %d, index=%d", r.id, m.From, m.Index)
		}
		r.readOnly.recvAck(m.Index, m.From, r.quorum())
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) becomeElectionAck() {
	r.acks = make(map[uint64]bool)
	r.acks[r.config.NodeID] = true
	if len(r.acks) >= r.quorum() {
		r.becomeLeader()
		return
	}

	logger.Debug("raft[%v] became election at term %d.", r.id, r.term)

	r.step = stepElectionAck
	r.reset(r.term, 0, false)
	r.tick = r.tickElectionAck
	r.state = stateElectionACK
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}

		m := proto.GetMessage()
		m.Type = proto.ReqMsgElectAck
		m.To = id
		r.send(m)
	}
}

func stepElectionAck(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] no leader at term %d; dropping proposal", r.id, r.term)
		}
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgAppend:
		r.becomeFollower(r.term, m.From)
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.becomeFollower(r.term, m.From)
		return

	case proto.ReqMsgElectAck:
		r.becomeFollower(r.term, m.From)
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespCheckQuorum:
		// TODO: remove this when stable
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum resp from %d, index=%d", r.id, m.From, m.Index)
		}
		r.readOnly.recvAck(m.Index, m.From, r.quorum())
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgElectAck:
		r.replicas[m.From].active = true
		r.replicas[m.From].lastActive = time.Now()
		r.acks[m.From] = true
		if len(r.acks) >= r.quorum() {
			r.becomeLeader()
			r.bcastAppend()
		}
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.pastElectionTimeout() {
		r.electionElapsed = 0
		if r.config.LeaseCheck && !r.checkLeaderLease() {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] stepped down to follower since quorum is not active.", r.id)
			}
			logger.Debug("[raft][%v] heartbeat election timeout at term[%d] leader[%d].", r.id, r.term, r.leader)
			r.becomeFollower(r.term, NoLeader)
		}
	}

	if r.state != stateLeader {
		return
	}

	if r.heartbeatElapsed >= r.config.HeartbeatTick {
		r.heartbeatElapsed = 0
		for id := range r.replicas {
			if id == r.config.NodeID {
				continue
			}
			if r.replicas[id].state != replicaStateSnapshot {
				r.replicas[id].resume()
			}
		}
		r.bcastReadOnly()
	}
}

func (r *raftFsm) tickElectionAck() {
	r.electionElapsed++
	if r.electionElapsed >= r.config.ElectionTick {
		r.electionElapsed = 0

		m := proto.GetMessage()
		m.Type = proto.LocalMsgHup
		m.From = r.config.NodeID
		r.Step(m)
	}
}

func (r *raftFsm) checkLeaderLease() bool {
	var act int
	for id := range r.replicas {
		if id == r.config.NodeID || r.replicas[id].state == replicaStateSnapshot {
			act++
			continue
		}

		if r.replicas[id].active {
			act++
		}
		r.replicas[id].active = false
	}

	return act >= r.quorum()
}

func (r *raftFsm) maybeCommit() bool {
	mis := make(util.Uint64Slice, 0, len(r.replicas))
	for _, rp := range r.replicas {
		mis = append(mis, rp.match)
	}
	sort.Sort(sort.Reverse(mis))
	mci := mis[r.quorum()-1]
	isCommit := r.raftLog.maybeCommit(mci, r.term)
	if r.state == stateLeader && r.replicas[r.config.NodeID] != nil {
		r.replicas[r.config.NodeID].committed = r.raftLog.committed
	}

	if r.state == stateLeader && !r.readOnly.committed && isCommit {
		if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) == r.term {
			r.readOnly.commit(r.raftLog.committed)
		}
		r.bcastReadOnly()
	}

	return isCommit
}

func (r *raftFsm) bcastAppend() {
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *raftFsm) sendAppend(to uint64) {
	pr := r.replicas[to]
	if pr.isPaused() {
		return
	}

	var (
		term       uint64
		ents       []*proto.Entry
		errt, erre error
		m          *proto.Message
	)
	fi := r.raftLog.firstIndex()
	if pr.next >= fi {
		term, errt = r.raftLog.term(pr.next - 1)
		ents, erre = r.raftLog.entries(pr.next, r.config.MaxSizePerMsg)
	}
	if pr.next < fi || errt != nil || erre != nil {
		if !pr.active {
			if logger.IsEnableDebug() {
				logger.Debug("[raft->sendAppend][%v]ignore sending snapshot to %v since it is not recently active.", r.id, to)
			}
			return
		}

		snapshot, err := r.sm.Snapshot()
		if err != nil || snapshot.ApplyIndex() < fi-1 {
			panic(AppPanicError(fmt.Sprintf("[raft->sendAppend][%v]failed to send snapshot[%d] to %v because snapshot is unavailable, error is: \r\n%v", r.id, snapshot.ApplyIndex(), to, err)))
		}

		m = proto.GetMessage()
		m.Type = proto.ReqMsgSnapShot
		m.To = to
		m.Snapshot = snapshot
		snapMeta := proto.SnapshotMeta{Index: snapshot.ApplyIndex(), Peers: make([]proto.Peer, 0, len(r.replicas))}
		if snapTerm, err := r.raftLog.term(snapMeta.Index); err != nil {
			panic(AppPanicError(fmt.Sprintf("[raft->sendAppend][%v]failed to send snapshot to %v because snapshot is unavailable, error is: \r\n%v", r.id, to, err)))
		} else {
			snapMeta.Term = snapTerm
		}
		for _, p := range r.replicas {
			snapMeta.Peers = append(snapMeta.Peers, p.peer)
		}
		m.SnapshotMeta = snapMeta
		pr.becomeSnapshot(snapMeta.Index)

		if logger.IsEnableDebug() {
			logger.Debug("[raft->sendAppend][%v][firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to [%v][%s]",
				r.id, fi, r.raftLog.committed, snapMeta.Index, snapMeta.Term, to, pr)
		}
	} else {
		m = proto.GetMessage()
		m.Type = proto.ReqMsgAppend
		m.To = to
		m.Index = pr.next - 1
		m.LogTerm = term
		m.Commit = r.raftLog.committed
		m.Entries = append(m.Entries, ents...)

		if n := len(m.Entries); n != 0 {
			switch pr.state {
			case replicaStateReplicate:
				last := m.Entries[n-1].Index
				pr.update(last)
				pr.inflight.add(last)
			case replicaStateProbe:
				pr.pause()
			default:
				errMsg := fmt.Sprintf("[repl->sendAppend][%v] is sending append in unhandled state %s.", r.id, pr.state)
				logger.Error(errMsg)
				panic(AppPanicError(errMsg))
			}
		}
	}
	pr.pending = true
	r.send(m)
}

func (r *raftFsm) appendEntry(es ...*proto.Entry) {
	r.raftLog.append(es...)
	r.replicas[r.config.NodeID].maybeUpdate(r.raftLog.lastIndex(), r.raftLog.committed)
	r.maybeCommit()
}

func (r *raftFsm) bcastReadOnly() {
	index := r.readOnly.lastPending()
	if index == 0 {
		return
	}
	if logger.IsEnableDebug() {
		logger.Debug("raft[%d] bcast readonly index: %d", r.id, index)
	}
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		msg := proto.GetMessage()
		msg.Type = proto.ReqCheckQuorum
		msg.To = id
		msg.Index = index
		r.send(msg)
	}
}
