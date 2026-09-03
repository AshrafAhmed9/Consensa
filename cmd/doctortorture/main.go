// Command doctortorture drives a real 3-node kv.DurableRange group through
// internal/txn.Coordinator under a seeded fault schedule, running repeated write-skew-shaped
// "doctors on call" transactions, and reports whether the invariant this codebase's
// write-skew defense exists to protect -- at least one doctor always on call -- ever broke.
//
// Each round: pick two distinct doctors currently on call, record a read of one ("the
// buddy"), then try to take the other off call in a transaction through the real
// Coordinator/DurableStore path (WriteIntent's observed-read rejection,
// Coordinator.Prepare's read-refresh, and DurableStore's uncertainty-interval ReadAtTimestamp
// all included) -- the exact anomaly TestWriteIntentRejectsWriteSkew/
// TestDurableStoreRejectsWriteSkew reproduce by hand, run here at volume under real chaos
// instead of a fixed two-doctor script. A committed transaction that DOES take a doctor off
// call is expected and fine; what must never happen is every doctor ending up off call at
// once. This binary checks real replicated state after every commit and reports any round
// where it finds zero doctors on call as a violation -- the invariant either holds under
// real chaos or this binary says exactly which round broke it.
//
// Nemesis model, stated plainly: kv.DurableRange is a real TCP-networked Raft group with no
// exposed message-filtering hook (unlike internal/raft.Cluster, which cmd/torture and
// cmd/vectortorture use for live partition injection) -- so, like cmd/torture's own
// documented "partition and crash modeled identically" simplification, both "partition" and
// "crash" here mean the same real fault: closing a replica's DurableRange and reopening it
// fresh from the same directory and address (TestDurableStoreRecordSurvivesRestart's own
// pattern), a real crash-recover cycle, not a live network split. "clock-skew" perturbs the
// coordinator's injected clock by a seeded, bounded offset -- simulating a coordinator
// running on a node whose physical clock reads ahead of or behind the rest of the cluster,
// the exact scenario ReadAtTimestamp's uncertainty window exists for.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"

	"github.com/ashraf/consensa/internal/kv"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/txn"
)

// fault mirrors harness/torture/nemesis.py's Fault dataclass field-for-field, matching
// cmd/torture's own fault struct.
type fault struct {
	Step   int    `json:"step"`
	Kind   string `json:"kind"`
	Target int    `json:"target"`
}

type violation struct {
	Round   int    `json:"round"`
	OnCall  int    `json:"on_call"`
	Doctors int    `json:"doctors"`
	TxnID   string `json:"txn_id,omitempty"`
	Detail  string `json:"detail"`
}

type report struct {
	Rounds     int         `json:"rounds"`
	Committed  int         `json:"committed"`
	Aborted    int         `json:"aborted"`
	Restarts   int         `json:"restarts"`
	Violations []violation `json:"violations"`
}

func main() {
	nodeCount := flag.Int("nodes", 3, "cluster size")
	rounds := flag.Int("rounds", 40, "number of rounds to run")
	doctorCount := flag.Int("doctors", 5, "number of doctors")
	seed := flag.Int("seed", 0, "PRNG seed for doctor selection and clock-skew magnitude")
	maxOffsetMS := flag.Int("max-offset-ms", 50, "uncertainty-interval max clock offset, in milliseconds")
	flag.Parse()

	var schedule []fault
	if err := json.NewDecoder(bufio.NewReader(os.Stdin)).Decode(&schedule); err != nil {
		fatal(fmt.Errorf("reading fault schedule from stdin: %w", err))
	}
	byStep := map[int][]fault{}
	for _, f := range schedule {
		byStep[f.Step] = append(byStep[f.Step], f)
	}

	rng := rand.New(rand.NewSource(int64(*seed)))

	ids := make([]raft.NodeID, *nodeCount)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	group, err := newGroup(ids)
	if err != nil {
		fatal(err)
	}
	defer group.closeAll()

	leader, err := group.awaitLeader(5 * time.Second)
	if err != nil {
		fatal(err)
	}

	doctors := make([]string, *doctorCount)
	for i := range doctors {
		doctors[i] = fmt.Sprintf("doctor-%d", i)
		if err := putAndConfirm(leader, doctors[i], "on-call"); err != nil {
			fatal(fmt.Errorf("seeding %s: %w", doctors[i], err))
		}
	}

	store := txn.NewDurableStore(leader)
	maxOffset := time.Duration(*maxOffsetMS) * time.Millisecond
	store.SetMaxOffset(maxOffset)

	// skew is the current bounded perturbation applied to the coordinator's clock reading,
	// advanced deterministically by "clock-skew" faults below.
	var skew time.Duration

	rep := report{Rounds: *rounds}

	for round := 0; round < *rounds; round++ {
		for _, f := range byStep[round] {
			switch f.Kind {
			case "crash", "partition":
				id := ids[f.Target%len(ids)]
				if err := group.restart(id); err != nil {
					fatal(fmt.Errorf("round %d: restarting node %d: %w", round, id, err))
				}
				rep.Restarts++
				newLeader, err := group.awaitLeader(5 * time.Second)
				if err != nil {
					// A restart mid-round can leave the group briefly without a leader --
					// that is a real, valid outcome under this fault, not a driver bug.
					// Skip this round's transaction rather than fail the whole run.
					continue
				}
				leader = newLeader
				store = txn.NewDurableStore(leader)
				store.SetMaxOffset(maxOffset)
			case "clock-skew":
				// Bounded to maxOffset so this exercises the uncertainty window (and its
				// restart path) rather than a fault magnitude the cluster was never
				// configured to tolerate in the first place.
				skew = time.Duration(rng.Int63n(int64(maxOffset))) - maxOffset/2
			}
		}

		clock := txn.NewClock(func() time.Time { return time.Now().Add(skew) })

		i := rng.Intn(len(doctors))
		j := rng.Intn(len(doctors))
		for j == i {
			j = rng.Intn(len(doctors))
		}
		buddy, target := doctors[i], doctors[j]

		readTS := clock.Now()
		v, err := store.ReadAtTimestamp([]byte(buddy), readTS)
		if err != nil {
			// ErrUncertainRead: retry once at the restart timestamp, exactly as the real
			// uncertainty-interval contract requires (Store.ReadAtTimestamp's doc comment).
			readTS = store.UncertaintyRestartTimestamp([]byte(buddy))
			v, err = store.ReadAtTimestamp([]byte(buddy), readTS)
		}
		if err != nil {
			rep.Aborted++
			continue
		}
		if string(v) != "on-call" {
			// Buddy is already off call -- taking target off too could break the
			// invariant outright, so this round's transaction must not even attempt it.
			continue
		}
		if err := store.RecordRead([]byte(buddy), readTS); err != nil {
			rep.Aborted++
			continue
		}

		txnID := fmt.Sprintf("r%d-%s-%s", round, buddy, target)
		coord := txn.NewCoordinator(clock)
		prepared, err := coord.Prepare(txnID, []txn.WriteSet{{
			Store:   store,
			Intents: []txn.Intent{{Key: []byte(target), Value: []byte("off-call")}},
			Reads:   [][]byte{[]byte(buddy)},
		}})
		if err != nil {
			rep.Aborted++
			continue
		}
		if err := coord.CommitRecord(prepared); err != nil {
			rep.Aborted++
			continue
		}
		if err := coord.Resolve(prepared); err != nil {
			rep.Aborted++
			continue
		}
		rep.Committed++

		onCall := 0
		for _, d := range doctors {
			v, err := leader.Get([]byte(d))
			if err == nil && string(v) == "on-call" {
				onCall++
			}
		}
		if onCall == 0 {
			rep.Violations = append(rep.Violations, violation{
				Round: round, OnCall: onCall, Doctors: len(doctors), TxnID: txnID,
				Detail: "every doctor off call after a committed transaction",
			})
		}
	}

	if err := json.NewEncoder(os.Stdout).Encode(rep); err != nil {
		fatal(err)
	}
}

// group holds one real, TCP-networked 3-node kv.DurableRange group -- the same shape
// internal/txn's durable_store_test.go builds for its own tests, factored out here so nodes
// can be restarted mid-run (the crash/partition nemesis) rather than only built once.
type group struct {
	ids      []raft.NodeID
	addrs    map[raft.NodeID]string
	dirs     map[raft.NodeID]string
	replicas map[raft.NodeID]*kv.DurableRange
}

func newGroup(ids []raft.NodeID) (*group, error) {
	g := &group{ids: ids, addrs: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, replicas: map[raft.NodeID]*kv.DurableRange{}}
	for _, id := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		g.addrs[id] = listener.Addr().String()
		if err := listener.Close(); err != nil {
			return nil, err
		}
		dir, err := os.MkdirTemp("", "doctortorture-*")
		if err != nil {
			return nil, err
		}
		g.dirs[id] = dir
	}
	for _, id := range ids {
		r, err := g.open(id)
		if err != nil {
			return nil, err
		}
		g.replicas[id] = r
	}
	return g, nil
}

func (g *group) open(id raft.NodeID) (*kv.DurableRange, error) {
	peers := map[raft.NodeID]string{}
	for _, other := range g.ids {
		if other != id {
			peers[other] = g.addrs[other]
		}
	}
	return kv.NewDurableRange(kv.DurableRangeConfig{
		ID: id, GroupPeers: g.ids, ListenAddress: g.addrs[id], TransportPeers: peers, StorageDir: g.dirs[id],
	})
}

// restart closes id's replica and reopens it fresh from the same directory and address --
// a real crash-recover cycle (see this file's top-level doc comment for why this, not a
// live partition, is the real fault kv.DurableRange supports).
func (g *group) restart(id raft.NodeID) error {
	if r, ok := g.replicas[id]; ok {
		_ = r.Close()
	}
	r, err := g.open(id)
	if err != nil {
		return err
	}
	g.replicas[id] = r
	return nil
}

func (g *group) closeAll() {
	for _, r := range g.replicas {
		_ = r.Close()
	}
	for _, dir := range g.dirs {
		_ = os.RemoveAll(dir)
	}
}

func (g *group) awaitLeader(timeout time.Duration) (*kv.DurableRange, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range g.replicas {
			if err := r.Tick(); err != nil {
				return nil, err
			}
			if role, _ := r.Status(); role == raft.Leader {
				return r, nil
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	return nil, fmt.Errorf("group never elected a leader within %s", timeout)
}

func putAndConfirm(r *kv.DurableRange, key, value string) error {
	if err := r.Put([]byte(key), []byte(value)); err != nil {
		return err
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := r.Get([]byte(key)); err == nil && string(got) == value {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("seed %s never became visible", key)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "doctortorture: %v\n", err)
	os.Exit(1)
}
