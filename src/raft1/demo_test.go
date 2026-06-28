package raft

import (
	"log"
	"testing"
	"time"
	"6.5840/tester1"
)

func TestDemo(t *testing.T) {
	servers := 3
	ts := makeTest(t, servers, false, false)
	defer ts.cleanup()

	tester.AnnotateTest("TestDemo", servers)
	ts.Begin("TestDemo")

	log.Printf("\n**** FAZA 1: 3 noduri (normal) ****\n")

	ts.one(101, 3, true)
	ts.one(102, 3, true)
	printState(ts)

	log.Printf("\n**** FAZA 2: izolez liderul ****\n")
	leader := ts.checkOneLeader()
	ts.g.DisconnectAll(leader)
	time.Sleep(2 * time.Second) // timp să aleagă lider nou
	printState(ts)

	log.Printf("\n**** FAZA 3: client pe lider nou (lider vechi inca izolat) ****\n")
	ts.one(201, 2, true)
	ts.one(202, 2, true)
	printState(ts)

	log.Printf("\n**** FAZA 4: vindec partiția ****\n")
	ts.g.ConnectOne(leader)
	time.Sleep(3 * time.Second) // las să se sincronizeze
	printState(ts)

	log.Printf("\n**** Toate replicile sincronizate ****\n")
}

func printState(ts *Test) {
	for i := 0; i < ts.n; i++ {
		term, isLeader := ts.srvs[i].raft.GetState()
		role := "adept"
		if isLeader {
			role = "LIDER"
		}
		ts.srvs[i].mu.Lock()
		applied := ts.srvs[i].logs
		ts.srvs[i].mu.Unlock()
		log.Printf("  N%d: %-8s term=%d  applied=%v", i, role, term, applied)
	}
	log.Printf("")
}
