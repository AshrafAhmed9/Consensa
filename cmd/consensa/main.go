// Command consensa starts a gRPC API process backed by an in-memory Raft group.
package main

import (
	"flag"
	"fmt"
	consensav1 "github.com/ashraf/consensa/api/consensa/v1"
	"github.com/ashraf/consensa/internal/ann"
	"github.com/ashraf/consensa/internal/metrics"
	"github.com/ashraf/consensa/internal/raft"
	"github.com/ashraf/consensa/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
)

func main() {
	listen := flag.String("listen", ":8080", "gRPC listen address")
	dimension := flag.Int("dimension", 3, "fixed collection vector dimension")
	replicas := flag.Int("replicas", 3, "in-memory Raft replicas")
	metricsListen := flag.String("metrics-listen", ":9090", "Prometheus metrics listen address")
	flag.Parse()
	if *replicas < 1 {
		log.Fatal("replicas must be positive")
	}
	ids := make([]raft.NodeID, *replicas)
	for i := range ids {
		ids[i] = raft.NodeID(i + 1)
	}
	index, err := ann.NewReplicatedIndex(ids, ann.Config{Dimension: *dimension, M: 16, EFConstruction: 64, EFSearch: 64, Seed: 1})
	if err != nil {
		log.Fatal(err)
	}
	metricRegistry := metrics.NewRegistry()
	if _, term, elected := index.Status(); elected {
		metricRegistry.RaftTerm.Set(float64(term))
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(metricRegistry.Registry, promhttp.HandlerOpts{}))
		if err := http.ListenAndServe(*metricsListen, mux); err != nil {
			log.Printf("metrics server stopped: %v", err)
		}
	}()
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	g := grpc.NewServer()
	consensav1.RegisterConsensaServer(g, server.NewService(index))
	log.Printf("consensa listening on %s; metrics on %s/metrics", *listen, fmt.Sprintf("http://%s", *metricsListen))
	log.Fatal(g.Serve(l))
}
