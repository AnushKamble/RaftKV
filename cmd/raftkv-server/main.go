package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"raftkv/internal/config"
	"raftkv/internal/runtime"
)

func main() {
	configPath := flag.String("config", "", "Path to YAML node configuration file")
	flag.Parse()

	if *configPath == "" {
		fmt.Fprintln(os.Stderr, "Error: --config flag is required")
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	node, err := runtime.NewNode(cfg)
	if err != nil {
		log.Fatalf("Failed to initialize RaftKV node %s: %v", cfg.ID, err)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start node %s: %v", cfg.ID, err)
	}

	log.Printf("[RaftKV] Node %s started listening on %s (data: %s)\n", cfg.ID, cfg.Address, cfg.DataDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	sig := <-sigCh
	log.Printf("[RaftKV] Received signal %v, shutting down node %s...\n", sig, cfg.ID)
	node.Stop()
	log.Printf("[RaftKV] Node %s stopped cleanly.\n", cfg.ID)
}
