package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"raftkv/internal/client"
)

func main() {
	endpointsFlag := flag.String("endpoints", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053", "Comma-separated list of cluster addresses")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "Operation timeout")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	endpoints := strings.Split(*endpointsFlag, ",")
	cli := client.NewClient(endpoints)
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()

	cmd := args[0]
	switch cmd {
	case "put":
		if len(args) < 3 {
			fmt.Println("Usage: raftkv-cli put <key> <value>")
			os.Exit(1)
		}
		key, value := args[1], args[2]
		if err := cli.Put(ctx, key, value); err != nil {
			fmt.Fprintf(os.Stderr, "Error putting %s: %v\n", key, err)
			os.Exit(1)
		}
		fmt.Printf("OK: %s = %s\n", key, value)

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: raftkv-cli get <key>")
			os.Exit(1)
		}
		key := args[1]
		val, found, err := cli.Get(ctx, key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting %s: %v\n", key, err)
			os.Exit(1)
		}
		if !found {
			fmt.Printf("NOT_FOUND: key %s does not exist\n", key)
		} else {
			fmt.Printf("VALUE: %s\n", val)
		}

	case "delete":
		if len(args) < 2 {
			fmt.Println("Usage: raftkv-cli delete <key>")
			os.Exit(1)
		}
		key := args[1]
		found, err := cli.Delete(ctx, key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting %s: %v\n", key, err)
			os.Exit(1)
		}
		if found {
			fmt.Printf("OK: deleted key %s\n", key)
		} else {
			fmt.Printf("NOT_FOUND: key %s was not present\n", key)
		}

	case "cas":
		if len(args) < 4 {
			fmt.Println("Usage: raftkv-cli cas <key> <expected> <new>")
			os.Exit(1)
		}
		key, expected, newValue := args[1], args[2], args[3]
		swapped, currentVal, err := cli.CompareAndSwap(ctx, key, expected, newValue)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error in CAS for %s: %v\n", key, err)
			os.Exit(1)
		}
		if swapped {
			fmt.Printf("OK: swapped %s from %s to %s\n", key, expected, newValue)
		} else {
			fmt.Printf("MISMATCH: current value is %q, expected was %q\n", currentVal, expected)
		}

	case "status":
		target := endpoints[0]
		if len(args) >= 2 {
			target = args[1]
		}
		st, err := cli.GetStatus(ctx, target)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching status from %s: %v\n", target, err)
			os.Exit(1)
		}
		fmt.Printf("Node: %s\nRole: %s\nTerm: %d\nLeader: %s\nCommitIndex: %d\nLastApplied: %d\nLastLogIndex: %d\n",
			st.NodeId, st.Role, st.Term, st.LeaderId, st.CommitIndex, st.LastApplied, st.LastLogIndex)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("RaftKV CLI - Command-Line Interface for Raft Replicated Key-Value Store")
	fmt.Println("\nCommands:")
	fmt.Println("  put <key> <value>            Write key-value pair")
	fmt.Println("  get <key>                    Retrieve value by key")
	fmt.Println("  delete <key>                 Delete key")
	fmt.Println("  cas <key> <expected> <new>   Compare-and-swap key value")
	fmt.Println("  status [node-addr]           Query node status and consensus state")
	fmt.Println("\nFlags:")
	fmt.Println("  --endpoints                  Comma-separated list of node addresses")
	fmt.Println("  --timeout                    Command timeout (default 5s)")
}
