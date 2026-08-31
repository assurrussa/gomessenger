package main

import (
	"flag"
	"fmt"
	"os"

	"example.com/gomessenger-durable-postgres-nats/internal/batchproof"
)

func main() {
	dir := flag.String("dir", "", "directory containing manifest.json")
	flag.Parse()
	if *dir == "" {
		_, _ = fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	proof, err := batchproof.EvaluateDir(*dir)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "evaluate batch proof: %v\n", err)
		os.Exit(2)
	}
	if err := batchproof.Write(*dir, proof); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "write batch proof: %v\n", err)
		os.Exit(2)
	}
	if !proof.Passed {
		_, _ = fmt.Fprintf(os.Stderr, "checkout-workspace batch advantage not proven; see %s/proof.md\n", *dir)
		os.Exit(1)
	}
	if _, err := fmt.Fprintf(os.Stdout, "checkout-workspace batch advantage proven; see %s/proof.md\n", *dir); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "write success message: %v\n", err)
		os.Exit(2)
	}
}
