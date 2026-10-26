package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alyelalwany/ghostkv/pkg/client"
	"github.com/spf13/cobra"
)

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Store a key-value pair",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		c, err := client.New(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		acks, err := c.Put(ctx, args[0], []byte(args[1]))
		if err != nil {
			fmt.Fprintf(os.Stderr, "put: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("OK (%d acks)\n", acks)
	},
}

func init() {
	rootCmd.AddCommand(putCmd)
}
