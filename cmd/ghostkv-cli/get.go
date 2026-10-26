package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alyelalwany/ghostkv/pkg/client"
	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Retrieve the value for a key",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		c, err := client.New(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		value, found, err := c.Get(ctx, args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "get: %v\n", err)
			os.Exit(1)
		}
		if !found {
			fmt.Println("not found")
			os.Exit(1)
		}
		fmt.Println(string(value))
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
}
