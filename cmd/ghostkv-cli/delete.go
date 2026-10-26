package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alyelalwany/ghostkv/pkg/client"
	"github.com/spf13/cobra"
)

var deleteCmd = &cobra.Command{
	Use:   "delete <key>",
	Short: "Delete a key",
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

		deleted, err := c.Delete(ctx, args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "delete: %v\n", err)
			os.Exit(1)
		}
		if deleted {
			fmt.Println("deleted")
		} else {
			fmt.Println("key not found")
		}
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}
