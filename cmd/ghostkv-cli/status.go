package main

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/alyelalwany/ghostkv/pkg/client"
	"github.com/spf13/cobra"
)

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		c, err := client.New(addr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect: %v\n", err)
			os.Exit(1)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		status, err := c.Status(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "status: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Replication factor: %d\n", status.GetReplicationFactor())
		fmt.Printf("Write quorum:       %d\n", status.GetWriteQuorum())
		fmt.Printf("Read quorum:        %d\n", status.GetReadQuorum())
		fmt.Println()

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "NODE\tADDRESS\tHEALTHY\tVNODES\tKEYS")
		for _, n := range status.GetNodes() {
			healthy := "yes"
			if !n.GetHealthy() {
				healthy = "no"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\n",
				n.GetNodeId(),
				n.GetAddress(),
				healthy,
				n.GetVirtualNodes(),
				n.GetKeyCount(),
			)
		}
		w.Flush()
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
