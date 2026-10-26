package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var addr string

var rootCmd = &cobra.Command{
	Use:   "ghostkv-cli",
	Short: "CLI client for GhostKV distributed key-value store",
}

func init() {
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "127.0.0.1:7001", "GhostKV server address")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
