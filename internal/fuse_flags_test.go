package internal

import (
	"testing"

	"github.com/urfave/cli"
)

func TestFuseCapabilityFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want [3]bool
	}{
		{name: "defaults"},
		{name: "direct IO", args: []string{"--async-dio"}, want: [3]bool{true, false, false}},
		{name: "buffered reads", args: []string{"--async-reads"}, want: [3]bool{false, true, false}},
		{name: "directory attributes", args: []string{"--readdirplus"}, want: [3]bool{false, false, true}},
		{name: "combined", args: []string{"--async-dio", "--async-reads", "--readdirplus"}, want: [3]bool{true, true, true}},
		{name: "explicitly disabled", args: []string{"--async-dio=false", "--async-reads=false", "--readdirplus=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := NewApp()
			called := false
			app.Action = func(c *cli.Context) error {
				called = true
				flags := PopulateFlags(c)
				got := [3]bool{flags.EnableAsyncDIO, flags.EnableAsyncReads, flags.EnableReaddirplus}
				if got != tc.want {
					t.Fatalf("FUSE flags = %v; want %v", got, tc.want)
				}
				return nil
			}
			args := append([]string{"goofys"}, tc.args...)
			args = append(args, "bucket", t.TempDir())
			if err := app.Run(args); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("CLI action was not invoked")
			}
		})
	}
}
