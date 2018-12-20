// +build !invoicesrpc

package main

import "github.com/urfave/cli"

// invoicesCommands will return nil for non-autopilotrpc builds.
func invoicesCommands() []cli.Command {
	return nil
}
