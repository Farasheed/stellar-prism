package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newSimulateCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf       streamFlags
		envelope string
	)

	cmd := &cobra.Command{
		Use:   "simulate",
		Short: "Continuously simulate a transaction template",
		Long: `Simulate a base64 TransactionEnvelope at each newly observed ledger
head and report changes to its return value, instructions, memory footprint,
and minimum resource fee.`,
		Example: `  prism simulate --tx-envelope AAA... --follow
  prism simulate --tx-envelope AAA... --json --limit 1`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if envelope == "" {
				return fmt.Errorf("--tx-envelope is required")
			}
			if err := sf.validate(); err != nil {
				return err
			}
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()
			from := sf.from
			if from == 0 {
				from, err = latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
			}
			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewSimulationSource(client, stream.SimulationQuery{
					TransactionEnvelope: envelope, StartLedger: startLedger,
				})
			}
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}
	sf.register(cmd, 1)
	cmd.Flags().StringVar(&envelope, "tx-envelope", "", "base64 TransactionEnvelope to simulate")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}
