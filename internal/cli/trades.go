package cli

import (
	"github.com/spf13/cobra"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
	"io"
)

func newTradesCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	var pair string
	cmd := &cobra.Command{Use: "trades", Short: "Stream classic Stellar path-payment trades", Args: cobra.NoArgs,
		Long:    "Extract completed classic path-payment executions and their actual traded amounts from transaction results. Soroban application-specific swap events and offer-book fills are not yet normalized by this command.",
		Example: "  prism trades --pair XLM/USDC --follow\n  prism trades --from 1234567 --json",
		RunE: func(cmd *cobra.Command, args []string) error {
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
			factory := func(start uint32) (stream.Source, error) {
				return stream.NewTradeSource(client, stream.TradeQuery{StartLedger: start, PageLimit: sf.pageSize, Pair: pair})
			}
			return runStream(cmd.Context(), factory, from, &sf, g, stdout, stderr)
		},
	}
	cmd.Flags().StringVar(&pair, "pair", "", "asset pair filter in ASSET/ASSET form")
	sf.register(cmd, stream.DefaultTransactionPageLimit)
	return cmd
}
