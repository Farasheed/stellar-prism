package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newOperationsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf             streamFlags
		types          []string
		sourceAccounts []string
	)

	cmd := &cobra.Command{
		Use:   "operations",
		Short: "Stream individual transaction operations",
		Long: `Deconstruct transactions into deterministic operation records.

Use --type and --source-account to narrow the stream, and --follow to keep
watching new ledgers.`,
		Example: `  prism operations --type payment --type invoke_host_function --follow
  prism operations --source-account G... --json`,
		Args: cobra.NoArgs,
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
			newSource := func(startLedger uint32) (stream.Source, error) {
				return stream.NewOperationSource(client, stream.OperationQuery{
					StartLedger: startLedger, PageLimit: sf.pageSize,
					Types: types, SourceAccounts: sourceAccounts,
				})
			}
			return runStream(cmd.Context(), newSource, from, &sf, g, stdout, stderr)
		},
	}
	sf.register(cmd, stream.DefaultTransactionPageLimit)
	cmd.Flags().StringSliceVar(&types, "type", nil, "operation type to include, such as payment or invoke_host_function")
	cmd.Flags().StringSliceVar(&sourceAccounts, "source-account", nil, "operation source account to include; repeatable")
	registerFlagCompletion(cmd, "type", "payment", "invoke_host_function", "change_trust", "manage_sell_offer")
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))
	return cmd
}
