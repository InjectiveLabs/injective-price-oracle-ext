package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	exchangetypes "github.com/InjectiveLabs/sdk-go/chain/exchange/types"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
	log "github.com/InjectiveLabs/suplog"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	cli "github.com/jawher/mow.cli"
	"github.com/pkg/errors"
	"github.com/xlab/closer"

	chainclient "github.com/InjectiveLabs/sdk-go/client/chain"
	"github.com/InjectiveLabs/sdk-go/client/common"

	"github.com/InjectiveLabs/injective-price-oracle/oracle"
	"github.com/InjectiveLabs/injective-price-oracle/pipeline"
)

// oracleCmd action runs the service
//
// $ injective-price-oracle start
func oracleCmd(cmd *cli.Cmd) {
	var (
		// Cosmos params
		cosmosChainID   *string
		cosmosGRPC      *string
		tendermintRPC   *string
		cosmosGasPrices *string
		networkNode     *string

		// Cosmos Key Management
		cosmosKeyringDir     *string
		cosmosKeyringAppName *string
		cosmosKeyringBackend *string

		cosmosKeyFrom       *string
		cosmosKeyPassphrase *string
		cosmosPrivKey       *string
		cosmosUseLedger     *bool

		// External Feeds params
		feedsDir       *string
		binanceBaseURL *string

		// Metrics
		statsdPrefix   *string
		statsdAddr     *string
		statsdAgent    *string
		statsdStuckDur *string
		statsdMocking  *string
		statsdDisabled *string

		// Stork Oracle websocket params
		websocketUrl              *string
		websocketHeader           *string
		websocketSubscribeMessage *string
	)

	initCosmosOptions(
		cmd,
		&cosmosChainID,
		&cosmosGRPC,
		&tendermintRPC,
		&cosmosGasPrices,
		&networkNode,
	)

	initCosmosKeyOptions(
		cmd,
		&cosmosKeyringDir,
		&cosmosKeyringAppName,
		&cosmosKeyringBackend,
		&cosmosKeyFrom,
		&cosmosKeyPassphrase,
		&cosmosPrivKey,
		&cosmosUseLedger,
	)

	initExternalFeedsOptions(
		cmd,
		&binanceBaseURL,
		&feedsDir,
	)

	initStatsdOptions(
		cmd,
		&statsdPrefix,
		&statsdAddr,
		&statsdAgent,
		&statsdStuckDur,
		&statsdMocking,
		&statsdDisabled,
	)

	initStorkOracleWebSocket(
		cmd,
		&websocketUrl,
		&websocketHeader,
		&websocketSubscribeMessage,
	)

	cmd.Action = func() {
		ctx := context.Background()
		// ensure a clean exit
		defer closer.Close()

		startMetricsGathering(
			statsdPrefix,
			statsdAddr,
			statsdAgent,
			statsdStuckDur,
			statsdMocking,
			statsdDisabled,
		)

		if *cosmosUseLedger {
			log.Fatalln("cannot really use Ledger for oracle service loop, since signatures msut be realtime")
		}

		networkNodeSplit := strings.Split(*networkNode, ",")
		networkStr, node := networkNodeSplit[0], networkNodeSplit[1]
		network := common.LoadNetwork(networkStr, node)

		senderAddress, cosmosKeyring, err := chainclient.InitCosmosKeyring(
			*cosmosKeyringDir,
			*cosmosKeyringAppName,
			*cosmosKeyringBackend,
			*cosmosKeyFrom,
			*cosmosKeyPassphrase,
			*cosmosPrivKey,
			*cosmosUseLedger,
		)
		if err != nil {
			log.WithError(err).Fatalln("failed to init Cosmos keyring")
		}

		log.Infoln("using Injective Sender", senderAddress.String())
		clientCtx, err := chainclient.NewClientContext(network.ChainId, senderAddress.String(), cosmosKeyring)
		if err != nil {
			log.WithError(err).Fatalln("failed to initialize cosmos client context")
		}
		clientCtx = clientCtx.WithNodeURI(network.TmEndpoint)
		tmRPC, err := rpchttp.New(network.TmEndpoint, "/websocket")
		if err != nil {
			log.WithError(err).Fatalln("failed to connect to tendermint RPC")
		}
		clientCtx = clientCtx.WithClient(tmRPC)
		cosmosClient, err := chainclient.NewChainClient(clientCtx, network, common.OptionGasPrices(*cosmosGasPrices))
		if err != nil {
			log.WithError(err).WithFields(log.Fields{
				"endpoint": *cosmosGRPC,
			}).Fatalln("failed to connect to daemon, is injectived running?")
		}
		closer.Bind(func() {
			cosmosClient.Close()
		})

		log.Infoln("waiting for GRPC services")
		time.Sleep(1 * time.Second)

		daemonWaitCtx, cancelWait := context.WithTimeout(ctx, 10*time.Second)
		defer cancelWait()

		daemonConn := cosmosClient.QueryClient()
		if err := waitForService(daemonWaitCtx, daemonConn); err != nil {
			panic(fmt.Errorf("failed to wait for cosmos client connection: %w", err))
		}

		var storkEnabled bool
		var storkTickers []string

		feedConfigs := make([]*oracle.FeedConfig, 0, 10)
		if len(*feedsDir) > 0 {
			err := filepath.WalkDir(*feedsDir, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				} else if d.IsDir() {
					return nil
				} else if filepath.Ext(path) != ".toml" {
					return nil
				}

				cfgBody, err := os.ReadFile(path)
				if err != nil {
					err = errors.Wrapf(err, "failed to read dynamic feed config")
					return err
				}

				feedCfg, err := oracle.ParseDynamicFeedConfig(cfgBody)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"filename": d.Name(),
					}).Errorln("failed to parse dynamic feed config")
					return nil
				}

				if feedCfg.ProviderName == oracle.FeedProviderStork.String() {
					storkEnabled = true
					storkTickers = append(storkTickers, feedCfg.Ticker)
				}

				feedConfigs = append(feedConfigs, feedCfg)

				return nil
			})

			if err != nil {
				err = errors.Wrapf(err, "feeds dir is specified, but failed to read from it: %s", *feedsDir)
				log.WithError(err).Fatalln("failed to load dynamic feeds")
				return
			}

			log.Infof("found %d dynamic feed configs", len(feedConfigs))
		}

		var storkFetcher oracle.StorkFetcher

		if storkEnabled {
			storkFetcher = oracle.NewStorkFetcher(*websocketSubscribeMessage, storkTickers)
		}

		svc, err := oracle.NewService(
			ctx,
			cosmosClient,
			exchangetypes.NewQueryClient(daemonConn),
			oracletypes.NewQueryClient(daemonConn),
			feedConfigs,
			storkFetcher,
		)
		if err != nil {
			log.Fatalln(err)
		}

		closer.Bind(func() {
			svc.Close()
		})

		go func() {
			if storkFetcher == nil {
				return // no stork feeds
			}
			connectIn := 0 * time.Second
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(connectIn):
				}

				connectIn = 5 * time.Second
				conn, err := pipeline.ConnectWebSocket(ctx, *websocketUrl, *websocketHeader, oracle.MaxRetriesReConnectWebSocket)
				if err != nil {
					log.WithError(err).Errorln("failed to connect to WebSocket")
					continue
				}

				err = storkFetcher.Start(ctx, conn)
				if err != nil {
					log.WithError(err).Errorln("stork fetcher failed")
				}
			}
		}()

		go func() {
			if err := svc.Start(); err != nil {
				log.Errorln(err)

				// signal there that the app failed
				os.Exit(1)
			}
		}()

		closer.Hold()
	}
}
