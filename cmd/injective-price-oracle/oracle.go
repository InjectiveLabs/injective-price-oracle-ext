package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	exchangetypes "github.com/InjectiveLabs/sdk-go/chain/exchange/types"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	"github.com/gorilla/websocket"
	cli "github.com/jawher/mow.cli"
	"github.com/pkg/errors"
	"github.com/xlab/closer"
	log "github.com/xlab/suplog"

	chainclient "github.com/InjectiveLabs/sdk-go/client/chain"
	"github.com/InjectiveLabs/sdk-go/client/common"

	"github.com/InjectiveLabs/injective-price-oracle/oracle"
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
		dynamicFeedsDir *string
		binanceBaseURL  *string
		storkFeedsDir   *string

		// Metrics
		statsdPrefix   *string
		statsdAddr     *string
		statsdStuckDur *string
		statsdMocking  *string
		statsdDisabled *string

		// Stork Oracle websocket params
		websocketUrl    *string
		websocketHeader *string
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
		&dynamicFeedsDir,
		&storkFeedsDir,
	)

	initStatsdOptions(
		cmd,
		&statsdPrefix,
		&statsdAddr,
		&statsdStuckDur,
		&statsdMocking,
		&statsdDisabled,
	)

	initStorkOracleWebSocket(
		cmd,
		&websocketUrl,
		&websocketHeader,
	)

	cmd.Action = func() {
		ctx := context.Background()
		// ensure a clean exit
		defer closer.Close()

		startMetricsGathering(
			statsdPrefix,
			statsdAddr,
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
		feedProviderConfigs := map[oracle.FeedProvider]interface{}{
			oracle.FeedProviderBinance: &oracle.BinanceEndpointConfig{
				BaseURL: *binanceBaseURL,
			},
		}

		dynamicFeedConfigs := make([]*oracle.DynamicFeedConfig, 0, 10)
		if len(*dynamicFeedsDir) > 0 {
			err := filepath.WalkDir(*dynamicFeedsDir, func(path string, d fs.DirEntry, err error) error {
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

				dynamicFeedConfigs = append(dynamicFeedConfigs, feedCfg)

				return nil
			})

			if err != nil {
				err = errors.Wrapf(err, "dynamic feeds dir is specified, but failed to read from it: %s", *dynamicFeedsDir)
				log.WithError(err).Fatalln("failed to load dynamic feeds")
				return
			}

			log.Infof("found %d dynamic feed configs", len(dynamicFeedConfigs))
		}

		storkFeedConfigs := make([]*oracle.StorkFeedConfig, 0, 10)
		if len(*storkFeedsDir) > 0 {
			err := filepath.WalkDir(*storkFeedsDir, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				} else if d.IsDir() {
					return nil
				} else if filepath.Ext(path) != ".toml" {
					return nil
				}

				cfgBody, err := os.ReadFile(path)
				if err != nil {
					err = errors.Wrapf(err, "failed to read stork feed config")
					return err
				}

				feedCfg, err := oracle.ParseStorkFeedConfig(cfgBody)
				if err != nil {
					log.WithError(err).WithFields(log.Fields{
						"filename": d.Name(),
					}).Errorln("failed to parse stork feed config")
					return nil
				}

				storkFeedConfigs = append(storkFeedConfigs, feedCfg)

				return nil
			})

			if err != nil {
				err = errors.Wrapf(err, "stork feeds dir is specified, but failed to read from it: %s", *storkFeedsDir)
				log.WithError(err).Fatalln("failed to load stork feeds")
				return
			}

			log.Infof("found %d stork feed configs", len(storkFeedConfigs))
		}

		storkWebsocket, err := ConnectWebSocket(ctx, *websocketUrl, *websocketHeader)
		if err != nil {
			err = errors.Wrapf(err, "can not connect with stork oracle websocket")
			log.WithError(err).Errorln("failed to load stork feeds")
			return
		}
		log.Info("Connected to stork websocket")

		svc, err := oracle.NewService(
			cosmosClient,
			exchangetypes.NewQueryClient(daemonConn),
			oracletypes.NewQueryClient(daemonConn),
			feedProviderConfigs,
			dynamicFeedConfigs,
			storkFeedConfigs,
			storkWebsocket,
		)
		if err != nil {
			log.Fatalln(err)
		}

		closer.Bind(func() {
			svc.Close()
		})

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

func ConnectWebSocket(ctx context.Context, websocketUrl, urlHeader string) (conn *websocket.Conn, err error) {
	u, err := url.Parse(websocketUrl)
	if err != nil {
		return &websocket.Conn{}, errors.Wrapf(err, "can not parse WS url %s: %v", websocketUrl, err)
	}

	header := http.Header{}
	header.Add("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(urlHeader)))

	dialer := websocket.DefaultDialer
	dialer.EnableCompression = true
	retries := 0
	for {
		conn, _, err = websocket.DefaultDialer.DialContext(ctx, u.String(), header)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		} else if err != nil {
			log.Infof("Failed to connect to WebSocket server: %v", err)
			retries++
			if retries > oracle.MaxRetriesReConnectWebSocket {
				log.Infof("Reached maximum retries (%d), exiting...", oracle.MaxRetriesReConnectWebSocket)
				return
			}
			log.Infof("Retrying connect %sth in 5s...", fmt.Sprint(retries))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.NewTimer(5*time.Second).C:
			}
		} else {
			log.Infof("Connected to WebSocket server")
			return
		}
	}
}
