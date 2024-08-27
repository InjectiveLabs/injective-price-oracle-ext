package oracle

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"cosmossdk.io/math"

	cosmtypes "github.com/cosmos/cosmos-sdk/types"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	log "github.com/xlab/suplog"

	"github.com/InjectiveLabs/metrics"
	exchangetypes "github.com/InjectiveLabs/sdk-go/chain/exchange/types"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
	chainclient "github.com/InjectiveLabs/sdk-go/client/chain"
	"github.com/gorilla/websocket"
)

type Service interface {
	Start() error
	Close()
}

type PricePuller interface {
	Provider() FeedProvider
	ProviderName() string
	Symbol() string
	Interval() time.Duration

	// PullPrice method must be implemented in order to get a price
	// from external source, handled by PricePuller.
	PullPrice(ctx context.Context) (price decimal.Decimal, err error)
	OracleType() oracletypes.OracleType
}

type MultiPricePuller interface {
	PricePuller

	AddSymbol(symbol string)
	Symbols() []string

	// PullPrices is a method that allows to pull multiple prices in a single batch.
	PullPrices(ctx context.Context) (prices map[string]decimal.Decimal, err error)
}

type oracleSvc struct {
	pricePullers        map[string]PricePuller
	supportedPriceFeeds map[string]PriceFeedConfig
	feedProviderConfigs map[FeedProvider]interface{}
	cosmosClient        chainclient.ChainClient
	exchangeQueryClient exchangetypes.QueryClient
	oracleQueryClient   oracletypes.QueryClient
	storkWebsocket      *websocket.Conn

	logger  log.Logger
	svcTags metrics.Tags
}

const (
	maxRespTime                  = 15 * time.Second
	maxRespHeadersTime           = 15 * time.Second
	maxRespBytes                 = 10 * 1024 * 1024
	maxTxStatusRetries           = 3
	maxRetriesPerInterval        = 3
	MaxRetriesReConnectWebSocket = 5
)

var (
	zeroPrice = decimal.Decimal{}
)

type FeedProvider string

const (
	FeedProviderDynamic FeedProvider = "_"
	FeedProviderBinance FeedProvider = "binance"
	FeedProviderStork   FeedProvider = "stork"

	// TODO: add your native implementations here
)

type PriceFeedConfig struct {
	Symbol        string
	FeedProvider  FeedProvider
	PullInterval  time.Duration
	DynamicConfig *DynamicFeedConfig
}

// getEnabledFeeds returns a mapping between ticker and price feeder config, this will query
// the chain to fetch only those configs where current Cosmos sender is authorized as a relayer.
func (s *oracleSvc) getEnabledFeeds() map[string]PriceFeedConfig {
	metrics.ReportFuncCall(s.svcTags)
	doneFn := metrics.ReportFuncTiming(s.svcTags)
	defer doneFn()

	ctx, cancelFn := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancelFn()

	feeds := make(map[string]PriceFeedConfig)

	sender := strings.ToLower(s.cosmosClient.FromAddress().String())
	res, err := s.oracleQueryClient.PriceFeedPriceStates(ctx, &oracletypes.QueryPriceFeedPriceStatesRequest{})
	if err != nil {
		metrics.ReportFuncError(s.svcTags)
		return nil
	}

	for _, priceFeedState := range res.PriceStates {
		var found bool
		for _, relayer := range priceFeedState.Relayers {
			if strings.ToLower(relayer) == sender {
				found = true
			}
		}

		if !found {
			continue
		}

		ticker := fmt.Sprintf("%s/%s", priceFeedState.Base, priceFeedState.Quote)
		if feed, ok := s.supportedPriceFeeds[ticker]; ok {
			feeds[ticker] = feed
		} else {
			s.logger.WithFields(log.Fields{
				"sender": sender,
			}).Warningf("current sender is authorized in %s feed, but no corresponding feed config loaded", ticker)
		}
	}

	s.logger.Infof("got %d enabled price feeds", len(feeds))

	return feeds
}

func NewService(
	cosmosClient chainclient.ChainClient,
	exchangeQueryClient exchangetypes.QueryClient,
	oracleQueryClient oracletypes.QueryClient,
	feedProviderConfigs map[FeedProvider]interface{},
	dynamicFeedConfigs []*DynamicFeedConfig,
	storkFeedConfigs []*StorkFeedConfig,
	storkWebsocket *websocket.Conn,
) (Service, error) {
	svc := &oracleSvc{
		cosmosClient:        cosmosClient,
		exchangeQueryClient: exchangeQueryClient,
		oracleQueryClient:   oracleQueryClient,
		storkWebsocket:      storkWebsocket,

		feedProviderConfigs: feedProviderConfigs,
		logger:              log.WithField("svc", "oracle"),
		svcTags: metrics.Tags{
			"svc": "price_oracle",
		},
	}

	// supportedPriceFeeds is a mapping between price ticker and its pricefeed config
	svc.supportedPriceFeeds = map[string]PriceFeedConfig{}
	for _, feedCfg := range dynamicFeedConfigs {
		svc.supportedPriceFeeds[feedCfg.Ticker] = PriceFeedConfig{
			FeedProvider:  FeedProviderDynamic,
			DynamicConfig: feedCfg,
		}
	}

	svc.pricePullers = map[string]PricePuller{}
	for _, feedCfg := range dynamicFeedConfigs {
		ticker := feedCfg.Ticker
		pricePuller, err := NewDynamicPriceFeed(feedCfg)
		if err != nil {
			err = errors.Wrapf(err, "failed to init dynamic price feed for ticker %s", ticker)
			return nil, err
		}
		svc.pricePullers[ticker] = pricePuller
	}
    // Warning: The logic for setting price pullers below will be overwrite ticket if it was already used previously (dynamicFeedConfigs)
	for _, feedCfg := range storkFeedConfigs {
		ticker := feedCfg.Ticker
		pricePuller, err := NewStorkPriceFeed(feedCfg)
		if err != nil {
			err = errors.Wrapf(err, "failed to init stork price feed for ticker %s", ticker)
			return nil, err
		}
		svc.pricePullers[ticker] = pricePuller
	}

	svc.logger.Infof("initialized %d price pullers", len(svc.pricePullers))
	return svc, nil
}

func (s *oracleSvc) Start() (err error) {
	defer s.panicRecover(&err)

	if len(s.pricePullers) > 0 {
		s.logger.Infoln("starting pullers for", len(s.pricePullers), "feeds")

		dataC := make(chan *PriceData, len(s.pricePullers))

		for ticker, pricePuller := range s.pricePullers {
			switch pricePuller.Provider() {
			case FeedProviderBinance, FeedProviderStork,
				FeedProviderDynamic:

				go s.processSetPriceFeed(ticker, pricePuller.ProviderName(), pricePuller, dataC)

			default:
				s.logger.WithField("provider", pricePuller.Provider()).Warningln("unsupported price feed provider")
			}
		}

		s.commitSetPrices(dataC)
	}

	return
}

func (s *oracleSvc) processSetPriceFeed(ticker, providerName string, pricePuller PricePuller, dataC chan<- *PriceData) {
	feedLogger := s.logger.WithFields(log.Fields{
		"ticker":   ticker,
		"provider": pricePuller.ProviderName(),
	})

	symbol := pricePuller.Symbol()

	t := time.NewTimer(5 * time.Second)
	for {
		select {
		case <-t.C:
			ctx, cancelFn := context.WithTimeout(context.Background(), maxRespTime)
			defer cancelFn()
			// define price and asset pair to tracking late
			var err error
			price := zeroPrice
			var assetPairs []*oracletypes.AssetPair

			if pricePuller.OracleType() == oracletypes.OracleType_Stork {
				storkPricePuller, ok := pricePuller.(*storkPriceFeed)
				if !ok {
					metrics.ReportFuncError(s.svcTags)
					feedLogger.WithFields(log.Fields{
						"oracle":  "Stork Oracle",
						"retries": maxRetriesPerInterval,
					}).WithError(err).Errorln("can not convert to stork price feed")
					continue
				}
				assetPairs, err = storkPricePuller.PullAssetPairs(s.storkWebsocket)
				if err != nil {
					metrics.ReportFuncError(s.svcTags)
					feedLogger.WithError(err).Warningln("retrying PullAssetPairs after error")

					for i := 0; i < maxRetriesPerInterval; i++ {
						if assetPairs, err = storkPricePuller.PullAssetPairs(s.storkWebsocket); err != nil {
							time.Sleep(time.Second)
							continue
						}
						break
					}

					if err != nil {
						metrics.ReportFuncError(s.svcTags)
						feedLogger.WithFields(log.Fields{
							"retries": maxRetriesPerInterval,
						}).WithError(err).Errorln("failed to fetch asset pairs")

						t.Reset(pricePuller.Interval())
						continue
					}
				}
				if len(assetPairs) == 0 {
					t.Reset(pricePuller.Interval())
					continue
				}
			} else {
				price, err = pricePuller.PullPrice(ctx)
				if err != nil {
					metrics.ReportFuncError(s.svcTags)
					feedLogger.WithError(err).Warningln("retrying PullPrice after error")

					for i := 0; i < maxRetriesPerInterval; i++ {
						if price, err = pricePuller.PullPrice(ctx); err != nil {
							time.Sleep(time.Second)
							continue
						}
						break
					}

					if err != nil {
						metrics.ReportFuncError(s.svcTags)
						feedLogger.WithFields(log.Fields{
							"symbol":  symbol,
							"retries": maxRetriesPerInterval,
						}).WithError(err).Errorln("failed to fetch price")

						t.Reset(pricePuller.Interval())
						continue
					}
				}
			}

			dataC <- &PriceData{
				Ticker:       Ticker(ticker),
				Symbol:       symbol,
				Timestamp:    time.Now().UTC(),
				ProviderName: pricePuller.ProviderName(),
				Price:        price,
				AssetPairs:   assetPairs,
				OracleType:   pricePuller.OracleType(),
			}

			t.Reset(pricePuller.Interval())
		}
	}
}

const (
	commitPriceBatchTimeLimit = 5 * time.Second
	commitPriceBatchSizeLimit = 100
)

func (s *oracleSvc) composePriceFeedMsgs(priceBatch []*PriceData) (results []cosmtypes.Msg) {
	msg := &oracletypes.MsgRelayPriceFeedPrice{
		Sender: s.cosmosClient.FromAddress().String(),
	}

	for _, priceData := range priceBatch {
		if priceData.OracleType != oracletypes.OracleType_PriceFeed {
			continue
		}

		msg.Base = append(msg.Base, priceData.Ticker.Base())
		msg.Quote = append(msg.Quote, priceData.Ticker.Quote())
		msg.Price = append(msg.Price, math.LegacyMustNewDecFromStr(priceData.Price.String()))
	}

	if len(msg.Base) > 0 {
		return []cosmtypes.Msg{msg}
	}

	return nil
}

func (s *oracleSvc) composeProviderFeedMsgs(priceBatch []*PriceData) (result []cosmtypes.Msg) {
	if len(priceBatch) == 0 {
		return nil
	}

	providerToMsg := make(map[string]*oracletypes.MsgRelayProviderPrices)
	for _, priceData := range priceBatch {
		if priceData.OracleType != oracletypes.OracleType_Provider {
			continue
		}

		provider := strings.ToLower(priceData.ProviderName)
		msg, exist := providerToMsg[provider]
		if !exist {
			msg = &oracletypes.MsgRelayProviderPrices{
				Sender:   s.cosmosClient.FromAddress().String(),
				Provider: priceData.ProviderName,
			}
			providerToMsg[provider] = msg
		}

		msg.Symbols = append(msg.Symbols, priceData.Symbol)
		msg.Prices = append(msg.Prices, math.LegacyMustNewDecFromStr(priceData.Price.String()))
	}

	for _, msg := range providerToMsg {
		result = append(result, msg)
	}
	return result
}

func (s *oracleSvc) composeStorkOracleMsgs(priceBatch []*PriceData) (result []cosmtypes.Msg) {
	if len(priceBatch) == 0 {
		return nil
	}

	for _, priceData := range priceBatch {
		if priceData.OracleType != oracletypes.OracleType_Stork {
			continue
		}
		msg := &oracletypes.MsgRelayStorkPrices{
			Sender:     s.cosmosClient.FromAddress().String(),
			AssetPairs: priceData.AssetPairs,
		}
		result = append(result, msg)
	}

	return result
}

func (s *oracleSvc) composeMsgs(priceBatch []*PriceData) (result []cosmtypes.Msg) {
	result = append(result, s.composePriceFeedMsgs(priceBatch)...)
	result = append(result, s.composeProviderFeedMsgs(priceBatch)...)
	result = append(result, s.composeStorkOracleMsgs(priceBatch)...)
	return result
}

func (s *oracleSvc) commitSetPrices(dataC <-chan *PriceData) {
	expirationTimer := time.NewTimer(commitPriceBatchTimeLimit)
	pricesBatch := make([]*PriceData, 0, commitPriceBatchSizeLimit)

	resetBatch := func() []*PriceData {
		expirationTimer.Reset(commitPriceBatchTimeLimit)

		prev := pricesBatch
		pricesBatch = make([]*PriceData, 0, commitPriceBatchSizeLimit)
		return prev
	}

	submitBatch := func(currentBatch []*PriceData, timeout bool) {
		if len(currentBatch) == 0 {
			return
		}

		batchLog := s.logger.WithFields(log.Fields{
			"batch_size": len(currentBatch),
			"timeout":    timeout,
		})

		msgs := s.composeMsgs(currentBatch)
		if len(msgs) == 0 {
			batchLog.Debugf("pipeline composed no messages, so do nothing")
			return
		}

		ts := time.Now()
		txResp, err := s.cosmosClient.SyncBroadcastMsg(msgs...)
		if err != nil {
			metrics.ReportFuncError(s.svcTags)
			batchLog.WithError(err).Errorln("failed to SyncBroadcastMsg")
			return
		}

		if txResp.TxResponse != nil {
			if txResp.TxResponse.Code != 0 {
				batchLog.WithFields(log.Fields{
					"hash":     txResp.TxResponse.TxHash,
					"err_code": txResp.TxResponse.Code,
				}).Errorf("set price Tx error: %s", txResp.String())

				return
			}
			batchLog.WithField("height", txResp.TxResponse.Height).
				WithField("hash", txResp.TxResponse.TxHash).
				Infoln("sent Tx in", time.Since(ts))
		}
	}

	for {
		select {
		case priceData, ok := <-dataC:
			if !ok {
				s.logger.Infoln("stopping committing prices")
				prevBatch := resetBatch()
				submitBatch(prevBatch, false)
				return
			}
			if priceData.OracleType != oracletypes.OracleType_Stork {
				if priceData.Price.IsZero() || priceData.Price.IsNegative() {
					s.logger.WithFields(log.Fields{
						"ticker":   priceData.Ticker,
						"provider": priceData.ProviderName,
					}).Debugln("got negative or zero price, skipping")
					continue
				}
			} else {
				if len(priceData.AssetPairs) == 0 {
					s.logger.WithFields(log.Fields{
						"ticker":   priceData.Ticker,
						"provider": priceData.ProviderName,
					}).Debugln("got zero asset pair for stork oracle, skipping")
					continue
				}
			}
			pricesBatch = append(pricesBatch, priceData)

			if len(pricesBatch) >= commitPriceBatchSizeLimit {
				prevBatch := resetBatch()
				submitBatch(prevBatch, false)
			}
		case <-expirationTimer.C:
			prevBatch := resetBatch()
			submitBatch(prevBatch, true)
		}
	}
}

func (s *oracleSvc) panicRecover(err *error) {
	if r := recover(); r != nil {
		*err = errors.Errorf("%v", r)

		if e, ok := r.(error); ok {
			s.logger.WithError(e).Errorln("service main loop panicked with an error")
			s.logger.Debugln(string(debug.Stack()))
		} else {
			s.logger.Errorln(r)
		}
	}
}

func (s *oracleSvc) Close() {
	// TODO: graceful shutdown if needed
}
