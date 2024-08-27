package oracle

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	log "github.com/xlab/suplog"

	"github.com/InjectiveLabs/metrics"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
)

var _ PricePuller = &binancePriceFeed{}

type BinanceEndpointConfig struct {
	BaseURL string
}

// NewBinancePriceFeed returns price puller for given symbol. The price will be pulled
// from endpoint and divided by scaleFactor. Symbol name (if reported by endpoint) must match.
func NewBinancePriceFeed(symbol string, interval time.Duration, endpointConfig *BinanceEndpointConfig) PricePuller {
	return &binancePriceFeed{
		client: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: maxRespHeadersTime,
			},
			Timeout: maxRespTime,
		},
		config: checkBinanceConfig(endpointConfig),

		symbol:   symbol,
		interval: interval,

		logger: log.WithFields(log.Fields{
			"svc":      "oracle",
			"provider": FeedProviderBinance,
		}),
		svcTags: metrics.Tags{
			"provider": string(FeedProviderBinance),
		},
	}
}

func checkBinanceConfig(cfg *BinanceEndpointConfig) *BinanceEndpointConfig {
	if cfg == nil {
		cfg = &BinanceEndpointConfig{}
	}

	if len(cfg.BaseURL) == 0 {
		cfg.BaseURL = "https://api.binance.com/api/v3"
	}

	return cfg
}

type binancePriceFeed struct {
	client *http.Client
	config *BinanceEndpointConfig

	symbol   string
	interval time.Duration

	logger  log.Logger
	svcTags metrics.Tags
}

func (f *binancePriceFeed) Interval() time.Duration {
	return f.interval
}

func (f *binancePriceFeed) Symbol() string {
	return f.symbol
}

func (f *binancePriceFeed) Provider() FeedProvider {
	return FeedProviderBinance
}

func (f *binancePriceFeed) ProviderName() string {
	return "Binance API v3"
}

func (f *binancePriceFeed) OracleType() oracletypes.OracleType {
	return oracletypes.OracleType_PriceFeed
}

func (f *binancePriceFeed) PullPrice(ctx context.Context) (
	price decimal.Decimal,
	err error,
) {
	metrics.ReportFuncCall(f.svcTags)
	doneFn := metrics.ReportFuncTiming(f.svcTags)
	defer doneFn()

	u, err := url.ParseRequestURI(urlJoin(f.config.BaseURL, "ticker", "price"))
	if err != nil {
		metrics.ReportFuncError(f.svcTags)
		f.logger.WithError(err).Fatalln("failed to parse URL")
	}

	q := make(url.Values)
	q.Set("symbol", f.symbol)
	u.RawQuery = q.Encode()
	reqURL := u.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		metrics.ReportFuncError(f.svcTags)
		f.logger.WithError(err).Fatalln("failed to create HTTP request")
	}

	resp, err := f.client.Do(req)
	if err != nil {
		metrics.ReportFuncError(f.svcTags)
		err = errors.Wrapf(err, "failed to fetch price from %s", reqURL)
		return zeroPrice, err
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
	if err != nil {
		metrics.ReportFuncError(f.svcTags)
		_ = resp.Body.Close()
		err = errors.Wrapf(err, "failed to read response body from %s", reqURL)
		return zeroPrice, err
	}
	defer resp.Body.Close()

	var priceResp binancePriceResp
	if err = json.Unmarshal(respBody, &priceResp); err != nil {
		metrics.ReportFuncError(f.svcTags)
		err = errors.Wrapf(err, "failed to unmarshal response body for %s", f.symbol)
		f.logger.WithField("url", reqURL).Warningln(string(respBody))
		return zeroPrice, err
	} else if priceResp.Symbol != f.symbol {
		err = errors.Wrapf(err, "symbol name in response doesn't match requested: %s (resp) != %s (req)", priceResp.Symbol, f.symbol)
		return zeroPrice, err
	}

	if priceResp.Price.IsZero() {
		f.logger.Warningf("Price for [%s] fetched as zero!", f.symbol)
		return zeroPrice, nil
	}

	return priceResp.Price, nil
}

type binancePriceResp struct {
	Symbol string          `json:"symbol"`
	Price  decimal.Decimal `json:"price"`
}
