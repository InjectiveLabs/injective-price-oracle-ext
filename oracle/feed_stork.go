package oracle

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cosmossdk.io/math"
	"github.com/InjectiveLabs/metrics"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/websocket"
	"github.com/pelletier/go-toml/v2"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	log "github.com/xlab/suplog"
)

var _ PricePuller = &storkPriceFeed{}

type StorkFeedConfig struct {
	ProviderName string `toml:"provider"`
	Ticker       string `toml:"ticker"`
	PullInterval string `toml:"pullInterval"`
	Message      string `toml:"message"`
	OracleType   string `toml:"oracleType"`
}

type storkPriceFeed struct {
	providerName string
	ticker       string
	interval     time.Duration
	message      string

	logger  log.Logger
	svcTags metrics.Tags

	oracleType oracletypes.OracleType
}

func ParseStorkFeedConfig(body []byte) (*StorkFeedConfig, error) {
	var config StorkFeedConfig
	if err := toml.Unmarshal(body, &config); err != nil {
		err = errors.Wrap(err, "failed to unmarshal TOML config")
		return nil, err
	}

	return &config, nil
}

// NewStorkPriceFeed returns price puller
func NewStorkPriceFeed(cfg *StorkFeedConfig) (PricePuller, error) {
	pullInterval := 1 * time.Minute
	if len(cfg.PullInterval) > 0 {
		interval, err := time.ParseDuration(cfg.PullInterval)
		if err != nil {
			err = errors.Wrapf(err, "failed to parse pull interval: %s (expected format: 60s)", cfg.PullInterval)
			return nil, err
		}

		if interval < 1*time.Second {
			err = errors.Wrapf(err, "failed to parse pull interval: %s (minimum interval = 1s)", cfg.PullInterval)
			return nil, err
		}

		pullInterval = interval
	}

	var oracleType oracletypes.OracleType
	if cfg.OracleType == "" {
		oracleType = oracletypes.OracleType_Stork
	} else {
		tmpType, exist := oracletypes.OracleType_value[cfg.OracleType]
		if !exist {
			return nil, fmt.Errorf("oracle type does not exist: %s", cfg.OracleType)
		}

		oracleType = oracletypes.OracleType(tmpType)
	}

	feed := &storkPriceFeed{
		providerName: cfg.ProviderName,
		ticker:       cfg.Ticker,
		interval:     pullInterval,
		message:      cfg.Message,
		oracleType:   oracleType,

		logger: log.WithFields(log.Fields{
			"svc":      "oracle",
			"dynamic":  true,
			"provider": cfg.ProviderName,
		}),

		svcTags: metrics.Tags{
			"provider": cfg.ProviderName,
		},
	}

	return feed, nil
}

func (f *storkPriceFeed) Interval() time.Duration {
	return f.interval
}

func (f *storkPriceFeed) Symbol() string {
	return f.ticker
}

func (f *storkPriceFeed) Provider() FeedProvider {
	return FeedProviderStork
}

func (f *storkPriceFeed) ProviderName() string {
	return f.providerName
}

func (f *storkPriceFeed) OracleType() oracletypes.OracleType {
	return oracletypes.OracleType_Stork
}

// PullAssetPairs pulls asset pair for an asset id
func (f *storkPriceFeed) PullAssetPairs(conn *websocket.Conn) (assetPairs []*oracletypes.AssetPair, err error) {
	metrics.ReportFuncCall(f.svcTags)
	doneFn := metrics.ReportFuncTiming(f.svcTags)
	defer doneFn()

	err = conn.WriteMessage(websocket.TextMessage, []byte(f.message))
	if err != nil {
		f.logger.Warningln("Error writing message:", err)
		return
	}

	var msgNeed []byte
	var messageRead []byte
	count := 0
	for count < 2 {
		_, messageRead, err = conn.ReadMessage()
		if err != nil {
			f.logger.Warningln("Error reading message:", err)
			return

		}
		msgNeed = messageRead
		count += 1
	}

	var msgResp messageResponse
	if err = json.Unmarshal(msgNeed, &msgResp); err != nil {
		f.logger.Warningln("error unmarshalling feed message:", err)
		return
	}

	assetIds := make([]string, 0)
	for key := range msgResp.Data {
		assetIds = append(assetIds, key)
	}

	for _, assetId := range assetIds {
		pair := ConvertDataToAssetPair(msgResp.Data[assetId], assetId)
		assetPairs = append(assetPairs, &pair)
	}

	return assetPairs, nil
}

func (f *storkPriceFeed) PullPrice(_ context.Context) (
	price decimal.Decimal,
	err error,
) {
	f.logger.Warning("Stork Price Feed is not implemented")
	return zeroPrice, nil
}

// ConvertDataToAssetPair converts data get from websocket to list of asset pairs
func ConvertDataToAssetPair(data Data, assetId string) (result oracletypes.AssetPair) {
	var signedPricesOfAssetPair []*oracletypes.SignedPriceOfAssetPair
	for i := range data.SignedPrices {
		newSignedPriceAssetPair := ConvertSignedPrice(data.SignedPrices[i])
		signedPricesOfAssetPair = append(signedPricesOfAssetPair, &newSignedPriceAssetPair)
	}
	result.SignedPrices = signedPricesOfAssetPair
	result.AssetId = assetId

	return result
}

// ConvertSignedPrice converts signed price to SignedPriceOfAssetPair of Stork
func ConvertSignedPrice(signeds SignedPrice) oracletypes.SignedPriceOfAssetPair {
	var signedPriceOfAssetPair oracletypes.SignedPriceOfAssetPair

	signature := CombineSignatureToString(signeds.TimestampedSignature.Signature)

	signedPriceOfAssetPair.Signature = common.Hex2Bytes(signature)
	signedPriceOfAssetPair.PublisherKey = signeds.PublisherKey
	signedPriceOfAssetPair.Timestamp = ConvertTimestampToSecond(signeds.TimestampedSignature.Timestamp)
	signedPriceOfAssetPair.Price = signeds.Price

	return signedPriceOfAssetPair
}

func ConvertTimestampToSecond(timestamp uint64) uint64 {
	switch {
	// nanosecond
	case timestamp > 1e18:
		return timestamp / uint64(1_000_000_000)
	// microsecond
	case timestamp > 1e15:
		return timestamp / uint64(1_000_000)
	// millisecond
	case timestamp > 1e12:
		return timestamp / uint64(1_000)
	// second
	default:
		return timestamp
	}
}

// CombineSignatureToString combines a signature to a string
func CombineSignatureToString(signature Signature) (result string) {
	prunedR := strings.TrimPrefix(signature.R, "0x")
	prunedS := strings.TrimPrefix(signature.S, "0x")
	prunedV := strings.TrimPrefix(signature.V, "0x")

	return prunedR + prunedS + prunedV
}

type messageResponse struct {
	Type    string          `json:"type"`
	TraceID string          `json:"trace_id"`
	Data    map[string]Data `json:"data"`
}

type Data struct {
	Timestamp     int64         `json:"timestamp"`
	AssetID       string        `json:"asset_id"`
	SignatureType string        `json:"signature_type"`
	Trigger       string        `json:"trigger"`
	Price         string        `json:"price"`
	SignedPrices  []SignedPrice `json:"signed_prices"`
}

type SignedPrice struct {
	PublisherKey         string               `json:"publisher_key"`
	ExternalAssetID      string               `json:"external_asset_id"`
	SignatureType        string               `json:"signature_type"`
	Price                math.LegacyDec       `json:"price"`
	TimestampedSignature TimestampedSignature `json:"timestamped_signature"`
}

type TimestampedSignature struct {
	Signature Signature `json:"signature"`
	Timestamp uint64    `json:"timestamp"`
	MsgHash   string    `json:"msg_hash"`
}

type Signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V string `json:"v"`
}
