package oracle

import (
	"strings"
	"time"

	"github.com/shopspring/decimal"

	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"
)

// PriceData stores additional meta info for a price report.
type PriceData struct {
	// Ticker is BASE/QUOTE pair name
	Ticker Ticker

	// ProviderName is the name of the feed
	ProviderName string

	// Symbol is provider-specific
	Symbol string

	// Price is the reported price by feed integarion
	Price decimal.Decimal

	// Asset pair - for Stork Oracle
	AssetPairs []*oracletypes.AssetPair

	// Timestamp of the report
	Timestamp time.Time

	OracleType oracletypes.OracleType
}

type Ticker string

func (t Ticker) Base() string {
	return strings.Split(string(t), "/")[0]
}

func (t Ticker) Quote() string {
	return strings.Split(string(t), "/")[1]
}
