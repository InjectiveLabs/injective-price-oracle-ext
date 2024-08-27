package oracle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	log "github.com/xlab/suplog"

	"github.com/InjectiveLabs/metrics"
	oracletypes "github.com/InjectiveLabs/sdk-go/chain/oracle/types"

	"github.com/InjectiveLabs/injective-price-oracle/pipeline"
)

type DynamicFeedConfig struct {
	ProviderName      string `toml:"provider"`
	Ticker            string `toml:"ticker"`
	PullInterval      string `toml:"pullInterval"`
	ObservationSource string `toml:"observationSource"`
	OracleType        string `toml:"oracleType"`
}

func ParseDynamicFeedConfig(body []byte) (*DynamicFeedConfig, error) {
	var config DynamicFeedConfig
	if err := toml.Unmarshal(body, &config); err != nil {
		err = errors.Wrap(err, "failed to unmarshal TOML config")
		return nil, err
	}

	// validate the observation source graph
	_, err := pipeline.Parse(config.ObservationSource)
	if err != nil {
		err = errors.Wrap(err, "observation source pipeline parse error")
		return nil, err
	}

	return &config, nil
}

func (c *DynamicFeedConfig) Hash() string {
	h := sha256.New()

	_, _ = h.Write([]byte(c.ProviderName))
	_, _ = h.Write([]byte(c.Ticker))
	_, _ = h.Write([]byte(c.ObservationSource))

	return hex.EncodeToString(h.Sum(nil))
}

// NewDynamicPriceFeed returns price puller that is implemented by Chainlink's job spec
// runner that accepts dotDag graphs as a definition of the observation source.
func NewDynamicPriceFeed(cfg *DynamicFeedConfig) (PricePuller, error) {
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
		oracleType = oracletypes.OracleType_PriceFeed
	} else {
		tmpType, exist := oracletypes.OracleType_value[cfg.OracleType]
		if !exist {
			return nil, fmt.Errorf("oracle type does not exist: %s", cfg.OracleType)
		}

		oracleType = oracletypes.OracleType(tmpType)
	}

	feed := &dynamicPriceFeed{
		ticker:       cfg.Ticker,
		providerName: cfg.ProviderName,
		interval:     pullInterval,
		dotDagSource: cfg.ObservationSource,
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

type dynamicPriceFeed struct {
	ticker       string
	providerName string
	interval     time.Duration
	dotDagSource string

	runNonce int32

	logger  log.Logger
	svcTags metrics.Tags

	oracleType oracletypes.OracleType
}

func (f *dynamicPriceFeed) Interval() time.Duration {
	return f.interval
}

func (f *dynamicPriceFeed) Symbol() string {
	// dynamic price feeds don't expose symbol name outside observation source graph,
	// so we just report its associated ticker here.
	return f.ticker
}

func (f *dynamicPriceFeed) Provider() FeedProvider {
	return FeedProviderDynamic
}

func (f *dynamicPriceFeed) ProviderName() string {
	return f.providerName
}

func (f *dynamicPriceFeed) OracleType() oracletypes.OracleType {
	if f.oracleType == oracletypes.OracleType_Unspecified {
		return oracletypes.OracleType_PriceFeed
	}
	return f.oracleType
}

func (f *dynamicPriceFeed) PullPrice(ctx context.Context) (
	price decimal.Decimal,
	err error,
) {
	metrics.ReportFuncCall(f.svcTags)
	doneFn := metrics.ReportFuncTiming(f.svcTags)
	defer doneFn()

	ts := time.Now()

	runner := pipeline.NewRunner(f.logger)
	runLogger := f.logger.WithFields(log.Fields{
		"ticker": f.ticker,
	})

	jobID := atomic.AddInt32(&f.runNonce, 1)
	spec := pipeline.Spec{
		ID:           jobID,
		DotDagSource: f.dotDagSource,
		CreatedAt:    time.Now().UTC(),

		JobID:   jobID,
		JobName: fmt.Sprintf("%s_%s", f.providerName, f.ticker),
	}

	runVars := pipeline.NewVarsFrom(map[string]interface{}{})
	run, trrs, err := runner.ExecuteRun(ctx, spec, runVars, runLogger)
	if err != nil {
		err = errors.Wrap(err, "failed to execute pipeline run")
		return zeroPrice, err
	} else if run.State != pipeline.RunStatusCompleted {
		if run.HasErrors() {
			runLogger.Warningf("final run result has non-critical errors: %s", run.AllErrors.ToError())
		}

		if run.HasFatalErrors() {
			err = errors.Errorf("final run result has fatal errors: %s", run.FatalErrors.ToError())
			return zeroPrice, err
		}

		err = errors.Errorf("expected run to be completed, yet got %v", run.State)
		return zeroPrice, err
	}

	finalResult := trrs.FinalResult(runLogger)

	if finalResult.HasErrors() {
		runLogger.Warningf("final run result has non-critical errors: %v", finalResult.AllErrors)
	}

	if finalResult.HasFatalErrors() {
		return zeroPrice, errors.Errorf("final run result has fatal errors: %v", finalResult.FatalErrors)
	}

	res, err := finalResult.SingularResult()
	if err != nil {
		return zeroPrice, errors.Wrap(err, "failed to get single result of pipeline run")
	}

	price, ok := res.Value.(decimal.Decimal)
	if !ok {
		if floatPrice, ok := res.Value.(float64); ok {
			price = decimal.NewFromFloat(floatPrice)
		} else if someString, ok := res.Value.(string); ok {
			price, err = decimal.NewFromString(someString)
		} else {
			err = errors.New("value is neither decimals, float64 nor string")
		}

		if err != nil {
			err = fmt.Errorf("expected pipeline result as string, decimal.Decimal or float64, but got %T, err: %w", res.Value, err)
			return zeroPrice, err
		}
	}

	runLogger.Infoln("PullPrice (pipeline run) done in", time.Since(ts))

	return price, nil
}
