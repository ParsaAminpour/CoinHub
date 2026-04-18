package market

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// CoinGeckoPriceFeed implements PriceFeed using CoinGecko's /simple/price endpoint.
// baseURL should be the API root, e.g. "https://api.coingecko.com/api/v3".
// apiKey is the x_cg_demo_api_key (or x_cg_pro_api_key for Pro accounts).
type CoinGeckoPriceFeed struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func NewCoinGeckoPriceFeed(baseURL, apiKey string) *CoinGeckoPriceFeed {
	return &CoinGeckoPriceFeed{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		apiKey:  apiKey,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

// GetPrice fetches the current price of baseSym quoted in quoteSym.
// e.g. GetPrice("BTC", "USDT") → 67_000.00
func (cg *CoinGeckoPriceFeed) GetPrice(baseSym, quoteSym string) (decimal.Decimal, error) {
	id := coingeckoIDForSymbol(baseSym)
	vs := coingeckoVsCurrencyForSymbol(quoteSym)

	if id == "" {
		return decimal.Zero, fmt.Errorf("coingecko: unsupported base symbol %q", baseSym)
	}
	if vs == "" {
		return decimal.Zero, fmt.Errorf("coingecko: unsupported quote symbol %q", quoteSym)
	}

	u, err := buildCoinGeckoURL(cg.baseURL, cg.apiKey, id, vs)
	if err != nil {
		return decimal.Zero, fmt.Errorf("coingecko: failed to build url: %w", err)
	}

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return decimal.Zero, fmt.Errorf("coingecko: failed to create request: %w", err)
	}

	resp, err := cg.client.Do(req)
	if err != nil {
		zap.S().Warnw("coingecko: request failed", "error", err, "url", u)
		return decimal.Zero, fmt.Errorf("coingecko: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decimal.Zero, fmt.Errorf("coingecko: non-2xx response %d for %s", resp.StatusCode, u)
	}

	// Response shape: { "<id>": { "<vs>": 123.45 } }
	var payload map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return decimal.Zero, fmt.Errorf("coingecko: failed to decode response: %w", err)
	}

	price, ok := payload[id][vs]
	if !ok {
		return decimal.Zero, fmt.Errorf("coingecko: price not found for %s/%s in response", id, vs)
	}

	return decimal.NewFromFloat(price), nil
}

// buildCoinGeckoURL constructs the full /simple/price request URL.
// It appends the /simple/price path to baseURL and sets ids, vs_currencies,
// and the api key as query parameters.
func buildCoinGeckoURL(baseURL, apiKey, id, vs string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	parsed.Path = path.Join(parsed.Path, "simple", "price")

	q := url.Values{}
	q.Set("ids", id)
	q.Set("vs_currencies", vs)
	if apiKey != "" {
		q.Set("x_cg_demo_api_key", apiKey)
	}
	parsed.RawQuery = q.Encode()
	return parsed.String(), nil
}

// coingeckoIDForSymbol maps a currency symbol to the CoinGecko coin ID.
func coingeckoIDForSymbol(sym string) string {
	switch strings.ToUpper(strings.TrimSpace(sym)) {
	case "BTC":
		return "bitcoin"
	case "ETH":
		return "ethereum"
	case "USDT":
		return "tether"
	case "USDC":
		return "usd-coin"
	case "BNB":
		return "binancecoin"
	case "SOL":
		return "solana"
	default:
		return ""
	}
}

// coingeckoVsCurrencyForSymbol maps a quote symbol to a CoinGecko vs_currency value.
func coingeckoVsCurrencyForSymbol(sym string) string {
	switch strings.ToUpper(strings.TrimSpace(sym)) {
	case "USD", "USDT", "USDC":
		return "usd"
	case "EUR":
		return "eur"
	case "GBP":
		return "gbp"
	default:
		return ""
	}
}
