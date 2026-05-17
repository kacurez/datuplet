// Package main is the Finnhub Extractor component: fetches market data from
// the Finnhub API and writes it to the data lake via the DataGateway.
// Supports 7 modes: quote, news, company-news, basic-financials, earnings,
// recommendations, insider-transactions.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	finnhub "github.com/Finnhub-Stock-API/finnhub-go/v2"

	pb "github.com/datuplet/datuplet/pkg/datagateway/proto/v2"
	sdk "github.com/datuplet/datuplet/sdk/go"
)

type Config struct {
	Mode         string   `json:"mode"`
	Symbols      []string `json:"symbols"`
	Category     string   `json:"category"`
	LookbackDays int      `json:"lookback_days"`
	Limit        int      `json:"limit"`
	APIKey       string   `json:"apiKey"`
}

func main() {
	ctx := context.Background()

	client, err := sdk.New(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to connect to gateway: %v", err))
	}
	defer client.Close()

	cfg := client.Config()
	client.Log(ctx, "INFO", fmt.Sprintf("Finnhub Extractor started: execution=%s", cfg.ExecutionID))

	var compCfg Config
	if err := client.ParseConfig(&compCfg); err != nil {
		sdk.ExitUserError(fmt.Sprintf("failed to parse config: %v", err))
	}

	if compCfg.Mode == "" {
		sdk.ExitUserError("config.mode is required")
	}

	// Init Finnhub client — API key from config (fallback to env var)
	apiKey := compCfg.APIKey
	if apiKey == "" {
		apiKey = os.Getenv("FINNHUB_API_KEY")
	}
	if apiKey == "" {
		sdk.ExitUserError("config.apiKey or FINNHUB_API_KEY environment variable is required")
	}

	finnCfg := finnhub.NewConfiguration()
	finnCfg.AddDefaultHeader("X-Finnhub-Token", apiKey)
	finnClient := finnhub.NewAPIClient(finnCfg).DefaultApi

	client.Log(ctx, "INFO", fmt.Sprintf("Mode: %s, Symbols: %v", compCfg.Mode, compCfg.Symbols))

	var records []map[string]any
	var tableName string

	switch compCfg.Mode {
	case "quote":
		tableName = "market_data"
		records, err = extractQuotes(ctx, finnClient, compCfg.Symbols)
	case "news":
		tableName = "news_raw"
		records, err = extractNews(ctx, finnClient, compCfg.Category, compCfg.LookbackDays)
	case "company-news":
		tableName = "company_news"
		records, err = extractCompanyNews(ctx, finnClient, compCfg.Symbols, compCfg.LookbackDays)
	case "basic-financials":
		tableName = "basic_financials"
		records, err = extractBasicFinancials(ctx, finnClient, compCfg.Symbols)
	case "earnings":
		tableName = "earnings"
		limit := compCfg.Limit
		if limit == 0 {
			limit = 4
		}
		records, err = extractEarnings(ctx, finnClient, compCfg.Symbols, limit)
	case "recommendations":
		tableName = "recommendations"
		records, err = extractRecommendations(ctx, finnClient, compCfg.Symbols)
	case "insider-transactions":
		tableName = "insider_tx"
		records, err = extractInsiderTransactions(ctx, finnClient, compCfg.Symbols, compCfg.LookbackDays)
	default:
		sdk.ExitUserError(fmt.Sprintf("unknown mode: %s", compCfg.Mode))
	}

	if err != nil {
		sdk.ExitUserError(fmt.Sprintf("extraction failed: %v", err))
	}

	client.Log(ctx, "INFO", fmt.Sprintf("Extracted %d records for table %s", len(records), tableName))

	if len(records) == 0 {
		client.Log(ctx, "WARN", "No records extracted")
		if _, err := client.Commit(ctx); err != nil {
			sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
		}
		sdk.StatusMessage(fmt.Sprintf("extracted 0 records (mode=%s)", compCfg.Mode))
		return
	}

	// Write records via gateway
	writer, err := client.OpenWriter(ctx, tableName, sdk.WithFormat(pb.DataFormat_FORMAT_JSON))
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to open writer: %v", err))
	}

	jsonData, err := json.Marshal(records)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to marshal JSON: %v", err))
	}

	if err := writer.Write(ctx, jsonData); err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to write: %v", err))
	}

	closeResult, err := writer.Close(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("failed to close writer: %v", err))
	}

	client.Log(ctx, "INFO", fmt.Sprintf("Completed %s.%s: %d rows", writer.Bucket(), writer.Table(), closeResult.TotalRows))

	result, err := client.Commit(ctx)
	if err != nil {
		sdk.ExitAppError(fmt.Sprintf("commit failed: %v", err))
	}
	if !result.Success {
		sdk.ExitAppError("commit returned failure")
	}

	sdk.StatusMessage(fmt.Sprintf("extracted %d records (mode=%s, table=%s)", len(records), compCfg.Mode, tableName))
}

// extractQuotes fetches current OHLC quotes for each symbol.
func extractQuotes(ctx context.Context, client *finnhub.DefaultApiService, symbols []string) ([]map[string]any, error) {
	today := time.Now().Format("2006-01-02")
	var records []map[string]any

	for _, sym := range symbols {
		q, _, err := client.Quote(ctx).Symbol(sym).Execute()
		if err != nil {
			return nil, fmt.Errorf("quote %s: %w", sym, err)
		}

		records = append(records, map[string]any{
			"symbol":         sym,
			"date":           today,
			"open":           q.GetO(),
			"high":           q.GetH(),
			"low":            q.GetL(),
			"close":          q.GetC(),
			"prev_close":     q.GetPc(),
			"change":         q.GetD(),
			"change_percent": q.GetDp(),
		})
	}

	return records, nil
}

// extractNews fetches general market news, filtered by lookback days.
func extractNews(ctx context.Context, client *finnhub.DefaultApiService, category string, lookbackDays int) ([]map[string]any, error) {
	if category == "" {
		category = "general"
	}

	news, _, err := client.MarketNews(ctx).Category(category).Execute()
	if err != nil {
		return nil, fmt.Errorf("market news: %w", err)
	}

	cutoff := time.Now().AddDate(0, 0, -lookbackDays).Unix()
	seen := make(map[int64]bool)
	var records []map[string]any

	for _, n := range news {
		id := n.GetId()
		if seen[id] {
			continue
		}
		seen[id] = true

		dt := n.GetDatetime()
		if lookbackDays > 0 && dt < cutoff {
			continue
		}

		records = append(records, map[string]any{
			"id":       id,
			"headline": n.GetHeadline(),
			"summary":  n.GetSummary(),
			"source":   n.GetSource(),
			"url":      n.GetUrl(),
			"datetime": dt,
			"category": n.GetCategory(),
			"related":  n.GetRelated(),
		})
	}

	return records, nil
}

// extractCompanyNews fetches per-symbol news with date range filtering.
func extractCompanyNews(ctx context.Context, client *finnhub.DefaultApiService, symbols []string, lookbackDays int) ([]map[string]any, error) {
	if lookbackDays == 0 {
		lookbackDays = 2
	}
	from := time.Now().AddDate(0, 0, -lookbackDays).Format("2006-01-02")
	to := time.Now().Format("2006-01-02")

	seen := make(map[int64]bool)
	var records []map[string]any

	for _, sym := range symbols {
		news, _, err := client.CompanyNews(ctx).Symbol(sym).From(from).To(to).Execute()
		if err != nil {
			return nil, fmt.Errorf("company news %s: %w", sym, err)
		}

		for _, n := range news {
			id := n.GetId()
			if seen[id] {
				continue
			}
			seen[id] = true

			records = append(records, map[string]any{
				"id":       id,
				"symbol":   sym,
				"headline": n.GetHeadline(),
				"summary":  n.GetSummary(),
				"source":   n.GetSource(),
				"url":      n.GetUrl(),
				"datetime": n.GetDatetime(),
				"related":  n.GetRelated(),
			})
		}
	}

	return records, nil
}

// extractBasicFinancials fetches key financial metrics per symbol.
func extractBasicFinancials(ctx context.Context, client *finnhub.DefaultApiService, symbols []string) ([]map[string]any, error) {
	today := time.Now().Format("2006-01-02")
	var records []map[string]any

	for _, sym := range symbols {
		metrics, _, err := client.CompanyBasicFinancials(ctx).Symbol(sym).Metric("all").Execute()
		if err != nil {
			return nil, fmt.Errorf("basic financials %s: %w", sym, err)
		}

		m := metrics.GetMetric()

		records = append(records, map[string]any{
			"symbol":        sym,
			"fetch_date":    today,
			"beta":          getFloat(m, "beta"),
			"pe_annual":     getFloat(m, "peBasicExclExtraTTM"),
			"pb_annual":     getFloat(m, "pbAnnual"),
			"dividend_yield": getFloat(m, "dividendYieldIndicatedAnnual"),
			"eps_ttm":       getFloat(m, "epsBasicExclExtraItemsTTM"),
			"week_52_high":  getFloat(m, "52WeekHigh"),
			"week_52_low":   getFloat(m, "52WeekLow"),
			"avg_volume_10d": getFloat(m, "10DayAverageTradingVolume"),
			"avg_volume_3m": getFloat(m, "3MonthAverageTradingVolume"),
			"market_cap":    getFloat(m, "marketCapitalization"),
		})
	}

	return records, nil
}

// extractEarnings fetches earnings surprises per symbol.
func extractEarnings(ctx context.Context, client *finnhub.DefaultApiService, symbols []string, limit int) ([]map[string]any, error) {
	var records []map[string]any

	for _, sym := range symbols {
		earnings, _, err := client.CompanyEarnings(ctx).Symbol(sym).Limit(int64(limit)).Execute()
		if err != nil {
			return nil, fmt.Errorf("earnings %s: %w", sym, err)
		}

		for _, e := range earnings {
			records = append(records, map[string]any{
				"symbol":          e.GetSymbol(),
				"period":          e.GetPeriod(),
				"actual":          e.GetActual(),
				"estimate":        e.GetEstimate(),
				"surprise":        e.GetSurprise(),
				"surprise_percent": e.GetSurprisePercent(),
			})
		}
	}

	return records, nil
}

// extractRecommendations fetches analyst consensus per symbol.
func extractRecommendations(ctx context.Context, client *finnhub.DefaultApiService, symbols []string) ([]map[string]any, error) {
	var records []map[string]any

	for _, sym := range symbols {
		recs, _, err := client.RecommendationTrends(ctx).Symbol(sym).Execute()
		if err != nil {
			return nil, fmt.Errorf("recommendations %s: %w", sym, err)
		}

		for _, r := range recs {
			records = append(records, map[string]any{
				"symbol":      r.GetSymbol(),
				"period":      r.GetPeriod(),
				"strong_buy":  r.GetStrongBuy(),
				"buy":         r.GetBuy(),
				"hold":        r.GetHold(),
				"sell":        r.GetSell(),
				"strong_sell": r.GetStrongSell(),
			})
		}
	}

	return records, nil
}

// extractInsiderTransactions fetches SEC insider trades per symbol.
func extractInsiderTransactions(ctx context.Context, client *finnhub.DefaultApiService, symbols []string, lookbackDays int) ([]map[string]any, error) {
	var cutoff time.Time
	if lookbackDays > 0 {
		cutoff = time.Now().AddDate(0, 0, -lookbackDays)
	}

	var records []map[string]any

	for _, sym := range symbols {
		txns, _, err := client.InsiderTransactions(ctx).Symbol(sym).Execute()
		if err != nil {
			return nil, fmt.Errorf("insider transactions %s: %w", sym, err)
		}

		data := txns.GetData()
		for _, t := range data {
			txDate := t.GetTransactionDate()
			if lookbackDays > 0 && txDate != "" {
				parsed, err := time.Parse("2006-01-02", txDate)
				if err == nil && parsed.Before(cutoff) {
					continue
				}
			}

			records = append(records, map[string]any{
				"symbol":           t.GetSymbol(),
				"name":             t.GetName(),
				"share":            t.GetShare(),
				"change":           t.GetChange(),
				"transaction_date": txDate,
				"transaction_code": t.GetTransactionCode(),
				"filing_date":      t.GetFilingDate(),
			})
		}
	}

	return records, nil
}

// getFloat extracts a float64 from a map[string]interface{} safely.
func getFloat(m map[string]interface{}, key string) interface{} {
	v, ok := m[key]
	if !ok {
		return nil
	}
	return v
}
