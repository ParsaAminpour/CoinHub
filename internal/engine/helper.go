package engine

import (
	"fmt"
	"strings"
)

func (a *SupportedPairLight) fixSupportedPairLight() error {
	if !strings.Contains(*a.Symbol, "-") {
		if !strings.Contains(*a.Symbol, "/") || len(strings.Split(*a.Symbol, "/")) != 2 {
			return fmt.Errorf("the asset symbol format is incorrect %s", *a.Symbol)
		}
		formattedSymbol := fmt.Sprintf("%s-%s", strings.Split(*a.Symbol, "/")[0], strings.Split(*a.Symbol, "/")[1])
		a.Symbol = &formattedSymbol
	}
	return nil
}

// removeEmptyLevels filters out price levels that have no remaining orders.
func removeEmptyLevels(levels []*PriceLevel) []*PriceLevel {
	result := levels[:0]
	for _, lvl := range levels {
		if len(lvl.Orders) > 0 {
			result = append(result, lvl)
		}
	}
	return result
}
