package gaptool

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var promQLReservedWords = map[string]struct{}{
	"sum": {}, "avg": {}, "min": {}, "max": {}, "count": {}, "rate": {},
	"irate": {}, "increase": {}, "delta": {}, "topk": {}, "bottomk": {},
	"quantile": {}, "histogram_quantile": {}, "avg_over_time": {},
	"sum_over_time": {}, "count_over_time": {}, "last_over_time": {},
	"by": {}, "without": {}, "on": {}, "ignoring": {}, "bool": {},
	"and": {}, "or": {}, "unless": {},
}

func asMap(value any) map[string]any {
	if typed, ok := value.(map[string]any); ok && typed != nil {
		return typed
	}
	return map[string]any{}
}

func asSlice(value any) []any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []any:
		return typed
	}

	rawValue := reflect.ValueOf(value)
	if !rawValue.IsValid() {
		return nil
	}
	if rawValue.Kind() != reflect.Slice && rawValue.Kind() != reflect.Array {
		return nil
	}

	result := make([]any, 0, rawValue.Len())
	for i := 0; i < rawValue.Len(); i++ {
		result = append(result, rawValue.Index(i).Interface())
	}
	return result
}

func toMapSlice(value any) []map[string]any {
	items := asSlice(value)
	result := make([]map[string]any, 0, len(items))
	for _, item := range items {
		mapped := asMap(item)
		if len(mapped) > 0 {
			result = append(result, mapped)
		}
	}
	return result
}

func listItems(value any) []map[string]any {
	if items := toMapSlice(value); len(items) > 0 {
		return items
	}
	if rawItems := asSlice(value); len(rawItems) > 0 {
		items := make([]map[string]any, 0, len(rawItems))
		for _, item := range rawItems {
			if mapped := asMap(item); len(mapped) > 0 {
				items = append(items, mapped)
				continue
			}
			items = append(items, map[string]any{"value": item})
		}
		return items
	}
	mapped := asMap(value)
	for _, key := range []string{"data", "results", "items"} {
		if items := toMapSlice(mapped[key]); len(items) > 0 {
			return items
		}
		if rawItems := asSlice(mapped[key]); len(rawItems) > 0 {
			items := make([]map[string]any, 0, len(rawItems))
			for _, item := range rawItems {
				if mappedItem := asMap(item); len(mappedItem) > 0 {
					items = append(items, mappedItem)
					continue
				}
				items = append(items, map[string]any{"value": item})
			}
			return items
		}
	}
	return []map[string]any{}
}

func toString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func intFromAny(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	case float32:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func floatFromAny(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err == nil {
			return parsed, true
		}
	}
	return 0, false
}

func normalizedKey(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func firstString(values ...any) string {
	for _, value := range values {
		text := strings.TrimSpace(toString(value))
		if text != "" {
			return text
		}
	}
	return ""
}

func cloneMap(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func truncateString(value string, maxLen int) string {
	if len(value) <= maxLen {
		return value
	}
	return value[:maxLen]
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func looksLikePromQLFunction(token string) bool {
	_, ok := promQLReservedWords[strings.ToLower(token)]
	return ok
}

func mapStrings(value map[string]any) map[string]string {
	result := map[string]string{}
	for key, raw := range value {
		result[key] = toString(raw)
	}
	return result
}
