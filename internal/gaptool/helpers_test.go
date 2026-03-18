package gaptool

import (
	"strings"
	"testing"
)

// ── toString ──────────────────────────────────────────────────────────────────

func TestToStringVariants(t *testing.T) {
	cases := []struct {
		input any
		want  string
	}{
		{nil, ""},
		{"hello", "hello"},
		{float64(3.14), "3.14"},
		{float32(2.5), "2.5"},
		{int(42), "42"},
		{int64(64), "64"},
		{int32(32), "32"},
		{true, "true"},
		{false, "false"},
	}
	for _, c := range cases {
		got := toString(c.input)
		if got != c.want {
			t.Errorf("toString(%v) = %q, want %q", c.input, got, c.want)
		}
	}
}

func TestToStringFmtStringer(t *testing.T) {
	// strings.Builder implements fmt.Stringer via String()
	var b strings.Builder
	b.WriteString("stringer")
	got := toString(&b)
	if got != "stringer" {
		t.Fatalf("expected stringer, got %q", got)
	}
}

// ── intFromAny ────────────────────────────────────────────────────────────────

func TestIntFromAny(t *testing.T) {
	cases := []struct {
		input    any
		fallback int
		want     int
	}{
		{int(7), 0, 7},
		{int32(32), 0, 32},
		{int64(64), 0, 64},
		{float64(9.9), 0, 9},
		{float32(4.4), 0, 4},
		{"123", 0, 123},
		{"not-a-number", 99, 99},
		{nil, 5, 5},
	}
	for _, c := range cases {
		got := intFromAny(c.input, c.fallback)
		if got != c.want {
			t.Errorf("intFromAny(%v, %d) = %d, want %d", c.input, c.fallback, got, c.want)
		}
	}
}

// ── floatFromAny ──────────────────────────────────────────────────────────────

func TestFloatFromAny(t *testing.T) {
	cases := []struct {
		input any
		want  float64
		ok    bool
	}{
		{float64(1.5), 1.5, true},
		{float32(2.5), 2.5, true},
		{int(3), 3.0, true},
		{int64(4), 4.0, true},
		{int32(5), 5.0, true},
		{"6.6", 6.6, true},
		{"bad", 0, false},
		{nil, 0, false},
	}
	for _, c := range cases {
		got, ok := floatFromAny(c.input)
		if ok != c.ok {
			t.Errorf("floatFromAny(%v) ok=%v, want %v", c.input, ok, c.ok)
			continue
		}
		if ok && got != c.want {
			t.Errorf("floatFromAny(%v) = %v, want %v", c.input, got, c.want)
		}
	}
}

// ── asSlice ───────────────────────────────────────────────────────────────────

func TestAsSliceFromArray(t *testing.T) {
	arr := [3]int{1, 2, 3}
	got := asSlice(arr)
	if len(got) != 3 {
		t.Fatalf("expected 3 items, got %d", len(got))
	}
}

func TestAsSliceFromNonSlice(t *testing.T) {
	got := asSlice("not-a-slice")
	if got != nil {
		t.Fatalf("expected nil for non-slice, got %v", got)
	}
}

// ── listItems ─────────────────────────────────────────────────────────────────

func TestListItemsFromResultsKey(t *testing.T) {
	input := map[string]any{
		"results": []any{map[string]any{"id": "1"}},
	}
	got := listItems(input)
	if len(got) != 1 {
		t.Fatalf("expected 1 item from results key, got %d", len(got))
	}
}

func TestListItemsFromItemsKey(t *testing.T) {
	input := map[string]any{
		"items": []any{map[string]any{"id": "1"}, map[string]any{"id": "2"}},
	}
	got := listItems(input)
	if len(got) != 2 {
		t.Fatalf("expected 2 items from items key, got %d", len(got))
	}
}

func TestListItemsEmpty(t *testing.T) {
	got := listItems(nil)
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

// ── cloneMap ──────────────────────────────────────────────────────────────────

func TestCloneMap(t *testing.T) {
	source := map[string]any{"a": 1, "b": "hello"}
	cloned := cloneMap(source)
	if len(cloned) != 2 {
		t.Fatalf("expected 2 entries in clone, got %d", len(cloned))
	}
	// Mutations to clone must not affect source
	cloned["a"] = 999
	if source["a"] != 1 {
		t.Fatal("cloneMap mutated source")
	}
}

// ── truncateString ────────────────────────────────────────────────────────────

func TestTruncateStringAtExactLimit(t *testing.T) {
	got := truncateString("hello", 5)
	if got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
}

func TestTruncateStringExceedsLimit(t *testing.T) {
	got := truncateString("hello world", 5)
	if got != "hello" {
		t.Fatalf("expected hello, got %q", got)
	}
}

// ── escapeSQLString ───────────────────────────────────────────────────────────

func TestEscapeSQLString(t *testing.T) {
	got := escapeSQLString("it's a test")
	if got != "it''s a test" {
		t.Fatalf("expected it''s a test, got %q", got)
	}
}

func TestEscapeSQLStringNoQuotes(t *testing.T) {
	got := escapeSQLString("no quotes here")
	if got != "no quotes here" {
		t.Fatalf("unexpected change: %q", got)
	}
}

// ── mapStrings ────────────────────────────────────────────────────────────────

func TestMapStrings(t *testing.T) {
	input := map[string]any{"key": "val", "num": 42}
	got := mapStrings(input)
	if got["key"] != "val" {
		t.Fatalf("expected val, got %q", got["key"])
	}
	if got["num"] != "42" {
		t.Fatalf("expected 42, got %q", got["num"])
	}
}
