package store

import (
	"testing"
)

func TestToJSONEmpty(t *testing.T) {
	if got := toJSON(nil); got != "{}" {
		t.Fatalf("want {}, got %s", got)
	}
	if got := toJSON(map[string]string{}); got != "{}" {
		t.Fatalf("want {}, got %s", got)
	}
}

func TestToJSONSingle(t *testing.T) {
	got := toJSON(map[string]string{"region": "east"})
	if got != `{"region":"east"}` {
		t.Fatalf("unexpected json: %s", got)
	}
}
