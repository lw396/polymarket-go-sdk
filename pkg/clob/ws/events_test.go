package ws

import (
	"encoding/json"
	"testing"
)

func TestProcessEventTickSizeChangeWireFields(t *testing.T) {
	c := &clientImpl{tickSizeCh: make(chan TickSizeChangeEvent, 1)}
	var raw map[string]any
	if err := json.Unmarshal([]byte(`{"event_type":"tick_size_change","asset_id":"token","market":"market","new_tick_size":"0.001","old_tick_size":"0.01","tick_size":"0.1","minimum_tick_size":"0.01","timestamp":"123"}`), &raw); err != nil {
		t.Fatal(err)
	}
	c.processEvent(raw)
	select {
	case event := <-c.tickSizeCh:
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]string
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"asset_id", "market", "new_tick_size", "old_tick_size", "tick_size", "minimum_tick_size", "timestamp"} {
			if got[key] != raw[key] {
				t.Errorf("decoded %s = %q, want %q", key, got[key], raw[key])
			}
		}
	default:
		t.Fatal("tick_size_change was not dispatched")
	}
}
