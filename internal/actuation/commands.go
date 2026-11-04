package actuation

import "context"

type Command struct {
	Action    string                 `json:"action"`
	HouseID   string                 `json:"house_id,omitempty"`
	MeterID   string                 `json:"meter_id,omitempty"`
	AssetID   string                 `json:"asset_id,omitempty"`
	Timestamp string                 `json:"timestamp"`
	PowerKw   int32                  `json:"power_kw,omitempty"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

func (k *KafkaActuationProducer) SendActivateMeter(ctx context.Context, meterID string) error {
	return k.SendCommand(ctx, Command{
		Action:  "ACTIVATE_METER",
		MeterID: meterID,
	})
}
