package store

import "testing"

func TestDefaultTopicRemark(t *testing.T) {
	cases := map[string]string{
		"tns1:RuleEngine/CellMotionDetector/Motion":  "移动侦测",
		"RuleEngine/CellMotionDetector/Intrusion":    "区域入侵检测",
		"RuleEngine/CellMotionDetector/People":       "人行检测",
		"RuleEngine/CellMotionDetector/TpSmartEvent": "智能事件",
		"tns1:RuleEngine/TamperDetector/Tamper":      "遮挡告警",
		"RuleEngine/SomeVendor/UnknownThing":         "",
	}
	for topic, want := range cases {
		if got := defaultTopicRemark(topic); got != want {
			t.Errorf("defaultTopicRemark(%q) = %q, want %q", topic, got, want)
		}
	}
}
