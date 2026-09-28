package model

import (
	"maps"
	"math"
	"time"
)

// Weighted score contributions. Topic terms include TopicWeight; topicCap is
// the adjustment applied after adding topic terms. Their sum equals total.
var ScoreComponentKeys = [...]string{"p1", "p2", "p3", "p3b", "p4", "p5", "p6", "p7", "topicCap", "total"}

type ScoreStatistic struct {
	Count int     `json:"count"`
	Mean  float64 `json:"mean"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

func FiniteScore(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
func (s ScoreStatistic) Valid() bool {
	return s.Count >= 0 && s.Count <= 10000000 && FiniteScore(s.Mean) && FiniteScore(s.Min) && FiniteScore(s.Max) &&
		(s.Count == 0 && s.Mean == 0 && s.Min == 0 && s.Max == 0 || s.Count > 0 && s.Min <= s.Mean && s.Mean <= s.Max)
}
func (s *ScoreStatistic) Add(value float64) {
	s.Merge(ScoreStatistic{Count: 1, Mean: value, Min: value, Max: value})
}
func (s *ScoreStatistic) Merge(other ScoreStatistic) {
	if !other.Valid() || other.Count == 0 {
		return
	}
	if s.Count == 0 {
		*s = other
		return
	}
	n := s.Count + other.Count
	weight := float64(other.Count) / float64(n)
	if math.Signbit(s.Mean) == math.Signbit(other.Mean) {
		s.Mean += (other.Mean - s.Mean) * weight
	} else {
		s.Mean = s.Mean*(1-weight) + other.Mean*weight
	}
	s.Min, s.Max, s.Count = min(s.Min, other.Min), max(s.Max, other.Max), n
	// Floating-point averaging can round just outside the original extrema.
	s.Mean = max(s.Min, min(s.Max, s.Mean))
}

// One bounded summary per scoring observer and inspection, not an O(N²) dump
// of peer/topic pairs. Retained scores are included, as in libp2p's inspector.
type PeerScoreSample struct {
	ObservedAt      time.Time                 `json:"observedAt"`
	IntervalSeconds float64                   `json:"intervalSeconds"`
	Components      map[string]ScoreStatistic `json:"components"`
}

func (s *PeerScoreSample) Clone() *PeerScoreSample {
	if s == nil {
		return nil
	}
	out := *s
	out.Components = maps.Clone(s.Components)
	return &out
}
func (s *PeerScoreSample) Valid() bool {
	if s == nil || s.ObservedAt.IsZero() || !FiniteScore(s.IntervalSeconds) || s.IntervalSeconds <= 0 || len(s.Components) != len(ScoreComponentKeys) {
		return false
	}
	count := s.Components["total"].Count
	for _, key := range ScoreComponentKeys {
		v, ok := s.Components[key]
		if !ok || !v.Valid() || v.Count != count {
			return false
		}
	}
	return true
}
func (s *PeerScoreSample) Fresh(now time.Time) bool {
	if !s.Valid() {
		return false
	}
	age := now.Sub(s.ObservedAt).Seconds()
	return age >= -5 && age <= max(30, 2*s.IntervalSeconds+5)
}
