package controller

import (
	"sort"
	"time"

	"github.com/k-p2p-lab/kpl-v3/internal/model"
)

const analysisScorePointLimit = 720
const analysisScoreSampleLimit = analysisScorePointLimit - 2*len(model.ScoreComponentKeys)

type analysisScorePoint struct {
	At          time.Time                       `json:"at"`
	Components  map[string]model.ScoreStatistic `json:"components"`
	BreakBefore bool                            `json:"breakBefore,omitempty"`
	index       uint64
	gaps        uint64
}

type analysisScoreSeries struct {
	Group  string               `json:"group"`
	Points []analysisScorePoint `json:"points"`
}

type scoreTimelineGroup struct {
	points          []analysisScorePoint
	latest          analysisScorePoint
	lastObservation uint64
	lastMask        uint16
	samples         uint64
	stride          uint64
	gaps            uint64
	unordered       bool
	first           [len(model.ScoreComponentKeys)]analysisScorePoint
	last            [len(model.ScoreComponentKeys)]analysisScorePoint
}

// Score evidence has its own sampling budget per observer group. Sampling the
// general topology observations first can discard every measured score from a
// short interval, even though those measurements remain in the original log.
type scoreTimeline struct {
	observation uint64
	groups      map[string]*scoreTimelineGroup
}

func (timeline *scoreTimeline) observe(observation analysisObservation) {
	timeline.observation++
	for _, group := range observation.Groups {
		if len(group.ScoreComponents) == 0 {
			continue
		}
		var mask uint16
		components := make(map[string]model.ScoreStatistic)
		for i, key := range model.ScoreComponentKeys {
			stat, exists := group.ScoreComponents[key]
			if !exists || !stat.Valid() {
				continue
			}
			components[key] = stat
			if stat.Count > 0 {
				mask |= 1 << i
			}
		}
		if mask == 0 {
			continue
		}
		if timeline.groups == nil {
			timeline.groups = make(map[string]*scoreTimelineGroup)
		}
		series := timeline.groups[group.Group]
		if series == nil {
			series = &scoreTimelineGroup{stride: 1}
			timeline.groups[group.Group] = series
		}
		// A missing group or empty score inspection is a gap, not a zero or a
		// value to carry forward. Partial-component transitions also break the
		// series conservatively so thinning cannot connect over absent values.
		if series.samples > 0 && (series.lastObservation+1 != timeline.observation || series.lastMask != mask) {
			series.gaps++
		}
		if series.samples > 0 && !observation.At.After(series.latest.At) {
			series.unordered = true
		}
		series.lastObservation, series.lastMask = timeline.observation, mask
		point := analysisScorePoint{At: observation.At, Components: components, index: series.samples, gaps: series.gaps}
		series.latest = point
		for i := range model.ScoreComponentKeys {
			if mask&(1<<i) == 0 {
				continue
			}
			if series.first[i].Components == nil {
				series.first[i] = point
			}
			series.last[i] = point
		}
		if series.samples%series.stride == 0 {
			series.points = append(series.points, point)
			// Reserve bounded space for the first and last measurement of
			// every component, including sparse, partially recorded maps.
			if len(series.points) == analysisScoreSampleLimit {
				series.thin()
			}
		}
		series.samples++
	}
}

func (series *scoreTimelineGroup) thin() {
	kept := 1 // Always retain the first measured sample.
	for i := 1; i < len(series.points); i++ {
		if i%2 == 0 {
			series.points[kept] = series.points[i]
			kept++
		}
	}
	clear(series.points[kept:])
	series.points = series.points[:kept]
	series.stride *= 2
}

func (timeline *scoreTimeline) result() []analysisScoreSeries {
	names := make([]string, 0, len(timeline.groups))
	for name := range timeline.groups {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]analysisScoreSeries, 0, len(names))
	for _, name := range names {
		series := timeline.groups[name]
		points := append([]analysisScorePoint(nil), series.points...)
		indices := make(map[uint64]bool, len(points)+2*len(model.ScoreComponentKeys))
		for _, point := range points {
			indices[point.index] = true
		}
		for i := range model.ScoreComponentKeys {
			for _, point := range []analysisScorePoint{series.first[i], series.last[i]} {
				if point.Components != nil && !indices[point.index] {
					points = append(points, point)
					indices[point.index] = true
				}
			}
		}
		sort.Slice(points, func(i, j int) bool { return points[i].index < points[j].index })
		for i := range points {
			// A generation changes whenever original observations contain a
			// gap. It survives sampling, including removed gap boundaries.
			points[i].BreakBefore = i > 0 && points[i].gaps != points[i-1].gaps
		}
		if series.unordered {
			// Preserve recorded times and values, but do not infer continuity
			// after a clock reversal or overlapping observation timestamp.
			sort.SliceStable(points, func(i, j int) bool { return points[i].At.Before(points[j].At) })
			for i := range points {
				points[i].BreakBefore = i > 0
			}
		}
		result = append(result, analysisScoreSeries{Group: name, Points: points})
	}
	return result
}
