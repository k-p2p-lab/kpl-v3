// Package distribution provides seeded duration distributions shared by scenarios and peer network configuration.
package distribution

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type Distribution struct {
	Model    string  `json:"model" yaml:"model"`
	Value    string  `json:"value" yaml:"value"`
	Mean     string  `json:"mean" yaml:"mean"`
	Sigma    string  `json:"sigma" yaml:"sigma"`
	XM       string  `json:"xm" yaml:"xm"`
	Alpha    float64 `json:"alpha" yaml:"alpha"`
	Beta     float64 `json:"beta" yaml:"beta"`
	Scale    string  `json:"scale" yaml:"scale"`
	Mu       float64 `json:"mu" yaml:"mu"`
	LogSigma float64 `json:"logSigma" yaml:"logSigma"`
	Min      string  `json:"min" yaml:"min"`
	Max      string  `json:"max" yaml:"max"`
}

func (d Distribution) Validate(optional bool) error {
	if d.Model == "" {
		if optional && d == (Distribution{}) {
			return nil
		}
		return fmt.Errorf("model is required")
	}
	var err error
	switch d.Model {
	case "fixed":
		_, err = parseDurationOrZero(d.Value)
	case "exponential", "poisson":
		_, err = positiveDuration(d.Mean, "mean")
	case "normal":
		if _, err = time.ParseDuration(d.Mean); err == nil {
			_, err = parseDurationOrZero(d.Sigma)
		}
	case "pareto":
		if _, err = positiveDuration(d.XM, "xm"); err == nil && !finitePositive(d.Alpha) {
			err = fmt.Errorf("alpha must be positive and finite")
		}
	case "gamma":
		if !finitePositive(d.Alpha) {
			err = fmt.Errorf("alpha must be positive and finite")
		} else if d.Scale != "" && d.Beta != 0 {
			err = fmt.Errorf("gamma scale and beta are mutually exclusive")
		} else if d.Scale != "" {
			_, err = positiveDuration(d.Scale, "scale")
		} else if !finitePositive(d.Beta) {
			err = fmt.Errorf("gamma requires a positive finite beta rate or scale duration")
		}
	case "lognormal":
		if d.Sigma != "" && d.LogSigma != 0 {
			err = fmt.Errorf("lognormal sigma and logSigma are mutually exclusive")
		} else if logSigma, sigmaErr := d.lognormalSigma(); sigmaErr != nil {
			err = sigmaErr
		} else if math.IsNaN(d.Mu) || math.IsInf(d.Mu, 0) || math.IsNaN(logSigma) || math.IsInf(logSigma, 0) || logSigma < 0 {
			err = fmt.Errorf("lognormal mu must be finite and sigma must be finite and non-negative")
		}
	default:
		return fmt.Errorf("unsupported model %q", d.Model)
	}
	if err != nil {
		return err
	}
	minimum, err := parseDurationOrZero(d.Min)
	if err != nil {
		return fmt.Errorf("min: %w", err)
	}
	maximum, err := parseDurationOrZero(d.Max)
	if err != nil {
		return fmt.Errorf("max: %w", err)
	}
	if d.Min != "" && d.Max != "" && minimum > maximum {
		return fmt.Errorf("min cannot exceed max")
	}
	return nil
}

func (d Distribution) Sample(r *rand.Rand) time.Duration {
	var value time.Duration
	switch d.Model {
	case "":
		return 0
	case "fixed":
		value, _ = time.ParseDuration(d.Value)
	case "exponential":
		mean, _ := time.ParseDuration(d.Mean)
		value = durationFromNanoseconds(r.ExpFloat64() * float64(mean))
	case "poisson":
		mean, _ := time.ParseDuration(d.Mean)
		value = durationFromSeconds(float64(SamplePoisson(r, mean.Seconds())))
	case "normal":
		mean, _ := time.ParseDuration(d.Mean)
		sigma, _ := time.ParseDuration(d.Sigma)
		value = durationFromNanoseconds(float64(mean) + r.NormFloat64()*float64(sigma))
	case "pareto":
		xm, _ := time.ParseDuration(d.XM)
		u := math.Max(r.Float64(), math.SmallestNonzeroFloat64)
		value = durationFromNanoseconds(float64(xm) / math.Pow(u, 1/d.Alpha))
	case "gamma":
		sample := sampleGamma(r, d.Alpha)
		if d.Scale != "" {
			scale, _ := time.ParseDuration(d.Scale)
			value = durationFromNanoseconds(sample * float64(scale))
		} else {
			value = durationFromSeconds(sample / d.Beta)
		}
	case "lognormal":
		logSigma, _ := d.lognormalSigma()
		value = durationFromSeconds(math.Exp(d.Mu + logSigma*r.NormFloat64()))
	}
	if value < 0 {
		value = 0
	}
	if d.Min != "" {
		min, _ := time.ParseDuration(d.Min)
		if value < min {
			value = min
		}
	}
	if d.Max != "" {
		max, _ := time.ParseDuration(d.Max)
		if value > max {
			value = max
		}
	}
	return value
}

func (d Distribution) lognormalSigma() (float64, error) {
	if d.Sigma == "" {
		return d.LogSigma, nil
	}
	value, err := strconv.ParseFloat(d.Sigma, 64)
	if err != nil {
		return 0, fmt.Errorf("lognormal sigma must be a dimensionless number: %w", err)
	}
	return value, nil
}

func sampleGamma(r *rand.Rand, shape float64) float64 {
	if shape < 1 {
		u := math.Max(r.Float64(), math.SmallestNonzeroFloat64)
		return sampleGamma(r, shape+1) * math.Pow(u, 1/shape)
	}
	d := shape - 1.0/3.0
	c := 1 / math.Sqrt(9*d)
	for {
		x := r.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := r.Float64()
		if u < 1-0.0331*x*x*x*x || math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}

// SamplePoisson draws a non-negative count using an exact sampler. The caller
// must supply a positive finite lambda (Distribution.Validate enforces this).
func SamplePoisson(r *rand.Rand, lambda float64) int64 {
	if lambda < 30 {
		limit := math.Exp(-lambda)
		product := 1.0
		var count int64
		for product > limit {
			count++
			product *= r.Float64()
		}
		return count - 1
	}
	// PTRS (Hörmann, 1993) is an exact transformed-rejection sampler whose
	// expected cost remains constant for large means. A normal approximation
	// would subtly change v2 experiment distributions at higher rates.
	sqrtLambda := math.Sqrt(lambda)
	b := 0.931 + 2.53*sqrtLambda
	a := -0.059 + 0.02483*b
	invAlpha := 1.1239 + 1.1328/(b-3.4)
	vR := 0.9277 - 3.6224/(b-2)
	for {
		u := r.Float64() - 0.5
		v := r.Float64()
		us := 0.5 - math.Abs(u)
		k := math.Floor((2*a/us+b)*u + lambda + 0.43)
		if us >= 0.07 && v <= vR {
			return int64(k)
		}
		if k < 0 || us < 0.013 && v > us {
			continue
		}
		logFactorial, _ := math.Lgamma(k + 1)
		if math.Log(v*invAlpha/(a/(us*us)+b)) <= -lambda+k*math.Log(lambda)-logFactorial {
			return int64(k)
		}
	}
}

func durationFromSeconds(value float64) time.Duration {
	return durationFromNanoseconds(value * float64(time.Second))
}

func durationFromNanoseconds(value float64) time.Duration {
	if math.IsNaN(value) || value <= 0 {
		return 0
	}
	if math.IsInf(value, 1) || value >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(value)
}

func finitePositive(value float64) bool {
	return value > 0 && !math.IsNaN(value) && !math.IsInf(value, 0)
}

func parseDurationOrZero(value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", value)
	}
	if d < 0 {
		return 0, fmt.Errorf("duration cannot be negative")
	}
	return d, nil
}

func positiveDuration(value, name string) (time.Duration, error) {
	d, err := parseDurationOrZero(value)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be positive", name)
	}
	return d, nil
}
