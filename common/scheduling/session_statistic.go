package scheduling

type SessionStatistic interface {
	Add(val float64)
	Sum() float64
	Window() int64
	N() int64
	Avg() float64
	Last() float64
	LastN(n int64) float64
}

func NewSessionStatistic(window int64) SessionStatistic {
	if window > 0 {
		return &MovingStat{
			window:    window,
			n:         0,
			values:    make([]float64, window),
			last:      0,
			active:    0,
			resetting: 1,
		}
	} else if window < 0 {
		panic("Negative window size (meaning no window) is not yet supported.")
	} else {
		panic("Window size cannot be 0.")
	}
}
