package scheduling

type MovingStat struct {
	window    int64
	n         int64
	values    []float64
	last      int64
	sum       [2]float64 // include a moving sum(0) and a reset(1) used to ensuring moving sum by adding only.
	active    int
	resetting int
}

func (s *MovingStat) Add(val float64) {
	// Move forward.
	s.last = (s.last + 1) % s.window

	// Add difference to sum.
	s.sum[s.active] += val - s.values[s.last]
	s.sum[s.resetting] += val // Resetting is used to sum from ground above each window interval.
	if s.last == 0 {
		s.active = s.resetting
		s.resetting = (s.resetting + 1) % len(s.sum)
		s.sum[s.resetting] = 0.0
	}

	// Record history value
	s.values[s.last] = val

	// update length
	if s.n < s.window {
		s.n += 1
	}
}

func (s *MovingStat) Sum() float64 {
	return s.sum[s.active]
}

func (s *MovingStat) Window() int64 {
	return s.window
}

func (s *MovingStat) N() int64 {
	return s.n
}

func (s *MovingStat) Avg() float64 {
	return s.sum[s.active] / float64(s.n)
}

func (s *MovingStat) Last() float64 {
	return s.values[s.last]
}

func (s *MovingStat) LastN(n int64) float64 {
	if n > s.n {
		n = s.n
	}
	return s.values[(s.last+s.window-n)%s.window]
}
