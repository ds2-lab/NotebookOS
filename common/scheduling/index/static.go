package index

type StaticIndex struct {
	*MultiIndex[*LeastLoadedIndex]
}

func NewStaticIndex(maxGpus int32) (*StaticIndex, error) {
	multi, err := NewMultiIndex[*LeastLoadedIndex](maxGpus, NewLeastLoadedIndexWrapper)
	if err != nil {
		return nil, err
	}

	static := &StaticIndex{
		MultiIndex: multi,
	}

	return static, nil
}
