package scheduling

const (
	CategoryClusterIndex = "BaseCluster"

	// IndexDisqualified indicates that the host has been indexed and unqualified now.
	IndexDisqualified IndexQualification = -1
	// IndexUnqualified indicates that the host is not qualified.
	IndexUnqualified IndexQualification = 0
	// IndexQualified indicates that the host has been indexed and is still qualified.
	IndexQualified IndexQualification = 1
	// IndexNewQualified indicates that the host is newly qualified and should be indexed.
	IndexNewQualified IndexQualification = 2
)

type IndexQualification int

type IndexProvider interface {
	// Category returns the category of the index and the expected value.
	Category() (category string, expected interface{})

	// IsQualified returns the actual value according to the index category and whether the host is qualified.
	// An index provider must be able to track indexed hosts and indicate disqualification.
	IsQualified(Host) (actual interface{}, qualified IndexQualification)

	// Len returns the number of hosts in the index.
	Len() int

	// Add adds a host to the index.
	Add(Host)

	// Update updates a host in the index.
	Update(Host)

	// Remove removes a host from the index.
	Remove(Host)

	// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
	GetMetrics(Host) (metrics []float64)
}
