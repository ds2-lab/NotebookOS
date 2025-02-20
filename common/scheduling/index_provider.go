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

	// Identifier returns the index's identifier.
	Identifier() string

	// IsQualified returns the actual value according to the index category and whether the Host is qualified.
	// An index provider must be able to track indexed Host instances and indicate disqualification.
	IsQualified(Host) (actual interface{}, qualified IndexQualification)

	// Len returns the number of Host instances in the index.
	Len() int

	// AddHost adds a Host to the index.
	AddHost(Host)

	// Update updates a Host in the index.
	Update(Host)

	// UpdateMultiple updates multiple Host instances in the index.
	UpdateMultiple([]Host)

	// Remove removes a Host from the index.
	RemoveHost(Host)

	// GetMetrics returns the metrics implemented by the index. This is useful for reusing implemented indexes.
	GetMetrics(Host) (metrics []float64)
}
