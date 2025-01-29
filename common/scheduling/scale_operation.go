package scheduling

type ScaleOperation interface {
	// IsScaleOutOperation returns true if the ScaleOperation is of type ScaleOutOperation.
	IsScaleOutOperation() bool

	// IsScaleInOperation returns true if the ScaleOperation is of type ScaleInOperation.
	IsScaleInOperation() bool

	// GetOperationId returns the OperationId of the target ScaleOperation.
	GetOperationId() string
}
