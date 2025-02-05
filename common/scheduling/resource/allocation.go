package resource

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/scusemua/distributed-notebook/common/scheduling"
	"github.com/scusemua/distributed-notebook/common/types"
	"github.com/shopspring/decimal"
	"time"
)

// getKey creates and returns a string of the form "<KernelID>-<ReplicaID>".
// This is used as a key to various maps belonging to the AllocationManager.
func getKey(replicaId int32, kernelId string) string {
	return fmt.Sprintf("%s-%d", kernelId, replicaId)
}

// Allocation encapsulates an allocation of HostResources to a kernel replica.
// Each Allocation encapsulates an allocation of GPU, CPU, and Memory HostResources.
type Allocation struct {
	Timestamp                time.Time       `json:"timestamp"`
	Millicpus                decimal.Decimal `json:"cpus"`
	ExecutionId              string
	VramGB                   decimal.Decimal `json:"vram"`
	AllocationId             string          `json:"ID"`
	MemoryMB                 decimal.Decimal `json:"memory"`
	cachedAllocationKey      string
	AllocationType           scheduling.AllocationType `json:"allocation_type"`
	GPUs                     decimal.Decimal           `json:"gpus"`
	KernelId                 string                    `json:"kernel_id"`
	HostId                   string
	HostName                 string
	GpuDeviceIds             []int `json:"gpu_device_ids"`
	ReplicaId                int32 `json:"replica_id"`
	IsPreCommittedAllocation bool  `json:"is_pre_committed"`
	IsReservationAllocation  bool  `json:"is_reservation"`
}

func (a *Allocation) GetHostId() string {
	return a.HostId
}

func (a *Allocation) GetHostName() string {
	return a.HostName
}

func (a *Allocation) GetAllocationType() scheduling.AllocationType {
	return a.AllocationType
}

func (a *Allocation) SetGpuDeviceIds(deviceIds []int) {
	a.GpuDeviceIds = make([]int, len(deviceIds))
	copy(a.GpuDeviceIds, deviceIds)
}

func (a *Allocation) ClearGpuDeviceIds() {
	a.GpuDeviceIds = make([]int, 0)
}

func (a *Allocation) GetGpuDeviceIds() []int {
	deviceIds := make([]int, len(a.GpuDeviceIds))
	copy(deviceIds, a.GpuDeviceIds)
	return deviceIds
}

func (a *Allocation) GetMillicpus() float64 {
	return a.Millicpus.InexactFloat64()
}

func (a *Allocation) GetMemoryMb() float64 {
	return a.MemoryMB.InexactFloat64()
}

func (a *Allocation) GetVramGb() float64 {
	return a.VramGB.InexactFloat64()
}

func (a *Allocation) GetGpus() float64 {
	return a.GPUs.InexactFloat64()
}

func (a *Allocation) GetTimestamp() time.Time {
	return a.Timestamp
}

func (a *Allocation) GetAllocationId() string {
	return a.AllocationId
}

func (a *Allocation) GetKernelId() string {
	return a.KernelId
}

func (a *Allocation) GetReplicaId() int32 {
	return a.ReplicaId
}

// CloneAndReturnedAdjusted returns a copy of the target Allocation with its resource quantities
// adjusted to patch the given types.Spec.
//
// If the given types.Spec is nil, then the cloned/copied Allocation struct contains the same resource
// quantities as the original, target Allocation struct.
func (a *Allocation) CloneAndReturnedAdjusted(spec types.Spec) scheduling.Allocation {
	var (
		gpus decimal.Decimal
		vram decimal.Decimal
		mem  decimal.Decimal
		cpus decimal.Decimal
	)

	if spec == nil {
		gpus = a.GPUs.Copy()
		vram = a.VramGB.Copy()
		mem = a.MemoryMB.Copy()
		cpus = a.Millicpus.Copy()
	} else {
		gpus = decimal.NewFromFloat(spec.GPU())
		vram = decimal.NewFromFloat(spec.VRAM())
		mem = decimal.NewFromFloat(spec.MemoryMB())
		cpus = decimal.NewFromFloat(spec.CPU())
	}

	clonedGpuDeviceIds := make([]int, 0, len(a.GpuDeviceIds))
	for _, deviceId := range a.GpuDeviceIds {
		clonedGpuDeviceIds = append(clonedGpuDeviceIds, deviceId)
	}

	clonedResourceAllocation := &Allocation{
		AllocationId:             a.AllocationId,
		GPUs:                     gpus,
		VramGB:                   vram,
		Millicpus:                cpus,
		MemoryMB:                 mem,
		ReplicaId:                a.ReplicaId,
		KernelId:                 a.KernelId,
		Timestamp:                a.Timestamp,
		AllocationType:           a.AllocationType,
		IsPreCommittedAllocation: a.IsPreCommittedAllocation,
		cachedAllocationKey:      a.cachedAllocationKey,
		GpuDeviceIds:             clonedGpuDeviceIds,
	}

	return clonedResourceAllocation
}

// String returns a string representation of the Allocation suitable for logging.
func (a *Allocation) String() string {
	o, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}

	return string(o)
}

// ToSpecString returns a string representation of the Allocation (suitable for logging) in the format
// of the String() methods of types.Spec implementations.
func (a *Allocation) ToSpecString() string {
	return fmt.Sprintf("ResourceSpec[Millicpus: %s, Memory: %s MB, GPUs: %s (%v)]",
		a.Millicpus.StringFixed(4), a.MemoryMB.StringFixed(4), a.GPUs.StringFixed(1), a.GpuDeviceIds)
}

// ToSpec converts the Allocation to a types.Spec instance with the same resource values as the
// Allocation's resource values.
//
// Specifically, a new types.DecimalSpec is created using copies of the Allocation's internal
// decimal.Decimal fields, and a pointer to this new types.DecimalSpec is returned.
//
// This is, in some sense, an alias for the ToDecimalSpec method, though ToSpec returns a types.Spec interface,
// whereas ToDecimalSpec returns a pointer to a types.DecimalSpec struct.
func (a *Allocation) ToSpec() types.Spec {
	return a.ToDecimalSpec()
}

// ToDecimalSpec converts the Allocation to a types.DecimalSpec struct with the same resource values as the
// Allocation's resource values and returns a pointer to it (the newly-created types.DecimalSpec).
func (a *Allocation) ToDecimalSpec() *types.DecimalSpec {
	return &types.DecimalSpec{
		GPUs:      a.GPUs.Copy(),
		Millicpus: a.Millicpus.Copy(),
		MemoryMb:  a.MemoryMB.Copy(),
		VRam:      a.VramGB.Copy(),
	}
}

// IsNonZero returns true if any of the HostResources (cpu, gpu, memory) encapsulated by the Allocation are > 0.
func (a *Allocation) IsNonZero() bool {
	return a.GPUs.GreaterThan(decimal.Zero) || a.Millicpus.GreaterThan(decimal.Zero) || a.MemoryMB.GreaterThan(decimal.Zero)
}

func (a *Allocation) SpecToString() string {
	return a.ToSpec().String()
}

// IsPending returns true if the Allocation is of type PendingAllocation.
// If the Allocation is instead of type CommittedAllocation, then IsPending returns false.
func (a *Allocation) IsPending() bool {
	return a.AllocationType == scheduling.PendingAllocation
}

// IsCommitted returns true if the Allocation is of type CommittedAllocation.
// If the Allocation is instead of type PendingAllocation, then IsCommitted returns false.
func (a *Allocation) IsCommitted() bool {
	return a.AllocationType == scheduling.CommittedAllocation
}

func (a *Allocation) SetAllocationType(typ scheduling.AllocationType) {
	a.AllocationType = typ
}

// IsReservation returns the Allocation's IsReservationAllocation value, which indicates whether the HostResources were
// allocated (as either pending or committed, depending upon the configured scheduling policy) in anticipation of a
// scheduling.KernelContainer being placed onto the scheduling.Host. If true, then that means that the associated
// scheduling.KernelContainer has not yet started running on the scheduling.Host (or that the notification that the
// scheduling.KernelContainer has started running has not yet been received).
func (a *Allocation) IsReservation() bool {
	return a.IsReservationAllocation
}

// SetIsReservation is used to set the value of the Allocation's IsPreCommittedAllocation flag.
//
// The IsReservationAllocation indicates whether the HostResources were allocated (as either pending or committed,
// depending upon the configured scheduling policy) in anticipation of a scheduling.KernelContainer being placed onto
// the scheduling.Host. If true, then that means that the associated scheduling.KernelContainer has not yet started
// running on the scheduling.Host (or that the notification that the scheduling.KernelContainer has started running
// has not yet been received).
func (a *Allocation) SetIsReservation(isReservation bool) {
	a.IsReservationAllocation = isReservation
}

// IsPreCommitted returns the Allocation's IsPreCommittedAllocation value, which indicates whether the HostResources were
// commited in anticipation of a leader election, or if they are committed to a kernel that is actively training.
func (a *Allocation) IsPreCommitted() bool {
	return a.IsPreCommittedAllocation
}

// SetIsPreCommitted is used to set the value of the Allocation's IsPreCommittedAllocation flag.
//
// The IsPreCommittedAllocation indicates whether the HostResources were commited in anticipation of a leader election,
// or if they are committed to a kernel that is actively training.
func (a *Allocation) SetIsPreCommitted(isPreCommittedAllocation bool) {
	a.IsPreCommittedAllocation = isPreCommittedAllocation
}

func (a *Allocation) SetGpus(gpus decimal.Decimal) {
	a.GPUs = gpus.Copy()
}

func (a *Allocation) SetVramGb(vram decimal.Decimal) {
	a.VramGB = vram.Copy()
}

func (a *Allocation) SetMemoryMb(mem decimal.Decimal) {
	a.MemoryMB = mem.Copy()
}

func (a *Allocation) SetMillicpus(millicpus decimal.Decimal) {
	a.Millicpus = millicpus.Copy()
}

func (a *Allocation) SetReplicaId(replicaId int32) {
	a.ReplicaId = replicaId
}

func (a *Allocation) GetExecutionId() string {
	return a.ExecutionId
}

func (a *Allocation) SetExecutionId(executionId string) {
	a.ExecutionId = executionId
}

// AllocationBuilder is a utility struct whose purpose is to facilitate the creation of a
// new Allocation struct.
type AllocationBuilder struct {
	kernelId        string
	gpus            decimal.Decimal
	vramGb          decimal.Decimal
	millicpus       decimal.Decimal
	memoryMb        decimal.Decimal
	allocationType  scheduling.AllocationType
	executionId     string
	hostId          string
	hostName        string
	allocationId    string
	gpuDeviceIds    []int
	replicaId       int32
	isPreCommitment bool
	isReservation   bool
}

// NewResourceAllocationBuilder creates a new AllocationBuilder and returns a pointer to it.
// The AllocationID of the Allocation being constructed is randomly generated at this point.
func NewResourceAllocationBuilder() *AllocationBuilder {
	return &AllocationBuilder{
		allocationId: uuid.NewString(),
		gpuDeviceIds: make([]int, 0),
	}
}

// WithHostId enables the specification of the Allocation's HostId field.
func (b *AllocationBuilder) WithHostId(hostId string) *AllocationBuilder {
	b.hostId = hostId
	return b
}

// WithHostName enables the specification of the Allocation's HostName field.
func (b *AllocationBuilder) WithHostName(hostName string) *AllocationBuilder {
	b.hostName = hostName
	return b
}

// WithExecutionId allows the specification of the ExecutionId field, which is the Jupyter message ID ("msg_id" from
// the header) of the associated "execute_request" message.
func (b *AllocationBuilder) WithExecutionId(executionId string) *AllocationBuilder {
	b.executionId = executionId
	return b
}

// WithIdOverride enables the specification of a specific ID to be used as the Allocation ID of the Allocation
// that is being created. This is entirely optional. If no ID is specified explicitly, then a random UUID is
// generated to be used as the Allocation ID of the Allocation that is under construction.
func (b *AllocationBuilder) WithIdOverride(id string) *AllocationBuilder {
	b.allocationId = id
	return b
}

// WithAllocationType enables the specification of the AllocationType of the Allocation that is being created.
func (b *AllocationBuilder) WithAllocationType(allocationType scheduling.AllocationType) *AllocationBuilder {
	b.allocationType = allocationType
	return b
}

// WithGpuDeviceIds enables the specification of the GPU device IDs to be included within the Allocation.
func (b *AllocationBuilder) WithGpuDeviceIds(deviceIds []int) *AllocationBuilder {
	b.gpuDeviceIds = deviceIds
	return b
}

// WithGpuDeviceId adds a single GPU device ID to the slice of GPU device IDs to be included within the Allocation.
// WithGpuDeviceId can be called multiple times to add multiple GPU device IDs in a one-at-a-type manner.
func (b *AllocationBuilder) WithGpuDeviceId(deviceId int) *AllocationBuilder {
	if b.gpuDeviceIds == nil {
		b.gpuDeviceIds = make([]int, 0, 1)
	}

	found := false
	for _, id := range b.gpuDeviceIds {
		if id == deviceId {
			found = true
			break
		}
	}

	if !found {
		b.gpuDeviceIds = append(b.gpuDeviceIds, deviceId)
	}

	return b
}

// WithKernelReplica enables the specification of the target of the Allocation (i.e., the kernel replica).
func (b *AllocationBuilder) WithKernelReplica(replicaId int32, kernelId string) *AllocationBuilder {
	b.kernelId = kernelId
	b.replicaId = replicaId
	return b
}

// WithGPUs enables the specification of the number of GPUs in the Allocation that is being constructed.
func (b *AllocationBuilder) WithGPUs(gpus float64) *AllocationBuilder {
	b.gpus = decimal.NewFromFloat(gpus)
	return b
}

// WithVRAM enables the specification of the amount of VRAM (in GB) in the Allocation that is being constructed.
func (b *AllocationBuilder) WithVRAM(vramGb float64) *AllocationBuilder {
	b.vramGb = decimal.NewFromFloat(vramGb)
	return b
}

// WithMillicpus enables the specification of the number of Millicpus (in millicpus, or 1/1000th of a core)
// in the Allocation that is being constructed.
func (b *AllocationBuilder) WithMillicpus(millicpus float64) *AllocationBuilder {
	b.millicpus = decimal.NewFromFloat(millicpus)
	return b
}

// WithMemoryMB enables the specification of the amount of memory (in megabytes)
// in the Allocation that is being constructed.
func (b *AllocationBuilder) WithMemoryMB(memoryMb float64) *AllocationBuilder {
	b.memoryMb = decimal.NewFromFloat(memoryMb)
	return b
}

// IsAReservation indicates that the Allocation being created is a reservation.
func (b *AllocationBuilder) IsAReservation() *AllocationBuilder {
	b.isReservation = true
	return b
}

// IsNotAReservation indicates that the Allocation being created is NOT a reservation.
func (b *AllocationBuilder) IsNotAReservation() *AllocationBuilder {
	b.isReservation = false
	return b
}

// IsAPreCommitment indicates that the Allocation being created is a pre-commitment of resources.
func (b *AllocationBuilder) IsAPreCommitment() *AllocationBuilder {
	b.isPreCommitment = true
	return b
}

// IsNotAPreCommitment indicates that the Allocation being created is NOT a pre-commitment of resources.
func (b *AllocationBuilder) IsNotAPreCommitment() *AllocationBuilder {
	b.isPreCommitment = false
	return b
}

// BuildResourceAllocation constructs the Allocation with the values specified to the AllocationBuilder.
func (b *AllocationBuilder) BuildResourceAllocation() *Allocation {
	return &Allocation{
		AllocationId:             b.allocationId,
		GpuDeviceIds:             b.gpuDeviceIds,
		GPUs:                     b.gpus,
		VramGB:                   b.vramGb,
		Millicpus:                b.millicpus,
		MemoryMB:                 b.memoryMb,
		ReplicaId:                b.replicaId,
		KernelId:                 b.kernelId,
		AllocationType:           b.allocationType,
		HostName:                 b.hostName,
		HostId:                   b.hostId,
		IsReservationAllocation:  b.isReservation,
		IsPreCommittedAllocation: b.isPreCommitment,
		Timestamp:                time.Now(),
		cachedAllocationKey:      getKey(b.replicaId, b.kernelId),
	}
}
