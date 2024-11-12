package device

import (
	"errors"
	"fmt"
	"sync"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	"github.com/elliotchance/orderedmap/v2"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

type resourceManagerImpl struct {
	sync.Mutex
	log logger.Logger

	resource         string
	devices          Devices
	allocatedDevices *orderedmap.OrderedMap[string, *Device]
	freeDevices      *orderedmap.OrderedMap[string, *Device]

	// The index of the largest device.
	// The device with this index could be removed if the admin adjusts the number of devices that are available,
	// in which case there would be no device that actually had this index.
	largestDeviceIndex int
}

var (
	ErrDeviceNotFound                 = errors.New("the specified device cannot be found")
	ErrInsufficientResourcesAvailable = errors.New("there are insufficient resources available to fulfill the request in its entirety")
	// ErrAllocationError                = errors.New("unexpected error encountered while performing device allocation")
)

func NewResourceManager(resource string, numVirtualGPUs int) ResourceManager {
	m := &resourceManagerImpl{
		resource:         resource,
		devices:          make(Devices),
		allocatedDevices: orderedmap.NewOrderedMap[string, *Device](),
		freeDevices:      orderedmap.NewOrderedMap[string, *Device](),
	}

	config.InitLogger(&m.log, m)

	var i int
	for i = 0; i < numVirtualGPUs; i++ {
		dev := BuildDevice(i)
		m.devices.Insert(dev)
		m.freeDevices.Set(dev.ID, dev)
	}

	m.largestDeviceIndex = int(numVirtualGPUs - 1)

	m.log.Debug("ResourceManager has started with %d device(s).", m.devices.Size())

	return m
}

// Returns (nil, ErrDeviceNotFound) if the specified device cannot be found.
func (m *resourceManagerImpl) GetDevice(id string) (*Device, error) {
	device := m.devices.GetByID(id)

	if device != nil {
		return device, nil
	}

	return nil, ErrDeviceNotFound
}

// Allocate a specific Device identified by the device's ID.
//
// Returns ErrDeviceNotFound if the specified device cannot be found.
// Return ErrDeviceAlreadyAllocated if the specified device is already marked as allocated.
// Otherwise, return nil.
func (m *resourceManagerImpl) AllocateSpecificDevice(id string) error {
	m.Lock()
	defer m.Unlock()

	return m.__allocateSpecificDeviceUnsafe(id)
}

// Allocate a specific Device identified by the device's ID.
// This does the actual allocation.
//
// The lock MUST be held before calling this method.
//
// Returns ErrDeviceNotFound if the specified device cannot be found.
// Return ErrDeviceAlreadyAllocated if the specified device is already marked as allocated.
// Otherwise, return nil.
func (m *resourceManagerImpl) __allocateSpecificDeviceUnsafe(id string) error {
	device := m.devices.GetByID(id)

	if device == nil {
		return ErrDeviceNotFound
	}

	err := device.MarkAllocated()
	if err == nil {
		m.freeDevices.Delete(device.ID)
		m.allocatedDevices.Set(device.ID, device)
		m.log.Debug("Allocated Device: %s", device.ID)
		return nil
	} else {
		m.log.Error("Device %s is already marked as allocated.", device.ID)
	}

	return err
}

// Modify the total number of resources that are available.
// This will return an error if this value is less than the number of allocated devices.
func (m *resourceManagerImpl) SetTotalNumDevices(value int32) error {
	m.Lock()
	defer m.Unlock()

	originalNumDevices := m.__unsafeNumDevices()

	if value < int32(m.__unsafeNumAllocatedDevices()) {
		return ErrInvalidResourceAdjustment
	}

	var err error
	if value >= int32(m.__unsafeNumDevices()) {
		err = m.__unsafeIncreaseTotalNumDevices(value)
	} else {
		err = m.__unsafeDecreaseTotalNumDevices(value)
	}

	// If there was no error and yet we don't have the correct number of devices,
	// then something went wrong that we didn't catch, so we panic.
	if err == nil && int32(m.__unsafeNumDevices()) != value {
		m.log.Error("No error occurred, yet we have the incorrect number of devices now. Originally: %d. Expected: %d. Actual: %d.", originalNumDevices, value, m.__unsafeNumDevices())
	}

	return err
}

// Increase the total number of devices available.
//
// The lock MUST be held before calling this method.
//
// If `value` is greater than the current number of devices, then this panics.
// If `value` is greater than the current number of free devices, then this panics.
func (m *resourceManagerImpl) __unsafeDecreaseTotalNumDevices(value int32) error {
	if value > int32(len(m.devices)) {
		panic(fmt.Sprintf("cannot decrease number of devices from %d to %d (%d > %d)", len(m.devices), value, value, len(m.devices)))
	}

	numDevicesToRemove := int32(len(m.devices)) - value
	if numDevicesToRemove == 0 {
		// Nothing to do.
		return nil
	}

	m.log.Debug("Removing %d device(s) so that there is a total of %d devices (current: %d).", numDevicesToRemove, value, int32(len(m.devices)))

	var toRemove = make([]*Device, 0, numDevicesToRemove)
	for el := m.freeDevices.Front(); el != nil; el = el.Next() {
		device := el.Value
		toRemove = append(toRemove, device)

		// Break once we have accumulated enough devices to remove.
		if int32(len(toRemove)) == numDevicesToRemove {
			break
		}
	}

	for _, device := range toRemove {
		m.devices.Remove(device)
		m.freeDevices.Delete(device.ID)
	}

	return nil
}

// Decrease the total number of devices available.
//
// The lock MUST be held before calling this method.
//
// If `value` is less than the current number of devices, then this panics.
func (m *resourceManagerImpl) __unsafeIncreaseTotalNumDevices(value int32) error {
	if value < int32(len(m.devices)) {
		panic(fmt.Sprintf("cannot increase number of devices from %d to %d (%d < %d)", len(m.devices), value, value, len(m.devices)))
	}

	numDevicesToCreate := value - int32(len(m.devices))
	if numDevicesToCreate == 0 {
		// Nothing to do.
		return nil
	}

	m.log.Debug("Adding %d device(s) so that there is a total of %d devices (current: %d).", numDevicesToCreate, value, int32(len(m.devices)))
	var i int32
	deviceIndex := m.largestDeviceIndex + 1
	for i = 0; i < numDevicesToCreate; i++ {
		dev := BuildDevice(deviceIndex)
		m.devices.Insert(dev)
		m.freeDevices.Set(dev.ID, dev)

		deviceIndex += 1
	}
	return nil
}

// Allocate n devices. The allocation is performed all at once.
//
// If there is an insufficient number of devices available to fulfill the entire request, then no devices are allocated and an ErrInsufficientResourcesAvailable error is returned.
// If the allocation is performed successfully, then a slice containing the IDs of the allocated devices is returned along with a slice of DeviceSpecs (one for each allocated device) and a nil error.
func (m *resourceManagerImpl) AllocateDevices(n int) ([]string, []*pluginapi.DeviceSpec, error) {
	m.Lock()
	defer m.Unlock()

	// Check if there are enough resources to fulfill the entire request.
	if m.freeDevices.Len() < n {
		return nil, nil, ErrInsufficientResourcesAvailable
	}

	// Gather all of the devices that we're going to allocate.
	allocatedDeviceIDs := make([]string, 0, n)
	deviceSpecs := make([]*pluginapi.DeviceSpec, 0, n)
	for el := m.freeDevices.Front(); el != nil; el = el.Next() {
		allocatedDeviceIDs = append(allocatedDeviceIDs, el.Value.ID)

		// Stop once we've allocated `n` devices.
		if len(allocatedDeviceIDs) == n {
			break
		}
	}

	// Allocate each individual device.
	for _, deviceID := range allocatedDeviceIDs {
		err := m.__allocateSpecificDeviceUnsafe(deviceID)
		if err != nil {
			errorMessage := fmt.Sprintf("Failed to allocate device %s: %v", deviceID, err)
			m.log.Error(errorMessage)
			klog.Errorf(errorMessage)

			return nil, nil, fmt.Errorf(errorMessage)
		}

		deviceSpecs = append(deviceSpecs, &pluginapi.DeviceSpec{
			// We use "/dev/fuse" for these virtual devices.
			ContainerPath: "/dev/fuse",
			HostPath:      "/dev/fuse",
			Permissions:   "mrw",
		})
	}

	return allocatedDeviceIDs, deviceSpecs, nil
}

// Returns ErrDeviceNotFound if the specified device cannot be found.
// Return ErrDeviceAlreadyAllocated if the specified device is already marked as free.
// Otherwise, return nil.
func (m *resourceManagerImpl) FreeDevice(id string) error {
	m.Lock()
	defer m.Unlock()

	device := m.devices.GetByID(id)

	if device == nil {
		return ErrDeviceNotFound
	}

	err := device.MarkFree()
	if err == nil {
		m.allocatedDevices.Delete(device.ID)
		m.freeDevices.Set(device.ID, device)
		m.log.Debug("Freed Device: %s", device.ID)
		return nil
	} else {
		m.log.Error("Device %s is already marked as free.", device.ID)
	}

	return err
}

// Return the total number of devices.
func (m *resourceManagerImpl) NumDevices() int {
	m.Lock()
	defer m.Unlock()
	return m.__unsafeNumDevices()
}

func (m *resourceManagerImpl) __unsafeNumDevices() int {
	return len(m.devices)
}

func (m *resourceManagerImpl) NumFreeDevices() int {
	m.Lock()
	defer m.Unlock()
	return m.__unsafeNumFreeDevices()
}

func (m *resourceManagerImpl) __unsafeNumFreeDevices() int {
	return m.freeDevices.Len()
}

func (m *resourceManagerImpl) NumAllocatedDevices() int {
	m.Lock()
	defer m.Unlock()
	return m.__unsafeNumAllocatedDevices()
}

func (m *resourceManagerImpl) __unsafeNumAllocatedDevices() int {
	return m.allocatedDevices.Len()
}

func (m *resourceManagerImpl) Resource() string {
	return m.resource
}

func (m *resourceManagerImpl) Devices() Devices {
	return m.devices
}
