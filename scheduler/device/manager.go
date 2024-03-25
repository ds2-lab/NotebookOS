package device

import (
	"errors"
	"sync"

	"github.com/mason-leap-lab/go-utils/config"
	"github.com/mason-leap-lab/go-utils/logger"
)

type resourceManagerImpl struct {
	sync.Mutex
	log logger.Logger

	resource         string
	devices          Devices
	allocatedDevices Devices
	freeDevices      Devices
}

var (
	ErrDeviceNotFound = errors.New("the specified device cannot be found")
)

func NewResourceManager(resource string, numVirtualGPUs int) ResourceManager {
	m := &resourceManagerImpl{
		resource:         resource,
		devices:          make(Devices),
		allocatedDevices: make(Devices),
		freeDevices:      make(Devices),
	}

	config.InitLogger(&m.log, m)

	var i int
	for i = 0; i < numVirtualGPUs; i++ {
		dev := BuildDevice(i)
		m.devices.Insert(dev)
		m.freeDevices.Insert(dev)
	}

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

// Returns ErrDeviceNotFound if the specified device cannot be found.
// Return ErrDeviceAlreadyAllocated if the specified device is already marked as allocated.
// Otherwise, return nil.
func (m *resourceManagerImpl) AllocateDevice(id string) error {
	m.Lock()
	defer m.Unlock()

	device := m.devices.GetByID(id)

	if device == nil {
		return ErrDeviceNotFound
	}

	err := device.MarkAllocated()
	if err == nil {
		m.freeDevices.Remove(device)
		m.allocatedDevices.Insert(device)
		m.log.Debug("Allocated Device: %s", device.ID)
		return nil
	} else {
		m.log.Error("Device %s is already marked as allocated.", device.ID)
	}

	return err
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
		m.allocatedDevices.Remove(device)
		m.freeDevices.Insert(device)
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
	return len(m.devices)
}

func (m *resourceManagerImpl) NumFreeDevices() int {
	m.Lock()
	defer m.Unlock()
	return len(m.freeDevices)
}

func (m *resourceManagerImpl) NumAllocatedDevices() int {
	m.Lock()
	defer m.Unlock()
	return len(m.allocatedDevices)
}

func (m *resourceManagerImpl) Resource() string {
	return m.resource
}

func (m *resourceManagerImpl) Devices() Devices {
	return m.devices
}
