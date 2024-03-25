package device

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var (
	ErrDeviceAlreadyAllocated = errors.New("device is already marked as allocated")
	ErrDeviceAlreadyFree      = errors.New("device is already marked as free")
)

type Device struct {
	pluginapi.Device
	Index     int  `json:"index"`
	Allocated bool `json:"allocated"`
}

// Return an error (ErrDeviceAlreadyAllocated) if the device is already marked as allocated.
// Otherwise, return nil.
func (d *Device) MarkAllocated() error {
	if d.Allocated {
		return ErrDeviceAlreadyAllocated
	}

	d.Allocated = true
	return nil
}

// Return an error (ErrDeviceAlreadyFree) if the device is already marked as free.
// Otherwise, return nil.
func (d *Device) MarkFree() error {
	if !d.Allocated {
		return ErrDeviceAlreadyFree
	}

	d.Allocated = false
	return nil
}

func (d *Device) String() string {
	return fmt.Sprintf("Device[Idx=%d,ID=%s,Health=%s]", d.Index, d.ID, d.Health)
}

func BuildDevice(index int) *Device {
	dev := Device{}
	deviceUuid := uuid.New().String()
	deviceId := fmt.Sprintf("Virtual-GPU-%d", index)
	dev.ID = fmt.Sprintf("%s-%s", deviceId, deviceUuid[0:36-len(deviceId)]) // This will be problematic if the index is a number with ~35 digits.

	dev.Index = index
	dev.Health = pluginapi.Healthy

	return &dev
}

// Devices wraps a map[string]*Device with some functions.
type Devices map[string]*Device

// Alias for length.
func (d Devices) Size() int {
	return len(d)
}

func (d Devices) Length() int {
	return len(d)
}

func (ds Devices) Insert(device *Device) {
	ds[device.ID] = device
}

func (ds Devices) Remove(device *Device) {
	delete(ds, device.ID)
}

// Contains checks if Devices contains devices matching all ids.
func (ds Devices) Contains(ids ...string) bool {
	for _, id := range ids {
		if _, exists := ds[id]; !exists {
			return false
		}
	}
	return true
}

// GetByID returns a reference to the device matching the specified ID (nil otherwise).
func (ds Devices) GetByID(id string) *Device {
	return ds[id]
}

// GetByIndex returns a reference to the device matching the specified Index (nil otherwise).
func (ds Devices) GetByIndex(index int) *Device {
	for _, d := range ds {
		if d.Index == index {
			return d
		}
	}
	return nil
}

// Subset returns the subset of devices in Devices matching the provided ids.
// If any id in ids is not in Devices, then the subset that did match will be returned.
func (ds Devices) Subset(ids []string) Devices {
	res := make(Devices)
	for _, id := range ids {
		if ds.Contains(id) {
			res[id] = ds[id]
		}
	}
	return res
}

// Difference returns the set of devices contained in ds but not in ods.
func (ds Devices) Difference(ods Devices) Devices {
	res := make(Devices)
	for id := range ds {
		if !ods.Contains(id) {
			res[id] = ds[id]
		}
	}
	return res
}

// GetIDs returns the ids from all devices in the Devices
func (ds Devices) GetIDs() []string {
	var res []string
	for _, d := range ds {
		res = append(res, d.ID)
	}
	return res
}

// GetPluginDevices returns the plugin Devices from all devices in the Devices
func (ds Devices) GetPluginDevices() []*pluginapi.Device {
	var res []*pluginapi.Device
	for _, device := range ds {
		d := device
		res = append(res, &d.Device)
	}
	return res
}

// GetIndices returns the Indices from all devices in the Devices
func (ds Devices) GetIndices() []int {
	var res []int
	for _, d := range ds {
		res = append(res, d.Index)
	}
	return res
}
