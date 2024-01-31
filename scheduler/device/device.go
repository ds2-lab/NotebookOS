package device

import (
	"fmt"

	"github.com/google/uuid"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// Devices wraps a map[string]*Device with some functions.
type Devices map[string]*Device

type Device struct {
	pluginapi.Device
	Index int
}

func (d *Device) String() string {
	return fmt.Sprintf("Device[Idx=%d,ID=%s,Health=%s]", d.Index, d.ID, d.Health)
}

func BuildDevice(index int) *Device {
	dev := Device{}
	dev.ID = uuid.New().String()
	dev.Index = index
	dev.Health = pluginapi.Healthy

	return &dev
}

func (ds Devices) Insert(device *Device) {
	ds[device.ID] = device
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
