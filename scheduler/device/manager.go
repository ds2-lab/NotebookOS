package device

type resourceManagerImpl struct {
	resource string
	devices  Devices
}

func NewResourceManager(resource string, numVirtualGPUs int) ResourceManager {
	m := &resourceManagerImpl{
		resource: resource,
		devices:  make(Devices),
	}

	var i int
	for i = 0; i < numVirtualGPUs; i++ {
		dev := BuildDevice(i)
		m.devices.Insert(dev)
	}

	return m
}

func (m *resourceManagerImpl) Resource() string {
	return m.resource
}

func (m *resourceManagerImpl) Devices() Devices {
	return m.devices
}
