package device

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	informersCore "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Verify that none of the given states are 'running'.
// They should all be 'terminated' or 'waiting'.
// If none of them are 'running', then return true.
// Otherwise, return false.
func verifyNotRunning(containerStatuses []corev1.ContainerStatus) bool {
	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated == nil && containerStatus.State.Waiting == nil {
			return false
		}
	}
	return true
}

// Encapsulates the Pods running on this node that require vGPUs.
// If a Pod is running on this node but does not require any vGPUs, then it will not be included in this cache.
type podCacheImpl struct {
	kubeClient kubernetes.Interface
	nodeName   string
	informer   informersCore.PodInformer
	stopChan   chan struct{}
}

func NewPodCache(kubeClient kubernetes.Interface, nodeName string) PodCache {
	podCache := &podCacheImpl{}

	podCache.kubeClient = kubeClient
	podCache.nodeName = nodeName
	podCache.stopChan = make(chan struct{})

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, time.Minute,
		informers.WithTweakListOptions(func(lo *v1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}))

	podCache.informer = informerFactory.Core().V1().Pods()

	go podCache.informer.Informer().Run(podCache.stopChan)

	// Wait for the informer to synchronize before returning.
	for !podCache.informer.Informer().HasSynced() {
		time.Sleep(time.Second)
	}

	klog.V(2).Infof("PodCache WatchDog is now running")
	return podCache
}

func (c *podCacheImpl) GetActivePodIDs() sets.Set[string] {
	activePodIDs := sets.New[string]()

	for _, value := range c.informer.Informer().GetStore().List() {
		pod, ok := value.(*corev1.Pod)
		if !ok {
			continue
		}

		// Don't return Pods that have been terminated.
		if isPodTerminated(pod) {
			continue
		}

		if !podRequiresVirtualGPUs(pod) {
			continue
		}

		activePodIDs.Insert(string(pod.UID))
	}

	return activePodIDs
}

func (c *podCacheImpl) GetActivePods() map[string]*corev1.Pod {
	activePods := make(map[string]*corev1.Pod)

	for _, value := range c.informer.Informer().GetStore().List() {
		pod, ok := value.(*corev1.Pod)
		if !ok {
			continue
		}

		// Don't return Pods that have been terminated.
		if isPodTerminated(pod) {
			continue
		}

		if !podRequiresVirtualGPUs(pod) {
			continue
		}

		activePods[string(pod.UID)] = pod
	}

	return activePods
}

func (c *podCacheImpl) GetPod(namespace string, podName string) (*corev1.Pod, error) {
	pod, err := c.informer.Lister().Pods(namespace).Get(podName)
	if err != nil {
		return nil, err
	}

	if isPodTerminated(pod) {
		return nil, fmt.Errorf("the specified Pod has been terminated")
	}

	if !podRequiresVirtualGPUs(pod) {
		return nil, fmt.Errorf("the specified Pod does not require GPUs")
	}

	return pod, nil
}

func (c *podCacheImpl) StopChan() chan struct{} {
	return c.stopChan
}

func (c *podCacheImpl) Informer() informersCore.PodInformer {
	return c.informer
}
