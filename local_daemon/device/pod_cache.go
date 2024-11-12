package device

import (
	"context"
	"fmt"
	"github.com/zhangjyr/distributed-notebook/common/types"
	"os"
	"time"

	"github.com/Scusemua/go-utils/config"
	"github.com/Scusemua/go-utils/logger"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	informersCore "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
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
	log        logger.Logger
}

func NewPodCache(nodeName string) PodCache {
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		panic(err.Error())
	}

	podCache := &podCacheImpl{
		nodeName:   nodeName,
		kubeClient: clientset,
		stopChan:   make(chan struct{}),
	}

	config.InitLogger(&podCache.log, podCache)

	podCache.log.Debug("Creating InformerFactory now.")
	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, time.Second*30,
		informers.WithTweakListOptions(func(lo *metav1.ListOptions) {
			lo.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}),
		informers.WithNamespace("default") /* TODO(Ben): Make the namespace configurable. */)

	podCache.informer = informerFactory.Core().V1().Pods()

	podCache.log.Debug("Starting informer now.")
	go podCache.informer.Informer().Run(podCache.stopChan)

	podCache.log.Debug("Waiting for Informer to synchronize...")

	// Start to sync and call list
	if !cache.WaitForCacheSync(podCache.stopChan, podCache.informer.Informer().HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return nil
	}

	if _, err = podCache.informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if ok {
				podCache.log.Debug("Pod created: %s/%s", pod.Namespace, pod.Name)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod, ok := newObj.(*corev1.Pod)
			if ok {
				podCache.log.Debug("Pod updated: %s/%s", pod.Namespace, pod.Name)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if ok {
				podCache.log.Debug("Pod deleted: %s/%s", pod.Namespace, pod.Name)
			}
		},
	}); err != nil {
		podCache.log.Error("Failed to add event handler to pod cache's informer because: %v", err)
		return nil
	}

	podCache.log.Debug("Informer has synchronized successfully. PodCache WatchDog is now running.")
	klog.V(2).Infof("PodCache WatchDog is now running.")
	return podCache
}

func (c *podCacheImpl) GetActivePodIDs() StringSet {
	activePodIDs := sets.New[string]()

	for _, value := range c.informer.Informer().GetStore().List() {
		pod, ok := value.(*corev1.Pod)
		if !ok {
			continue
		}

		// Don't return Pods that have been terminated.
		if IsPodTerminated(pod) {
			continue
		}

		if !PodRequiresVirtualGPUs(pod) {
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
		if IsPodTerminated(pod) {
			continue
		}

		if !PodRequiresVirtualGPUs(pod) {
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

	if IsPodTerminated(pod) {
		return nil, fmt.Errorf("the specified Pod has been terminated")
	}

	if !PodRequiresVirtualGPUs(pod) {
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

// GetPodsRunningOnNode returns the Pods running on the specified node.
// Optionally return only the Pods in a particular phase by passing a pod phase via the `podPhase` parameter.
// If you do not want to restrict the Pods to any particular phase, then pass the empty string for the `podPhase` parameter.
func (c *podCacheImpl) GetPodsRunningOnNode(nodeName string, podPhase string) ([]corev1.Pod, error) {
	if nodeName == "" || nodeName == types.DockerNode {
		nodeName, _ = os.Hostname()
	}

	var pods []corev1.Pod
	var selector fields.Selector

	if podPhase == "" {
		selector = fields.SelectorFromSet(fields.Set{
			"spec.nodeName": nodeName,
		})
	} else {
		selector = fields.SelectorFromSet(fields.Set{
			"spec.nodeName": nodeName,
			"status.phase":  podPhase,
		})
	}

	deadline := time.Minute
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), deadline)
	defer deadlineCancel()
	err := wait.PollUntilContextTimeout(deadlineCtx, time.Second, deadline, true, func(ctx context.Context) (bool, error) {
		podList, err := c.kubeClient.CoreV1().Pods(corev1.NamespaceAll).List(ctx, metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			return false, err
		} else {
			pods = podList.Items
		}
		return true, nil
	})

	if err != nil {
		return pods, fmt.Errorf("failed to retrieve list of Pods running on node %s because: %v", nodeName, err)
	}

	return pods, nil
}
