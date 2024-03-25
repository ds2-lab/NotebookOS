package device

import (
	"context"
	"net"
	"sort"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// waitForDevicePluginServer checks if the DevicePlugin gRPC server is alive.
// This is done by creating a blocking gRPC connection to the server's socket.
// by making grpc blocking connection to the server socket.
func waitForDevicePluginServer(sock string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	defer cancel()

	conn, err := grpc.DialContext(ctx, sock,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
	)
	if conn != nil {
		_ = conn.Close()
	}

	// If err is nil, Wrapf returns nil.
	return errors.Wrapf(err, "Failed gRPC::DialContext for socket \"%s\"", sock)
}

// Return true if the Pod is terminated.
// Otherwise, return false.
func isPodTerminated(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed || pod.Status.Phase == corev1.PodSucceeded || (pod.DeletionTimestamp != nil && verifyNotRunning(pod.Status.ContainerStatuses))
}

// Return the number of virtual GPUs required by the given Pod.
func getVirtualGpuRequirementsOfPod(pod *corev1.Pod) int32 {
	var numVirtualGPUsRequired int32 = 0

	for _, container := range pod.Spec.Containers {
		if requiredVirtualGPUs, ok := container.Resources.Limits[VDeviceAnnotation]; ok {
			numVirtualGPUsRequired += int32(requiredVirtualGPUs.Value())
		}
	}

	return numVirtualGPUsRequired
}

// Return true if the Pod requires vGPUs. Otherwise, return false.
func podRequiresVirtualGPUs(pod *corev1.Pod) bool {
	return getVirtualGpuRequirementsOfPod(pod) > 0
}

// Check if pod has already been assigned
func podHasVirtualGPUsAllocated(pod *corev1.Pod) bool {
	if assigned, ok := pod.ObjectMeta.Annotations[VirtualGPUAssigned]; !ok {
		klog.V(3).Infof("No assigned flag for pod %s in namespace %s",
			pod.Name,
			pod.Namespace)
		return false
	} else if assigned == "false" {
		klog.V(3).Infof("pod %s in namespace %s has not been assigned",
			pod.Name,
			pod.Namespace)
		return false
	}

	return true
}

func getCreationTimeOfPod(pod *v1.Pod) (predicateTime uint64) {
	return uint64(pod.ObjectMeta.CreationTimestamp.UnixNano())
}

type PodsOrderedByCreationTime []*v1.Pod

func (pods PodsOrderedByCreationTime) Len() int {
	return len(pods)
}

func (pods PodsOrderedByCreationTime) Less(i, j int) bool {
	return getCreationTimeOfPod(pods[i]) <= getCreationTimeOfPod(pods[j])
}

func (pods PodsOrderedByCreationTime) Swap(i, j int) {
	pods[i], pods[j] = pods[j], pods[i]
}

func sortPodsByCreationTime(pods []*v1.Pod) []*v1.Pod {
	newPodList := make(PodsOrderedByCreationTime, 0, len(pods))
	for _, v := range pods {
		newPodList = append(newPodList, v)
	}
	sort.Sort(newPodList)
	return []*v1.Pod(newPodList)
}
