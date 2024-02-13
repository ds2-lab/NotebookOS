package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"k8s.io/client-go/util/retry"
)

// Used to patch the metadata of a Pod.
type LabelPatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

func getKubeClientset() *kubernetes.Clientset {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// Creates the Clientset.
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset
}

func getKubeDynamicClient() *dynamic.DynamicClient {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// Create the "Dynamic" client, which is used for unstructured components, such as CloneSets.
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return dynamicClient
}

func testAddAnnotations() {
	clientset := getKubeClientset()
	podName := "test-pod"
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		payload := `{"metadata": {"labels": {"apps.kruise.io/specified-delete": "true"}}}`
		_, updateErr := clientset.CoreV1().Pods("default").Patch(context.Background(), podName, types.MergePatchType, []byte(payload), metav1.PatchOptions{})
		if updateErr != nil {
			fmt.Printf("Error when updating labels for Pod \"%s\": %v\n", podName, updateErr)
			return errors.Wrapf(updateErr, fmt.Sprintf("Failed to add deletion label to Pod \"%s\".", podName))
		}

		fmt.Printf("Pod %s labelled successfully.", podName)
		return nil
	})

	if err != nil {
		fmt.Printf("Error when updating labels for Pod \"%s\": %v\n", podName, err)
	}

	pod, err := clientset.CoreV1().Pods("default").Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		fmt.Printf("Could not find Pod \"%s\" in namespace \"%s\": %v\n", podName, "default", err)
	}

	var annotations map[string]string = pod.GetAnnotations()

	annotations["apps.kruise.io/specified-delete"] = "true"
	annotations["controller.kubernetes.io/pod-deletion-cost"] = "-2147483647"

	pod.SetAnnotations(annotations)

	pod, err = clientset.
		CoreV1().
		Pods("default").
		Update(context.TODO(), pod, metav1.UpdateOptions{})

	if err != nil {
		fmt.Printf("Failed to update annotations of Pod %s/%s: %v", "default", podName, err)
	}
}

func testUpdateCloneSet() {
	dynamicClient := getKubeDynamicClient()

	clonesetRes := schema.GroupVersionResource{Group: "apps.kruise.io", Version: "v1alpha1", Resource: "clonesets"}
	cloneset_id := "sample-data"
	oldPodNames := []interface{}{"sample-data-9jd28", "sample-data-pnqb8", "sample-data-z7cgp", "sample-data-7r8xp"}
	// oldPodNames := make([]interface{}, 0)
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		result, getErr := dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Get(context.TODO(), cloneset_id, metav1.GetOptions{})

		if getErr != nil {
			panic(fmt.Errorf("Failed to get latest version of CloneSet \"%s\": %v", cloneset_id, getErr))
		}

		current_num_replicas, found, err := unstructured.NestedInt64(result.Object, "spec", "replicas")

		if err != nil || !found {
			log.Fatalf("[ERROR] Replicas not found for CloneSet %s: error=%v\n", cloneset_id, err)
		}

		fmt.Printf("Attempting to DECREASE the number of replicas of CloneSet \"%s\" by deleting pods \"%v\". Currently, it is configured to have %d replicas.\n", cloneset_id, oldPodNames, current_num_replicas)
		new_num_replicas := current_num_replicas - 4

		// Decrease the number of replicas.
		if err := unstructured.SetNestedField(result.Object, new_num_replicas, "spec", "replicas"); err != nil {
			panic(fmt.Errorf("Failed to set spec.replicas value for CloneSet \"%s\": %v", cloneset_id, err))
		}

		if err := unstructured.SetNestedField(result.Object, oldPodNames, "spec", "scaleStrategy", "podsToDelete"); err != nil {
			panic(fmt.Errorf("Failed to set spec.replicas value for CloneSet \"%s\": %v", cloneset_id, err))
		}

		_, updateErr := dynamicClient.Resource(clonesetRes).Namespace(corev1.NamespaceDefault).Update(context.TODO(), result, metav1.UpdateOptions{})

		if updateErr != nil {
			log.Fatalf("Failed to apply update to CloneSet \"%s\": error=%v\n", cloneset_id, updateErr)
		} else {
			fmt.Printf("Successfully decreased number of replicas of CloneSet \"%s\" to %d.\n", cloneset_id, new_num_replicas)
		}

		return updateErr
	})

	if retryErr != nil {
		log.Fatalf("Failed to scale-in CloneSet: %v\n", retryErr)
	}
}

func main() {
	testUpdateCloneSet()
}
