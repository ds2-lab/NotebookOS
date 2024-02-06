package main

import (
	"context"
	"flag"
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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

func main() {
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

	podName := "test-pod"

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
