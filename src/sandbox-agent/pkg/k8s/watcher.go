package k8s

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type Watcher struct {
	clientset *kubernetes.Clientset
	factory   informers.SharedInformerFactory
	store     *Store
	resync    time.Duration
	namespace string
}

// NewWatcher creates a new watcher that watches resources in the specified namespace.
// If namespace is empty, it watches all namespaces (requires cluster-scoped RBAC).
func NewWatcher(clientset *kubernetes.Clientset, resync time.Duration, namespace string) *Watcher {
	if resync <= 0 {
		resync = 5 * time.Minute
	}

	var factory informers.SharedInformerFactory
	if namespace != "" {
		// Namespace-scoped watching - only requires namespace-level RBAC
		factory = informers.NewSharedInformerFactoryWithOptions(
			clientset,
			resync,
			informers.WithNamespace(namespace),
		)
	} else {
		// Cluster-scoped watching - requires cluster-level RBAC
		factory = informers.NewSharedInformerFactory(clientset, resync)
	}

	return &Watcher{
		clientset: clientset,
		factory:   factory,
		store:     NewStore(),
		resync:    resync,
		namespace: namespace,
	}
}

func (w *Watcher) Store() *Store {
	return w.store
}

func (w *Watcher) Start(ctx context.Context) error {
	podInformer := w.factory.Core().V1().Pods().Informer()

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if ok {
				w.store.SetPod(pod)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			pod, ok := newObj.(*corev1.Pod)
			if ok {
				w.store.SetPod(pod)
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if ok {
				w.store.DeletePod(pod)
			}
		},
	})

	w.factory.Start(ctx.Done())

	return nil
}

func (w *Watcher) WaitForSync(ctx context.Context) error {
	for informerType, synced := range w.factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			runtime.HandleError(fmt.Errorf("failed to sync informer %v", informerType))
			return fmt.Errorf("failed to sync informer %v", informerType)
		}
	}

	return nil
}
