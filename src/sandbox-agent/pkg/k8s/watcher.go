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
}

func NewWatcher(clientset *kubernetes.Clientset, resync time.Duration) *Watcher {
	if resync <= 0 {
		resync = 5 * time.Minute
	}

	return &Watcher{
		clientset: clientset,
		factory:   informers.NewSharedInformerFactory(clientset, resync),
		store:     NewStore(),
		resync:    resync,
	}
}

func (w *Watcher) Store() *Store {
	return w.store
}

func (w *Watcher) Start(ctx context.Context) error {
	podInformer := w.factory.Core().V1().Pods().Informer()
	nodeInformer := w.factory.Core().V1().Nodes().Informer()

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

	nodeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if ok {
				w.store.SetNode(node)
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			node, ok := newObj.(*corev1.Node)
			if ok {
				w.store.SetNode(node)
			}
		},
		DeleteFunc: func(obj interface{}) {
			node, ok := obj.(*corev1.Node)
			if ok {
				w.store.DeleteNode(node)
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
