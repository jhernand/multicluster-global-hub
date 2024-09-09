package security

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type stackroxCentralCRController struct {
	mgr       manager.Manager
	log       logr.Logger
	client    client.Client
	centralCR *unstructured.Unstructured
}

func (s *stackroxCentralCRController) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	reqLogger := s.log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("ACS Central CR controller", "NamespacedName:", request.NamespacedName)

	centralCR, err := getResource(ctx, s.client, request.Name, request.Namespace, s.centralCR, reqLogger)
	if err != nil {
		return ctrl.Result{}, err
	} else if centralCR == nil {
		if _, ok := stackroxCentrals[request.NamespacedName]; ok {
			reqLogger.Info("Deleting Stackrox Central instance")
			delete(stackroxCentrals, request.NamespacedName)
		}
		return ctrl.Result{}, nil
	}

	if _, ok := stackroxCentrals[request.NamespacedName]; !ok {
		reqLogger.Info("Found a new central instance to sync")
		stackroxCentrals[request.NamespacedName] = &stackroxCentralData{}
	}

	return ctrl.Result{}, nil
}

func AddStackroxCentralController(mgr ctrl.Manager) error {
	central := &unstructured.Unstructured{}
	central.SetGroupVersionKind(centralCRGVK)

	controller := stackroxCentralCRController{
		mgr:       mgr,
		log:       ctrl.Log.WithName("stackrox-central-cr-controller"),
		client:    mgr.GetClient(),
		centralCR: central,
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		For(central).
		Complete(&controller); err != nil {
		return fmt.Errorf("failed to create Stackrox Central CR controller: %v", err)
	}

	return nil
}
