package security

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/go-logr/logr"
	"github.com/go-openapi/swag"
	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/stolostron/multicluster-global-hub/agent/pkg/clients"
	"github.com/stolostron/multicluster-global-hub/agent/pkg/status/controller/generic"
	"github.com/stolostron/multicluster-global-hub/pkg/bundle/version"
	"github.com/stolostron/multicluster-global-hub/pkg/enum"
	"github.com/stolostron/multicluster-global-hub/pkg/transport"
)

const (
	stackroxCentralServiceName = "central"
	stackroxCentralRouteName   = "central"

	// We need to explicitly disable 'gosec' because it wrongly thinks that this value is a secret, when it is only
	// the name of a secret.
	stackroxSecretAnnotation = "global-hub.open-cluster-management.io/with-stackrox-credentials-secret" // nolint:gosec

)

type stackroxDataSyncer struct {
	mgr                     manager.Manager
	ticker                  *time.Ticker
	topic                   string
	producer                transport.Producer
	client                  client.Client
	stackroxCentralRequests []stackroxCentralRequest
	log                     logr.Logger
}

// stackroxConnectionDetails contains the details needed to connect to the StackRox API.
type stackroxConnectionDetails struct {
	url   string
	token string
	ca    *x509.CertPool
}

func (s *stackroxDataSyncer) produceToKafka(ctx context.Context, messageStruct any) error {
	version, err := version.VersionFrom("0.1")
	if err != nil {
		return fmt.Errorf("failed to get version instance: %v", err)
	}
	emitter := generic.NewGenericEmitter(
		enum.SecurityAlertCountsType,
		messageStruct,
		generic.WithTopic(s.topic),
		generic.WithVersion(version),
	)

	evt, err := emitter.ToCloudEvent()
	if err != nil {
		return fmt.Errorf("failed to get CloudEvent instance from event %s: %v", *evt, err)
	}

	if !emitter.ShouldSend() {
		s.log.Info("should not send message to kafka", "topic", s.topic, "message", string(evt.Data()))
		return nil
	}

	s.log.Info("pushing message to kafka", "topic", s.topic, "message", string(evt.Data()))
	cloudEventsContext := cecontext.WithTopic(ctx, emitter.Topic())

	if err = s.producer.SendEvent(cloudEventsContext, *evt); err != nil {
		return fmt.Errorf("failed to send event %s: %v", *evt, err)
	}

	return nil
}

func (s *stackroxDataSyncer) fetchCentralData(ctx context.Context, central stackroxCentral) error {
	centralData := stackroxCentrals[central]

	mutex.Lock()
	defer mutex.Unlock()

	externalBaseURL, err := s.getCentralExternalBaseURL(ctx, central)
	if err != nil {
		return fmt.Errorf(
			"failed to get central external base URL (name: %s, namespace: %s): %v",
			central.name, central.namespace, err,
		)
	}
	centralData.externalBaseURL = *externalBaseURL

	connectionDetails, err := s.getCentralConnectionDetails(ctx, central)
	if err != nil {
		return err
	}

	apiClient, err := clients.NewStackroxClient().
		SetLogger(s.log).
		SetURL(connectionDetails.url).
		SetToken(connectionDetails.token).
		SetCA(connectionDetails.ca).
		Build()
	if err != nil {
		return err
	}
	centralData.apiClient = apiClient

	return nil
}

func (s *stackroxDataSyncer) pollStackroxCentral(ctx context.Context, client *clients.StackRoxClient,
	request stackroxCentralRequest, central stackroxCentral,
) error {
	response, statusCode, err := client.DoRequest(request.Method, request.Path, request.Body)
	if err != nil {
		return err
	}

	if *statusCode != 200 {
		if err := s.fetchCentralData(ctx, central); err != nil {
			return fmt.Errorf("failed to fetch central data on the second attempt: %v", err)
		}
	}

	err = json.Unmarshal(response, request.CacheStruct)
	if err != nil {
		return fmt.Errorf(
			"failed to unmarshal response (method: %s, path: %s, body: %s): %v",
			request.Method, request.Path, request.Body, err,
		)
	}

	return nil
}

func (s *stackroxDataSyncer) reconcileCentralInstance(ctx context.Context, central stackroxCentral) error {
	centralData := stackroxCentrals[central]

	for _, request := range s.stackroxCentralRequests {
		if err := s.pollStackroxCentral(ctx, centralData.apiClient, request, central); err != nil {
			return fmt.Errorf("failed to make a request to central: %v", err)
		}

		messageStruct, err := request.GenerateFromCache(request.CacheStruct, centralData.externalBaseURL)
		if err != nil {
			return fmt.Errorf("failed to generate struct for kafka message: %v", err)
		}

		if err := s.produceToKafka(ctx, messageStruct); err != nil {
			return fmt.Errorf("failed to produce a message to kafka: %v", err)
		}
	}

	return nil
}

func (s *stackroxDataSyncer) reconcile(ctx context.Context) error {
	for central, centralData := range stackroxCentrals {
		if centralData.apiClient == nil || centralData.externalBaseURL == "" {
			if err := s.fetchCentralData(ctx, central); err != nil {
				return fmt.Errorf(
					"failed to fetch central data on the first attempt (name: %s, namespace: %s): %v",
					central.name, central.namespace, err,
				)
			}
		}

		if err := s.reconcileCentralInstance(ctx, central); err != nil {
			return fmt.Errorf(
				"failed to reconcile central instance (name: %s, namespace: %s): %v",
				central.name, central.namespace, err,
			)
		}
	}

	return nil
}

func (a *stackroxDataSyncer) getCentralInternalBaseURL(central stackroxCentral) string {
	url := url.URL{
		Scheme: "https",
		Host:   fmt.Sprintf("%s.%s.svc", stackroxCentralServiceName, central.namespace),
	}
	return url.String()
}

func (a *stackroxDataSyncer) getCentralExternalBaseURL(ctx context.Context, central stackroxCentral) (*string, error) {
	centralRoute, err := getResource(ctx, a.client, stackroxCentralServiceName, central.namespace, &routev1.Route{}, a.log)
	if err != nil {
		return nil, err
	} else if centralRoute == nil {
		return nil, nil
	}

	centralRouteAsserted, ok := centralRoute.(*routev1.Route)
	if !ok {
		return nil, fmt.Errorf(
			"failed to assert central route (name: %s, namespace: %s)",
			central.name, central.namespace,
		)
	}

	return swag.String(centralRouteAsserted.Spec.Host), nil
}

func (a *stackroxDataSyncer) getCentralConnectionDetails(ctx context.Context,
	central stackroxCentral,
) (result *stackroxConnectionDetails, err error) {
	// Add the central namespace and name to the log:
	log := a.log.WithValues(
		"name", central.name,
		"namespace", central.namespace,
	)

	// Fetch the central:
	centralObject := &unstructured.Unstructured{}
	centralObject.SetGroupVersionKind(centralCRGVK)
	centralObjectKey := client.ObjectKey{
		Namespace: central.namespace,
		Name:      central.name,
	}
	err = a.client.Get(ctx, centralObjectKey, centralObject)
	if apierrors.IsNotFound(err) {
		a.log.Info("StackRox central object doesn't exist")
		return
	}

	// Get the location of the secret containing the connection details:
	annotations := centralObject.GetAnnotations()
	if annotations == nil {
		a.log.Info("StackRox central doesn't have annotations")
		return
	}
	annotation, ok := annotations[stackroxSecretAnnotation]
	if !ok {
		a.log.Info(
			"StackRox central doesn't have the annotation that contains the namespace and name of the "+
				"secret containing the connection details",
			"annotation", stackroxSecretAnnotation,
		)
		return
	}
	parts := strings.Split(annotation, "/")
	if len(parts) != 2 {
		a.log.Error(
			nil,
			"The fomat of the annotation that contains the namespace and name of the connection details "+
				"secret should be 'namespace/name",
			"annotation", stackroxSecretAnnotation,
			"value", annotation,
		)
		return
	}
	detailsSecretNamespace := parts[0]
	detailsSecretName := parts[1]
	log = log.WithValues(
		"secret_namespace", detailsSecretNamespace,
		"secret_name", detailsSecretName,
	)
	log.Info("Got location of the StackRox connection details secret")

	// Fetch the secret:
	detailsSecret := &corev1.Secret{}
	detailsSecretKey := client.ObjectKey{
		Namespace: detailsSecretNamespace,
		Name:      detailsSecretName,
	}
	err = a.client.Get(ctx, detailsSecretKey, detailsSecret)
	if apierrors.IsNotFound(err) {
		log.Info("StackRox connection details secret doesn't exist")
		err = nil
		return
	}
	if err != nil {
		return
	}

	// Get the URL:
	var url string
	urlBytes, ok := detailsSecret.Data["url"]
	if ok {
		url = strings.TrimSpace(string(urlBytes))
		log.Info(
			"Got StackRox URL from secret",
			"url", url,
		)
	} else {
		url = a.getCentralInternalBaseURL(central)
		log.Info(
			"StackRox connection details secret doesn't contain the 'url' key, will use the default",
			"url", url,
		)
	}

	// Get the token:
	var token string
	tokenBytes, ok := detailsSecret.Data["token"]
	if ok {
		token = strings.TrimSpace(string(tokenBytes))
		log.Info(
			"Got StackRox token from connection details secret",
			"token_length", len(token),
			"token_text", fmt.Sprintf("%.3s...", token),
		)
	} else {
		log.Error(
			nil,
			"StackRox connection details secret doesn't contain the 'token' key",
		)
		err = fmt.Errorf(
			"connection details secret '%s/%s' doesn't contain the mandatory 'token' key",
			detailsSecret.Namespace, detailsSecret.Name,
		)
		return
	}

	// Load the system CA certificates:
	ca, err := x509.SystemCertPool()
	if err != nil {
		return
	}

	// Add the CA given in the connection details secret:
	caBytes, ok := detailsSecret.Data["ca"]
	if ok {
		ok = ca.AppendCertsFromPEM(caBytes)
		if !ok {
			log.Error(
				nil,
				"The 'ca' key of the connection details secret doesn't contain any CA certificate",
				"ca", string(caBytes),
			)
			err = fmt.Errorf(
				"the 'ca' key of the connection details secret '%s/%s' doesn't contain any CA "+
					"certificate",
				detailsSecretKey.Namespace, detailsSecretKey.Name,
			)
			return
		}
		log.Info(
			"Loaded CA certificates from the 'ca' key of the connection details secret",
			"ca_pem", string(caBytes),
		)
	}

	// Return the result:
	result = &stackroxConnectionDetails{
		url:   url,
		token: token,
		ca:    ca,
	}
	return
}

func (a *stackroxDataSyncer) Start(ctx context.Context) error {
	a.log.Info("started stackrox data syncer...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-a.ticker.C:
				if err := a.reconcile(ctx); err != nil {
					a.log.Error(err, "failed to reconcile stackrox central data")
				}
			}
		}
	}()
	<-ctx.Done()

	a.log.Info("stopped stackrox data syncer")

	return nil
}

func AddStackroxDataSyncer(
	mgr manager.Manager,
	interval time.Duration,
	topic string,
	producer transport.Producer,
) error {
	return mgr.Add(&stackroxDataSyncer{
		mgr:                     mgr,
		client:                  mgr.GetClient(),
		ticker:                  time.NewTicker(interval),
		topic:                   topic,
		producer:                producer,
		stackroxCentralRequests: stackroxCentralRequests,
		log:                     ctrl.Log.WithName("stackrox-data-syncer"),
	})
}
