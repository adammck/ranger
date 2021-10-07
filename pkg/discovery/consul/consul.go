package consul

import (
	"fmt"
	"net"
	"strconv"
	"time"

	discovery "github.com/adammck/ranger/pkg/discovery"
	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type Discovery struct {
	svcName string
	addrPub string
	ident   string
	consul  *api.Client
	srv     *grpc.Server
	hs      *health.Server
}

func getIdent(addr string) (string, error) {
	host, sPort, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	nPort, err := strconv.Atoi(sPort)
	if err != nil {
		return "", err
	}

	if host == "" || host == "localhost" || host == "127.0.0.1" {
		return fmt.Sprintf("%d", nPort), nil
	}

	return fmt.Sprintf("%s:%d", host, nPort), nil
}

func New(serviceName, addrPub string, cfg *api.Config, srv *grpc.Server) (*Discovery, error) {
	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	ident, err := getIdent(addrPub)
	if err != nil {
		return nil, err
	}

	d := &Discovery{
		svcName: serviceName,
		addrPub: addrPub,
		ident:   ident,

		consul: client,
		srv:    srv,
		hs:     health.NewServer(),
	}

	d.hs.SetServingStatus("", hv1.HealthCheckResponse_SERVING)
	hv1.RegisterHealthServer(d.srv, d.hs)

	return d, nil
}

func (d *Discovery) Start() error {
	def := &api.AgentServiceRegistration{
		Name: d.svcName,
		ID:   d.ident,

		Check: &api.AgentServiceCheck{
			GRPC: d.addrPub,

			// How long to wait between checks.
			Interval: (3 * time.Second).String(),

			// How long to wait for a response before giving up.
			Timeout: (1 * time.Second).String(),

			// How long to wait after a service becomes critical (i.e. starts
			// returning error, unhealthy responses, or timing out) before
			// removing it from service discovery. Might actually take longer
			// than this because of Consul implementation.
			DeregisterCriticalServiceAfter: (10 * time.Second).String(),
		},
	}

	err := d.consul.Agent().ServiceRegister(def)
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) Stop() error {
	err := d.consul.Agent().ServiceDeregister(d.svcName)
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) Get(name string) ([]discovery.Remote, error) {
	res, _, err := d.consul.Catalog().Service("node", "", &api.QueryOptions{})
	if err != nil {
		return []discovery.Remote{}, err
	}

	output := make([]discovery.Remote, len(res))
	for i, r := range res {
		output[i] = discovery.Remote{
			Ident: r.ServiceID,
			Host:  r.Address,
			Port:  r.ServicePort,
		}
	}

	return output, nil
}
