package consul

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/adammck/ranger/pkg/api"
	consulapi "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type Discovery struct {
	svcName string
	host    string
	port    int
	consul  *consulapi.Client
	srv     *grpc.Server
	hs      *health.Server
}

func (d *Discovery) getIdent() string {
	if d.host == "" || d.host == "localhost" || d.host == "127.0.0.1" {
		return fmt.Sprintf("%d", d.port)
	}

	return fmt.Sprintf("%s:%d", d.host, d.port)
}

// TODO: Take a consul API here, not a cfg.
func New(serviceName, addr string, cfg *consulapi.Config, srv *grpc.Server) (*Discovery, error) {
	client, err := consulapi.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	// Extract host:port from the given address.
	// TODO: Maybe better to do this outside?
	host, sPort, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	nPort, err := strconv.Atoi(sPort)
	if err != nil {
		return nil, err
	}

	d := &Discovery{
		svcName: serviceName,
		host:    host,
		port:    nPort,

		consul: client,
		srv:    srv,
		hs:     health.NewServer(),
	}

	d.hs.SetServingStatus("", hv1.HealthCheckResponse_SERVING)
	hv1.RegisterHealthServer(d.srv, d.hs)

	return d, nil
}

func (d *Discovery) Start() error {
	def := &consulapi.AgentServiceRegistration{
		Name: d.svcName,
		ID:   d.getIdent(),

		// How other nodes should call the service.
		Address: d.host,
		Port:    d.port,

		Check: &consulapi.AgentServiceCheck{
			GRPC: fmt.Sprintf("%s:%d", d.host, d.port),

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

	// TODO: Send this in a loop while running, in case Consul dies.
	err := d.consul.Agent().ServiceRegister(def)
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) Stop() error {
	err := d.consul.Agent().ServiceDeregister(d.getIdent())
	if err != nil {
		return err
	}

	return nil
}

func (d *Discovery) Get(name string) ([]api.Remote, error) {
	res, _, err := d.consul.Catalog().Service(name, "", &consulapi.QueryOptions{})
	if err != nil {
		return []api.Remote{}, err
	}

	output := make([]api.Remote, len(res))
	for i, r := range res {
		output[i] = api.Remote{
			Ident: r.ServiceID,
			Host:  r.Address, // https://github.com/hashicorp/consul/issues/2076
			Port:  r.ServicePort,
		}
	}

	return output, nil
}
