package consul

import (
	"fmt"
	"os"
	"time"

	discovery "github.com/adammck/ranger/pkg/discovery"
	"github.com/hashicorp/consul/api"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
)

type Discovery struct {
	svcName string
	addrPub string
	consul  *consul.Client
	srv     *grpc.Server
	hs      *health.Server
}

func New(serviceName, addrPub string, cfg *consul.Config, srv *grpc.Server) (*Discovery, error) {
	client, err := consul.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	d := &Discovery{
		svcName: serviceName,
		addrPub: addrPub,

		consul: client,
		srv:    srv,
		hs:     health.NewServer(),
	}

	d.hs.SetServingStatus("", hv1.HealthCheckResponse_SERVING)
	hv1.RegisterHealthServer(d.srv, d.hs)

	return d, nil
}

func (d *Discovery) Start() error {
	// lis, err := net.Listen("tcp", d.addrLis)
	// if err != nil {
	// 	return err
	// }

	// go func() {
	// 	// Blocks until GracefulStop is called by d.Stop
	// 	// TODO: Do something with the return value?
	// 	d.srv.Serve(lis)
	// }()

	def := &consul.AgentServiceRegistration{
		Name: d.svcName,
		ID:   fmt.Sprintf("%s-%d", d.svcName, os.Getpid()), // ???

		Check: &consul.AgentServiceCheck{
			GRPC:     d.addrPub,
			Interval: (3 * time.Second).String(),
			Timeout:  (10 * time.Second).String(),
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

	// Allows RPCs to complete before closing.
	// Can't return error.
	//d.srv.GracefulStop()

	return nil
}

func (d *Discovery) Get(name string) ([]discovery.Remote, error) {
	res, _, err := d.consul.Catalog().Service("node", "", &api.QueryOptions{})
	if err != nil {
		return []discovery.Remote{}, err
	}

	output := make([]discovery.Remote, len(res))
	for i, r := range res {
		//fmt.Printf("%d: %+v\n", i, r)
		output[i] = discovery.Remote{
			Ident: r.ServiceID,
			Host:  r.Address,
			Port:  r.ServicePort,
		}
	}

	return output, nil
}
