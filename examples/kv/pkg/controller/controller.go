package controller

import (
	"log"
	"time"

	consul "github.com/hashicorp/consul/api"
)

type Controller struct {
	name  string
	agent *consul.Agent
}

func (c *Controller) Ping() bool {
	return true
}

func check(c *Controller, a *consul.Agent) {
	ok := c.Ping()

	var status string
	if ok {
		status = "pass"
	} else {
		status = "fail"
	}

	err := a.UpdateTTL("service:controller-xyz", "", status)
	if err != nil {
		log.Print(err)
	}
}

func Start(addr string) error {
	c := Controller{
		name: "controller",
	}
	ttl := 10 * time.Second

	cc, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		return err
	}
	c.agent = cc.Agent()

	serviceDef := &consul.AgentServiceRegistration{
		Name: c.name,
		ID:   "controller-xyz",

		Check: &consul.AgentServiceCheck{
			//CheckID: "xyz",
			TTL: ttl.String(),
		},
	}

	if err := c.agent.ServiceRegister(serviceDef); err != nil {
		return err
	}

	// TODO: Stop the ticker on shutdown
	go func() {
		ticker := time.NewTicker(ttl / 3)
		for range ticker.C {
			check(&c, c.agent)
		}
	}()

	select {}
}
