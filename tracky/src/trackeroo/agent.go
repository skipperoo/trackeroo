package trackeroo

import (
	"strings"
)

type Agent struct {
	Task
	tdmClient *TdmClient
}

func NewAgent(tdmClient *TdmClient) *Agent {
	agent := new(Agent)
	agent.name = "Agent"
	agent.kicked = UnixTime()
	agent.wtd = 120
	agent.tdmClient = tdmClient
	return agent
}

func (a *Agent) run() {
	a.running = true
	for a.running {
		a.Kick()
		if a.tdmClient == nil || a.tdmClient.client == nil || !a.tdmClient.client.IsConnected() {
			Info("Client is not currently connected to the tdm, waiting...")
			Millisleep(100)
			continue
		}
		qm_size, _ := Size("data")
		if qm_size > 0 {
			id, data, _ := DequeueData("data")
			if str, ok := data.(string); ok {
				tag, payload, _ := strings.Cut(string(str), "/")
				for err := a.tdmClient.Publish(tag, payload); err != nil; {
					Error("Cannot publish %v: %v. Retrying...", payload, err)
					a.Kick()
					Millisleep(1000)
				}
				Ack("data", id)

				Info("Published %v!", str)

			} else {
				Error("Cannot publish data != string -> %v", data)
				Ack("data", id)
			}
			Millisleep(100)
		} else {
			if UnixTime()%30 == 0 {
				Debug("No data, sleeping...")
			}
			Millisleep(1000)
		}
	}
}

func (a *Agent) Start() {
	go a.run()
	Debug("Started Agent goroutine")
}
