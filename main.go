package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version"
	"k8s.io/kubernetes/cmd/kube-scheduler/app"

	"github.com/santoshkal/gpusim-scheduler/pkg/gpusim"
)

func main() {
	command := app.NewSchedulerCommand(
		app.WithPlugin(gpusim.Name, gpusim.New),
	)
	os.Exit(cli.Run(command))
}
