package pytorch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"

	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	commonutil "github.com/kubeflow/training-operator/pkg/util"
)

var (
	masterGenerator EnvVarGenerator
	onceMaster      sync.Once
	EnvMasterPort   = "MASTER_PORT"
	EnvMasterAddr   = "MASTER_ADDR"

	PETMasterPort = "PET_MASTER_PORT"
	PETMasterAddr = "PET_MASTER_ADDR"
)

// MasterEnvVarGenerator is the environment variable generator for Master related arguments.
type MasterEnvVarGenerator struct {
}

func GetMasterEnvVarGenerator() EnvVarGenerator {
	onceMaster.Do(func() {
		masterGenerator = &MasterEnvVarGenerator{}
	})
	return masterGenerator
}

func (e MasterEnvVarGenerator) Generate(
	job *kubeflowv1.PyTorchJob, rtype string, KubeClient kubeclientset.Interface, isInitConatiner bool) ([]corev1.EnvVar, error) {
	envVars := []corev1.EnvVar{}
	if job.Spec.PyTorchReplicaSpecs[kubeflowv1.PyTorchJobReplicaTypeMaster] != nil {
		masterPort, err := getPortFromPyTorchJob(job, kubeflowv1.PyTorchJobReplicaTypeMaster)
		if err != nil {
			return nil, err
		}

		masterAddr := replicaName(job.Name, kubeflowv1.PyTorchJobReplicaTypeMaster, 0)
		if rtype == strings.ToLower(string(kubeflowv1.PyTorchJobReplicaTypeMaster)) {
			masterAddr = "localhost"
		} else if rtype == strings.ToLower(string(kubeflowv1.PyTorchJobReplicaTypeWorker)) && isInitConatiner {
			masterAddrForWorker, err := e.getMasterHostIpForWorkerPod(job, KubeClient)
			if err != nil {
				return nil, err
			}
			masterAddr = masterAddrForWorker
		}

		envVars = append(envVars, corev1.EnvVar{
			Name:  EnvMasterPort,
			Value: strconv.Itoa(int(masterPort)),
		})
		envVars = append(envVars, corev1.EnvVar{
			Name:  PETMasterPort,
			Value: strconv.Itoa(int(masterPort)),
		})
		envVars = append(envVars, corev1.EnvVar{
			Name:  EnvMasterAddr,
			Value: masterAddr,
		})
		envVars = append(envVars, corev1.EnvVar{
			Name:  PETMasterAddr,
			Value: masterAddr,
		})
	}
	return envVars, nil
}

func (e MasterEnvVarGenerator) getMasterHostIpForWorkerPod(job *kubeflowv1.PyTorchJob, KubeClient kubeclientset.Interface) (string, error) {
	rt := strings.ToLower(string(kubeflowv1.PyTorchJobReplicaTypeWorker))
	logger := commonutil.LoggerForReplica(job, rt)

	// labelSelector := "training.kubeflow.org/job-name=wangxj-simple1,training.kubeflow.org/job-role=master"
	labelSelector := fmt.Sprintf("training.kubeflow.org/job-name=%v,training.kubeflow.org/job-role=master", job.Name)
	pods, err := KubeClient.CoreV1().Pods("").List(context.TODO(), metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		logger.Errorf("failed get pod list for job[%v/%v]", job.Name, job.Namespace)
		return "", err
	}
	if len(pods.Items) == 0 {
		err = errors.New("couldn't find pod, wait a minute")
		logger.Errorf("err: %d, job[%v/%v]", err, job.Name, job.Namespace)
		return "", err
	}
	if pods.Items[0].Status.PodIP == "" {
		err = errors.New("wait scheduling master pod, wait a minute")
		logger.Warnf("err: %d, job[%v/%v]", err, job.Name, job.Namespace)
		return "", err
	}

	return pods.Items[0].Status.PodIP, nil
}
