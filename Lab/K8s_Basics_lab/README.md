# Kubernetes Basics Lab - Container Orchestration Fundamentals

## 🎯 Overview

Lab này cung cấp kiến thức cơ bản về **Kubernetes (K8s)** - container orchestration platform hàng đầu. Sinh viên sẽ học cách deploy, quản lý và scale applications trên Kubernetes từ cơ bản đến nâng cao.

## 📋 Prerequisites

- ✅ Hoàn thành các labs cơ bản (Kafka, Spark, Airflow)
- ✅ Hiểu Docker và Docker Compose
- ✅ Basic Linux/CLI knowledge
- ✅ Python 3.10+
- ✅ 8GB+ RAM (cho local K8s)

## 🎯 Learning Objectives

Sau khi hoàn thành lab này, bạn sẽ có thể:

- ✅ Hiểu Kubernetes architecture và core concepts
- ✅ Tạo và quản lý Pods, Deployments, Services
- ✅ Sử dụng ConfigMaps và Secrets
- ✅ Quản lý persistent storage
- ✅ Scale applications với HPA
- ✅ Debug và troubleshoot K8s applications
- ✅ Apply K8s knowledge vào production scenarios

## 📚 Lab Structure

### Lab 1: Kubernetes Fundamentals
- K8s architecture (Control Plane, Nodes, Pods)
- Local K8s setup (minikube hoặc kind)
- kubectl basics
- Namespaces

### Lab 2: Pods và Containers
- Pods là gì và cách hoạt động
- Multi-container pods
- Pod lifecycle
- Pod logs và debugging

### Lab 3: Deployments và ReplicaSets
- Deployments và ReplicaSets
- Rolling updates và rollbacks
- Scaling applications
- Deployment strategies

### Lab 4: Services và Networking
- Services (ClusterIP, NodePort, LoadBalancer)
- Service discovery
- Ingress (basic)
- Networking trong K8s

### Lab 5: ConfigMaps và Secrets
- ConfigMaps cho configuration
- Secrets cho sensitive data
- Environment variables
- Volume mounts

### Lab 6: Persistent Storage
- Volumes
- PersistentVolumes và PersistentVolumeClaims
- Storage classes
- Data persistence trong pods

### Lab 7: Resource Management và Scaling
- Resource requests và limits
- Horizontal Pod Autoscaler (HPA)
- Resource quotas
- Best practices

## 🚀 Quick Start

### 1. Setup Local Kubernetes

**Option A: Minikube (Recommended)**
```bash
# Install minikube
brew install minikube  # macOS
# hoặc download từ https://minikube.sigs.k8s.io/docs/start/

# Start minikube
minikube start --driver=docker --memory=4096 --cpus=2

# Verify
kubectl get nodes
```

**Option B: Kind (Kubernetes in Docker)**
```bash
# Install kind
brew install kind  # macOS
# hoặc download từ https://kind.sigs.k8s.io/docs/user/quick-start/

# Create cluster
kind create cluster --name k8s-lab

# Verify
kubectl get nodes
```

### 2. Install kubectl

```bash
# macOS
brew install kubectl

# Verify
kubectl version --client
```

### 3. Setup Python Environment

```bash
# Create conda environment
conda create -n k8s_lab python=3.10 -y
conda activate k8s_lab

# Install dependencies
pip install -r requirements.txt
```

### 4. Verify Setup

```bash
# Check K8s cluster
kubectl cluster-info

# Check nodes
kubectl get nodes

# Check kubectl config
kubectl config view
```

## 🏗️ Lab Architecture

```
Local Machine
├── Minikube/Kind Cluster
│   ├── Control Plane
│   │   ├── API Server
│   │   ├── etcd
│   │   ├── Scheduler
│   │   └── Controller Manager
│   └── Worker Nodes
│       ├── kubelet
│       ├── kube-proxy
│       └── Container Runtime
└── kubectl (CLI tool)
```

## 📖 Key Concepts

### Core Objects
- **Pods**: Smallest deployable unit
- **Deployments**: Manage Pod replicas
- **Services**: Expose Pods to network
- **ConfigMaps**: Configuration data
- **Secrets**: Sensitive data
- **Volumes**: Persistent storage

### Commands Reference
```bash
# Get resources
kubectl get pods
kubectl get deployments
kubectl get services

# Describe resources
kubectl describe pod <pod-name>

# Create from YAML
kubectl apply -f manifest.yaml

# Delete resources
kubectl delete pod <pod-name>
kubectl delete -f manifest.yaml

# Logs
kubectl logs <pod-name>

# Execute command in pod
kubectl exec -it <pod-name> -- /bin/bash
```

## 🐛 Troubleshooting

### Common Issues

1. **Minikube không start:**
```bash
minikube delete
minikube start --driver=docker --memory=4096
```

2. **kubectl connection refused:**
```bash
# Check minikube status
minikube status

# Get cluster info
kubectl cluster-info
```

3. **Pods không start:**
```bash
# Check pod status
kubectl describe pod <pod-name>

# Check events
kubectl get events --sort-by='.lastTimestamp'
```

4. **Not enough resources:**
```bash
# Increase minikube resources
minikube stop
minikube start --memory=4096 --cpus=2
```

## 📝 Notes

- Lab này focus vào **K8s basics**, không cover advanced topics như operators
- Deploy Spark/Kafka lên K8s là advanced topic, có thể tự học sau
- Local K8s (minikube/kind) đủ cho learning purposes
- Production K8s có thêm components (Ingress controllers, monitoring, etc.)

## 🎓 Next Steps

Sau khi hoàn thành lab này, bạn có thể:
- Tự học advanced topics (operators, CRDs)
- Deploy applications lên production K8s
- Learn về Helm charts
- Explore K8s monitoring và logging

## 🔗 Useful Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [kubectl Cheat Sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)
- [Minikube Documentation](https://minikube.sigs.k8s.io/docs/)
- [Kind Documentation](https://kind.sigs.k8s.io/)

---

**Happy Learning! 🚀**

