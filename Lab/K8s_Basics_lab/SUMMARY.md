# Kubernetes Basics Lab - Summary

## 📋 Overview
Lab này cung cấp kiến thức cơ bản về Kubernetes - container orchestration platform.

## 🎯 Learning Objectives
- Hiểu K8s architecture và core concepts
- Deploy và quản lý applications trên K8s
- Sử dụng ConfigMaps, Secrets, Volumes
- Scale applications với HPA
- Resource management

## 📚 Lab Structure

### Lab 1: Kubernetes Fundamentals
- K8s architecture
- Local setup (minikube/kind)
- kubectl basics
- Namespaces

### Lab 2: Pods và Containers
- Pods là gì
- Multi-container pods
- Pod lifecycle
- Debugging

### Lab 3: Deployments và ReplicaSets
- Deployments
- Scaling
- Rolling updates
- Rollbacks

### Lab 4: Services và Networking
- Services (ClusterIP, NodePort, LoadBalancer)
- Service discovery
- Ingress basics
- Networking

### Lab 5: ConfigMaps và Secrets
- ConfigMaps cho configuration
- Secrets cho sensitive data
- Environment variables
- Volume mounts

### Lab 6: Persistent Storage
- Volumes
- PVs và PVCs
- Storage classes
- Data persistence

### Lab 7: Resource Management và Scaling
- Resource requests/limits
- HPA
- Resource quotas
- Best practices

## 🚀 Quick Start

1. **Setup:**
```bash
./setup_k8s_lab.sh
```

2. **Verify:**
```bash
kubectl get nodes
```

3. **Start learning:**
```bash
jupyter notebook
```

## 🔗 Key Resources

- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [kubectl Cheat Sheet](https://kubernetes.io/docs/reference/kubectl/cheatsheet/)
- [Minikube](https://minikube.sigs.k8s.io/)
- [Kind](https://kind.sigs.k8s.io/)

## 📝 Notes

- Focus vào **K8s basics** only
- Không cover advanced topics như operators
- Local K8s (minikube/kind) đủ cho learning
- Production K8s có thêm components

