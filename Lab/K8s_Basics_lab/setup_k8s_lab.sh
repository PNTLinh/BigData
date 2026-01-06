#!/bin/bash

# Setup script for Kubernetes Basics Lab
# This script sets up the K8s lab environment

set -e

echo "🚀 Setting up Kubernetes Basics Lab Environment..."
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check if kubectl is installed
if ! command -v kubectl &> /dev/null; then
    echo -e "${YELLOW}⚠️  kubectl is not installed${NC}"
    echo "Please install kubectl:"
    echo "  macOS: brew install kubectl"
    echo "  Linux: https://kubernetes.io/docs/tasks/tools/install-kubectl-linux/"
    exit 1
else
    echo "✅ kubectl is installed"
    kubectl version --client --short
fi

# Check if minikube or kind is installed
K8S_TOOL=""
if command -v minikube &> /dev/null; then
    K8S_TOOL="minikube"
    echo "✅ minikube is installed"
elif command -v kind &> /dev/null; then
    K8S_TOOL="kind"
    echo "✅ kind is installed"
else
    echo -e "${YELLOW}⚠️  Neither minikube nor kind is installed${NC}"
    echo "Please install one of them:"
    echo "  minikube: brew install minikube"
    echo "  kind: brew install kind"
    exit 1
fi

# Check if conda is installed
if ! command -v conda &> /dev/null; then
    echo -e "${YELLOW}⚠️  Conda is not installed${NC}"
    echo "Please install Miniconda or Anaconda"
    exit 1
fi

# Create conda environment
ENV_NAME="k8s_lab"
echo ""
echo "📦 Creating conda environment '${ENV_NAME}'..."
if conda env list | grep -q "^${ENV_NAME} "; then
    echo "✅ Conda environment '${ENV_NAME}' already exists"
else
    conda create -n ${ENV_NAME} python=3.10 -y
    echo "✅ Conda environment created"
fi

# Activate environment
echo "🔧 Activating conda environment..."
eval "$(conda shell.bash hook)"
conda activate ${ENV_NAME}

# Install dependencies
echo "📥 Installing Python dependencies..."
pip install -r requirements.txt

# Setup Kubernetes cluster
echo ""
echo "🐳 Setting up Kubernetes cluster..."

if [ "$K8S_TOOL" == "minikube" ]; then
    echo "Using minikube..."
    
    # Check if minikube is running
    if minikube status &> /dev/null; then
        echo "✅ Minikube cluster is already running"
    else
        echo "Starting minikube cluster..."
        minikube start --driver=docker --memory=4096 --cpus=2
        echo "✅ Minikube cluster started"
    fi
    
    # Configure kubectl
    kubectl config use-context minikube
    
elif [ "$K8S_TOOL" == "kind" ]; then
    echo "Using kind..."
    
    # Check if cluster exists
    if kind get clusters | grep -q "k8s-lab"; then
        echo "✅ Kind cluster 'k8s-lab' already exists"
    else
        echo "Creating kind cluster..."
        kind create cluster --name k8s-lab
        echo "✅ Kind cluster created"
    fi
    
    # Configure kubectl
    kubectl config use-context kind-k8s-lab
fi

# Verify cluster
echo ""
echo "🔍 Verifying cluster..."
kubectl cluster-info
kubectl get nodes

echo ""
echo -e "${GREEN}✅ Kubernetes Basics Lab setup completed!${NC}"
echo ""
echo "📚 Next steps:"
echo "  1. Activate conda environment: conda activate ${ENV_NAME}"
echo "  2. Verify cluster: kubectl get nodes"
echo "  3. Open Jupyter: jupyter notebook"
echo "  4. Navigate to notebooks/ directory"
echo ""
echo "🔗 Useful commands:"
echo "  - kubectl get pods: List pods"
echo "  - kubectl get nodes: List nodes"
echo "  - kubectl cluster-info: Cluster information"
echo "  - kubectl config view: View configuration"
echo ""
if [ "$K8S_TOOL" == "minikube" ]; then
    echo "💡 Minikube commands:"
    echo "  - minikube status: Check status"
    echo "  - minikube stop: Stop cluster"
    echo "  - minikube delete: Delete cluster"
fi
echo ""

