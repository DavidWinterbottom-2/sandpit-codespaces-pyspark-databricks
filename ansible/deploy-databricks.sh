#!/bin/bash

# Complete Databricks Setup Script using Ansible
# This script orchestrates the full deployment process

set -e  # Exit on any error

echo "🚀 Azure Databricks Setup with Ansible"
echo "======================================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

log_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

log_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

log_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if Ansible is installed
    if ! command -v ansible &> /dev/null; then
        log_error "Ansible is not installed. Please install it first:"
        echo "  pip install ansible"
        exit 1
    fi
    
    # Check if Azure CLI is installed
    if ! command -v az &> /dev/null; then
        log_warning "Azure CLI is not installed. It's recommended for authentication:"
        echo "  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash"
    fi
    
    log_success "Prerequisites check passed"
}

# Install Ansible requirements
install_requirements() {
    log_info "Installing Ansible requirements..."
    
    if [ -f "ansible/requirements.yml" ]; then
        ansible-galaxy install -r ansible/requirements.yml
        log_success "Ansible requirements installed"
    else
        log_warning "requirements.yml not found, skipping"
    fi
}

# Authenticate with Azure
authenticate_azure() {
    log_info "Checking Azure authentication..."
    
    if az account show &> /dev/null; then
        SUBSCRIPTION=$(az account show --query name -o tsv)
        log_success "Already authenticated with Azure (Subscription: $SUBSCRIPTION)"
    else
        log_info "Please authenticate with Azure..."
        az login
        log_success "Azure authentication successful"
    fi
}

# Deploy Databricks workspace
deploy_workspace() {
    log_info "Deploying Databricks workspace..."
    
    cd ansible
    if ansible-playbook create-databricks-workspace.yml; then
        log_success "Databricks workspace deployed successfully"
    else
        log_error "Failed to deploy Databricks workspace"
        exit 1
    fi
    cd ..
}

# Setup authentication
setup_auth() {
    log_info "Setting up Databricks authentication..."
    
    cd ansible
    ansible-playbook databricks-auth-setup.yml
    cd ..
    
    log_warning "Please complete the authentication setup:"
    echo "1. Run: ./ansible/setup-auth.sh"
    echo "2. Create your Personal Access Token"
    echo "3. Set: export DATABRICKS_ACCESS_TOKEN='your-token'"
    
    read -p "Press Enter after setting up authentication..."
}

# Deploy compute cluster
deploy_cluster() {
    log_info "Deploying Databricks compute cluster..."
    
    if [ -z "$DATABRICKS_ACCESS_TOKEN" ]; then
        log_error "DATABRICKS_ACCESS_TOKEN is not set"
        echo "Please run: export DATABRICKS_ACCESS_TOKEN='your-token'"
        exit 1
    fi
    
    cd ansible
    if ansible-playbook create-databricks-cluster.yml; then
        log_success "Databricks cluster deployed successfully"
    else
        log_error "Failed to deploy Databricks cluster"
        exit 1
    fi
    cd ..
}

# Test the setup
test_setup() {
    log_info "Testing Databricks connection..."
    
    if python3 -c "from shared.databricks_connection import test_connection; test_connection()"; then
        log_success "Databricks connection test passed"
    else
        log_warning "Connection test failed - please check your configuration"
    fi
}

# Main execution
main() {
    echo ""
    log_info "Starting complete Databricks setup..."
    echo ""
    
    # Step 1: Prerequisites
    check_prerequisites
    
    # Step 2: Install requirements
    install_requirements
    
    # Step 3: Azure authentication
    authenticate_azure
    
    # Step 4: Deploy workspace
    deploy_workspace
    
    # Step 5: Setup authentication
    setup_auth
    
    # Step 6: Deploy cluster
    deploy_cluster
    
    # Step 7: Test setup
    test_setup
    
    echo ""
    log_success "🎉 Databricks setup complete!"
    echo ""
    echo "📋 Summary:"
    echo "   - Azure Databricks workspace created"
    echo "   - Standard compute cluster deployed"
    echo "   - Authentication configured"
    echo "   - Connection tested"
    echo ""
    echo "🚀 You can now:"
    echo "   - Open your Databricks workspace"
    echo "   - Run PySpark examples remotely"
    echo "   - Use databricks-connect from local environment"
    echo ""
    echo "📁 Configuration files:"
    echo "   - databricks-config.env"
    echo "   - databricks-cluster.env"
    echo "   - shared/databricks_config.py"
    echo "   - shared/databricks_connection.py"
}

# Handle command line arguments
case "${1:-all}" in
    "prereq")
        check_prerequisites
        ;;
    "workspace")
        check_prerequisites
        install_requirements
        authenticate_azure
        deploy_workspace
        ;;
    "auth")
        setup_auth
        ;;
    "cluster")
        deploy_cluster
        ;;
    "test")
        test_setup
        ;;
    "all")
        main
        ;;
    *)
        echo "Usage: $0 {all|prereq|workspace|auth|cluster|test}"
        echo ""
        echo "Commands:"
        echo "  all        - Run complete setup (default)"
        echo "  prereq     - Check prerequisites only"
        echo "  workspace  - Deploy workspace only"
        echo "  auth       - Setup authentication only"
        echo "  cluster    - Deploy cluster only"
        echo "  test       - Test connection only"
        exit 1
        ;;
esac