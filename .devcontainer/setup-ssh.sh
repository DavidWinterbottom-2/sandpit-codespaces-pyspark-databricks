#!/bin/bash

# SSH Setup Helper Script for DevContainer
# This script helps configure SSH keys and Git settings in the devcontainer

echo "🔑 SSH Key Setup Helper"
echo "======================="

# Check if SSH directory exists
if [ ! -d "$HOME/.ssh" ]; then
    echo "❌ No .ssh directory found"
    echo "   Please ensure your SSH keys are in ~/.ssh on your local machine"
    echo "   and restart the devcontainer"
    exit 1
fi

# List available SSH keys
echo "📋 Available SSH keys:"
ls -la ~/.ssh/*.pub 2>/dev/null || echo "   No public keys found"

# Set proper permissions
echo "🔧 Setting SSH key permissions..."
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_* 2>/dev/null || true
chmod 644 ~/.ssh/*.pub 2>/dev/null || true
chmod 644 ~/.ssh/known_hosts 2>/dev/null || true
chmod 600 ~/.ssh/config 2>/dev/null || true

# Start SSH agent if not running
if [ -z "$SSH_AUTH_SOCK" ]; then
    echo "🚀 Starting SSH agent..."
    eval "$(ssh-agent -s)"
fi

# Add SSH keys
echo "➕ Adding SSH keys to agent..."
if [ -f ~/.ssh/id_rsa ]; then
    ssh-add ~/.ssh/id_rsa
elif [ -f ~/.ssh/id_ed25519 ]; then
    ssh-add ~/.ssh/id_ed25519
else
    echo "⚠️  No standard SSH keys found (id_rsa or id_ed25519)"
fi

# List loaded keys
echo "📜 Currently loaded SSH keys:"
ssh-add -l 2>/dev/null || echo "   No keys loaded"

# Test GitHub connection
echo ""
echo "🧪 Testing GitHub SSH connection..."
ssh -T git@github.com 2>&1 | head -2

# Git configuration helper
echo ""
echo "🔧 Git Configuration:"
if [ -z "$(git config --global user.name)" ]; then
    echo "⚠️  Git user.name not set"
    echo "   Run: git config --global user.name 'Your Name'"
else
    echo "✅ Git user.name: $(git config --global user.name)"
fi

if [ -z "$(git config --global user.email)" ]; then
    echo "⚠️  Git user.email not set"
    echo "   Run: git config --global user.email 'your.email@example.com'"
else
    echo "✅ Git user.email: $(git config --global user.email)"
fi

echo ""
echo "🎉 SSH setup complete!"
echo ""
echo "💡 Useful commands:"
echo "   ssh-add -l          # List loaded SSH keys"
echo "   ssh-add ~/.ssh/key  # Add specific key"
echo "   ssh -T git@github.com  # Test GitHub connection"