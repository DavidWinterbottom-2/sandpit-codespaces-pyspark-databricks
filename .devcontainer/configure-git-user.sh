#!/bin/bash

# Quick Git user configuration script
# Run this to set your Git username and email

echo "🔧 Git User Configuration Setup"
echo "==============================="

# Check current configuration
current_name=$(git config --global user.name 2>/dev/null)
current_email=$(git config --global user.email 2>/dev/null)

if [ -n "$current_name" ] && [ -n "$current_email" ]; then
    echo "Current configuration:"
    echo "  Name:  $current_name"
    echo "  Email: $current_email"
    echo ""
    read -p "Do you want to change this configuration? (y/N): " change_config
    if [[ ! $change_config =~ ^[Yy]$ ]]; then
        echo "Keeping current configuration."
        exit 0
    fi
fi

# Get user input
echo ""
read -p "Enter your full name: " user_name
read -p "Enter your email address: " user_email

# Validate input
if [ -z "$user_name" ] || [ -z "$user_email" ]; then
    echo "❌ Both name and email are required."
    exit 1
fi

# Set the configuration
git config --global user.name "$user_name"
git config --global user.email "$user_email"

echo ""
echo "✅ Git configuration updated!"
echo "  Name:  $user_name"
echo "  Email: $user_email"
echo ""
echo "💡 Tip: To avoid setting this every time, add these to your host environment:"
echo "  export GIT_AUTHOR_NAME='$user_name'"
echo "  export GIT_AUTHOR_EMAIL='$user_email'"