# SSH Keys Setup for DevContainer

This guide explains how to set up SSH keys in your PySpark DevContainer environment to enable Git operations with SSH authentication.

## 🔑 SSH Key Configuration

The devcontainer is configured to automatically mount your SSH keys from your local machine's `~/.ssh` directory.

### Prerequisites

1. **SSH keys must exist on your local machine** in `~/.ssh/`
2. **Common key files:**
   - `id_rsa` / `id_rsa.pub` (RSA keys)
   - `id_ed25519` / `id_ed25519.pub` (Ed25519 keys - recommended)

## 🚀 Quick Setup

### Option 1: Automatic Setup (Recommended)

1. **Ensure SSH keys exist locally:**
   ```bash
   # On your local machine
   ls -la ~/.ssh/
   ```

2. **Rebuild the devcontainer:**
   - In VS Code: `Ctrl+Shift+P` → "Dev Containers: Rebuild Container"
   - Or restart the Codespace

3. **Verify setup:**
   ```bash
   # Inside the devcontainer
   .devcontainer/setup-ssh.sh
   ```

### Option 2: Manual Setup

If you need to set up SSH keys manually inside the container:

```bash
# Check if keys are mounted
ls -la ~/.ssh/

# Set proper permissions
chmod 700 ~/.ssh
chmod 600 ~/.ssh/id_*
chmod 644 ~/.ssh/*.pub

# Start SSH agent and add keys
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa  # or id_ed25519

# Test GitHub connection
ssh -T git@github.com
```

## 🔧 Git Configuration

Set up your Git identity:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

## 🧪 Testing SSH Setup

### Test GitHub Connection
```bash
ssh -T git@github.com
```

Expected output:
```
Hi username! You've successfully authenticated, but GitHub does not provide shell access.
```

### Test Git Operations
```bash
# Clone a repository
git clone git@github.com:username/repository.git

# Check current repository status
git status

# Test pushing changes
git add .
git commit -m "test commit"
git push origin main
```

## 🛠️ Troubleshooting

### SSH Keys Not Found
- **Issue:** No SSH keys in `~/.ssh`
- **Solution:** Create SSH keys on your local machine first
  ```bash
  # On local machine
  ssh-keygen -t ed25519 -C "your.email@example.com"
  ```

### Permission Denied
- **Issue:** `Permission denied (publickey)`
- **Solutions:**
  1. Run the setup script: `.devcontainer/setup-ssh.sh`
  2. Check if keys are loaded: `ssh-add -l`
  3. Add keys manually: `ssh-add ~/.ssh/id_rsa`

### SSH Agent Not Running
- **Issue:** SSH agent not available
- **Solution:**
  ```bash
  eval "$(ssh-agent -s)"
  ssh-add ~/.ssh/id_rsa
  ```

### Git Authentication Failing
- **Issue:** Git operations fail with authentication error
- **Solutions:**
  1. Ensure SSH keys are added to your GitHub/GitLab account
  2. Use SSH URLs: `git@github.com:user/repo.git` instead of HTTPS
  3. Test SSH connection: `ssh -T git@github.com`

## 📁 File Structure

After setup, your devcontainer should have:

```
/home/vscode/.ssh/
├── id_rsa          # Private key (600 permissions)
├── id_rsa.pub      # Public key (644 permissions)
├── known_hosts     # Known SSH hosts
└── config          # SSH config (optional)
```

## 🔐 Security Notes

1. **Private keys are never committed to the repository**
2. **SSH keys are mounted read-only from your local machine**
3. **Keys are only accessible within your devcontainer**
4. **Use strong passphrases for your SSH keys**

## 🆘 Getting Help

If you encounter issues:

1. Run the diagnostic script: `.devcontainer/setup-ssh.sh`
2. Check the post-create script logs
3. Verify your local SSH keys are properly configured
4. Ensure your public key is added to your Git hosting service (GitHub/GitLab)

## 🔗 Useful Commands

```bash
# List loaded SSH keys
ssh-add -l

# Add specific key
ssh-add ~/.ssh/id_ed25519

# Remove all keys from agent
ssh-add -D

# Test specific host
ssh -T git@github.com

# Debug SSH connection
ssh -vT git@github.com

# Check Git config
git config --list --global
```