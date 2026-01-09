# Fixing Pip Installation Issues

## Problem
You're experiencing:
1. Missing `email-validator` module (required by Pydantic for `EmailStr`)
2. Pip crash due to corrupted package metadata (`jupyterlab-widgets` has invalid version)

## Solution

### Step 1: Clean up corrupted package metadata

```bash
# Remove corrupted jupyterlab-widgets metadata
rm -rf .venv/lib/python3.12/site-packages/jupyterlab_widgets-*.dist-info

# Or if that doesn't work, reinstall it:
pip uninstall -y jupyterlab-widgets
pip install jupyterlab-widgets
```

### Step 2: Install email-validator

```bash
# Install the missing dependency
pip install email-validator==2.2.0

# Or install all API requirements (which now includes email-validator)
pip install -r requirements-api.txt
```

### Step 3: Verify installation

```bash
# Test that email-validator is installed
python -c "import email_validator; print('OK')"

# Run API tests
pytest tests/test_api_mapping.py tests/test_subscriptions_validation.py -v
```

## Alternative: Clean venv (if issues persist)

If the above doesn't work, you may need to recreate the virtual environment:

```bash
# Backup any important data first, then:
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -r requirements-api.txt
```

