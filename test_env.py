#!/usr/bin/env python3
"""
Test environment variable loading
"""

import os
from pathlib import Path

# Try to load .env file manually
env_path = Path('.env')
print(f"Looking for .env file at: {env_path.absolute()}")
print(f".env file exists: {env_path.exists()}")

if env_path.exists():
    print(f"\n.env file contents:")
    with open(env_path) as f:
        for i, line in enumerate(f, 1):
            if line.strip() and not line.strip().startswith('#'):
                if '=' in line:
                    key, value = line.strip().split('=', 1)
                    # Hide sensitive values
                    if 'TOKEN' in key or 'SECRET' in key or 'PASSWORD' in key:
                        display_value = value[:10] + "..." if len(value) > 10 else value
                    else:
                        display_value = value
                    print(f"  Line {i}: {key}={display_value}")
                    
                    # Set in environment
                    os.environ[key] = value

# Test environment variables
print(f"\nEnvironment variables:")
print(f"EDL_TOKEN: {'✅ Set' if os.getenv('EDL_TOKEN') else '❌ Not found'}")
print(f"ASF_USERNAME: {'✅ Set' if os.getenv('ASF_USERNAME') else '❌ Not found'}")
print(f"ASF_PASSWORD: {'✅ Set' if os.getenv('ASF_PASSWORD') else '❌ Not found'}")

# Show first 20 characters of token if available
token = os.getenv('EDL_TOKEN')
if token:
    print(f"EDL_TOKEN starts with: {token[:20]}...")
    print(f"EDL_TOKEN length: {len(token)} characters")

# Check if token looks valid (JWT format)
if token and token.count('.') == 2:
    print("✅ Token appears to be valid JWT format")
else:
    print("❌ Token does not appear to be valid JWT format")