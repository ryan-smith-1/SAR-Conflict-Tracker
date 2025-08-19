
import json
import os
from pathlib import Path

# ============================================================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================================================

# 1. SENTINEL HUB CREDENTIALS - GET FROM https://apps.sentinel-hub.com/dashboard/
SENTINEL_HUB_CLIENT_ID = "sh-09712a92-008f-4111-bf81-ab403318402b"  # Replace with your Client ID
SENTINEL_HUB_CLIENT_SECRET = "T5oljVaT68YS12JONV3Egl8OAGkNVYOm"  # Replace with your Client Secret
# Your Account/User ID (if available)
ACCOUNT_ID = "3cb81221-f04d-4003-b7db-88630d6222bc"  # Optional: Your account ID from the dashboard
SENTINEL_HUB_INSTANCE_ID = ''
# 2. AREA OF INTEREST - Define your monitoring area (650-1300 km²)
# Format: [[longitude, latitude], [longitude, latitude], ...]
# Example below is ~50km x 50km around NYC - REPLACE WITH YOUR COORDINATES
#
# HOW TO GET COORDINATES:
# Method 1: Use your Sentinel Hub Configuration Utility:
#   - Go to https://apps.sentinel-hub.com/dashboard/
#   - Open "test 21" configuration 
#   - Use map bounds tool to draw area
#   - Copy coordinates
#
# Method 2: Use online tools like https://geojson.io/
#   - Draw your area
#   - Copy coordinates from the GeoJSON
#
# Method 3: Bounding box (rectangle):
#   For 50km x 50km area around a center point (lon, lat):
#   [[lon-0.25, lat-0.25], [lon-0.25, lat+0.25], [lon+0.25, lat+0.25], [lon+0.25, lat-0.25], [lon-0.25, lat-0.25]]
#
AREA_COORDINATES = [
            [
              34.271328748121135,
              31.367306403175277
            ],
            [
              34.271328748121135,
              31.308940787645426
            ],
            [
              34.36415015532711,
              31.308940787645426
            ],
            [
              34.36415015532711,
              31.367306403175277
            ],
            [
              34.271328748121135,
              31.367306403175277
            ]
]
AREA_NAME = "target_monitoring_area"  # Name for your area

# 3. DATA DIRECTORIES
DATA_DIRECTORY = "./sar_data"
ASF_DOWNLOAD_DIRECTORY = "./sar_data/asf"

# 4. TEMPORAL SETTINGS
DAYS_BACK_DEFAULT = 7  # Default days to search back
MAX_CLOUD_COVER = 20   # Maximum cloud cover percentage

# 5. ASF SETTINGS
ASF_MAX_RESULTS = 100  # Maximum search results from ASF

# 6. PROCESSING SETTINGS
PROCESSING_RESOLUTION = 10  # Resolution in meters
BBOX_SIZE_KM = 40          # Bounding box size in km

# ============================================================================
# END CONFIGURATION - DO NOT EDIT BELOW THIS LINE
# ============================================================================

def create_configuration():
    """Create configuration from hardcoded values"""
    print("=== SAR Data Pipeline Setup ===\n")
    
    # Validate that credentials were updated
    if SENTINEL_HUB_CLIENT_ID == "YOUR_CLIENT_ID_HERE":
        print("❌ ERROR: Please update SENTINEL_HUB_CLIENT_ID in the script")
        print("   Get your credentials from: https://dataspace.copernicus.eu/")
        print("   In your dashboard, create OAuth client credentials")
        return False
        
    if SENTINEL_HUB_CLIENT_SECRET == "YOUR_CLIENT_SECRET_HERE":
        print("❌ ERROR: Please update SENTINEL_HUB_CLIENT_SECRET in the script")
        print("   Get your credentials from: https://dataspace.copernicus.eu/")
        print("   In your dashboard, create OAuth client credentials")
        return False
        
    # Instance ID is optional for CDSE
    if not SENTINEL_HUB_INSTANCE_ID:
        print("ℹ️  Using Copernicus Data Space Ecosystem (no Instance ID required)")
    else:
        print(f"ℹ️  Using Instance ID: {SENTINEL_HUB_INSTANCE_ID}")
    
    # Build configuration
    config = {
        "data_directory": DATA_DIRECTORY,
        "area_of_interest": {
            "name": AREA_NAME,
            "coordinates": AREA_COORDINATES
        },
        "temporal_range": {
            "days_back": DAYS_BACK_DEFAULT,
            "max_cloud_cover": MAX_CLOUD_COVER
        },
        "sentinel_hub": {
            "instance_id": SENTINEL_HUB_INSTANCE_ID,  # May be empty for CDSE
            "client_id": SENTINEL_HUB_CLIENT_ID,
            "client_secret": SENTINEL_HUB_CLIENT_SECRET,
            "sh_base_url": "https://sh.dataspace.copernicus.eu",  # CDSE endpoint
            "sh_token_url": "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        },
        "asf": {
            "download_directory": ASF_DOWNLOAD_DIRECTORY,
            "max_results": ASF_MAX_RESULTS
        },
        "processing": {
            "resolution": PROCESSING_RESOLUTION,
            "bbox_size_km": BBOX_SIZE_KM
        }
    }
    
    # Save configuration
    config_file = "pipeline_config.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Configuration saved to {config_file}")
    
    # Create data directories
    Path(DATA_DIRECTORY).mkdir(parents=True, exist_ok=True)
    Path(ASF_DOWNLOAD_DIRECTORY).mkdir(parents=True, exist_ok=True)
    Path(f"{DATA_DIRECTORY}/sentinel_hub").mkdir(parents=True, exist_ok=True)
    
    print("✅ Data directories created")
    
    # Create .env file for credentials
    env_content = f"""# SAR Pipeline Environment Variables
SH_CLIENT_ID={SENTINEL_HUB_CLIENT_ID}
SH_CLIENT_SECRET={SENTINEL_HUB_CLIENT_SECRET}
SH_INSTANCE_ID={SENTINEL_HUB_INSTANCE_ID}

# ASF Credentials (optional - for full downloads)
# ASF_USERNAME=your_asf_username
# ASF_PASSWORD=your_asf_password
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print("✅ Environment file created (.env)")
    
    # Print area info
    print(f"\n📍 Configured area: {AREA_NAME}")
    print(f"   Coordinates: {len(AREA_COORDINATES)-1} points")
    print(f"   Approximate size: {BBOX_SIZE_KM}km x {BBOX_SIZE_KM}km")
    
    print("\n=== Setup Complete! ===")
    print("\nNext steps:")
    print("1. Validate: python setup_pipeline.py validate")
    print("2. Test: python setup_pipeline.py test") 
    print("3. Run pipeline: python sar_pipeline.py --days-back 7")
    print("4. Schedule: python sar_pipeline.py --schedule")
    
    return True

# validation.py - Configuration validation script
def validate_config(config_file="pipeline_config.json"):
    """Validate pipeline configuration"""
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"❌ Config file {config_file} not found")
        return False
    except json.JSONDecodeError:
        print(f"❌ Invalid JSON in {config_file}")
        return False
    
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = [
        'data_directory',
        'area_of_interest',
        'temporal_range',
        'sentinel_hub',
        'asf',
        'processing'
    ]
    
    for field in required_fields:
        if field not in config:
            errors.append(f"Missing required field: {field}")
    
    # Validate area of interest
    if 'area_of_interest' in config:
        aoi = config['area_of_interest']
        if 'coordinates' not in aoi:
            errors.append("Missing coordinates in area_of_interest")
        elif len(aoi['coordinates']) < 3:
            errors.append("Area of interest needs at least 3 coordinate pairs")
    
    # Validate Sentinel Hub config
    if 'sentinel_hub' in config:
        sh = config['sentinel_hub']
        if not sh.get('client_id'):
            warnings.append("Sentinel Hub client_id not configured")
        if not sh.get('client_secret'):
            warnings.append("Sentinel Hub client_secret not configured")
        
        # Instance ID is optional for CDSE (Copernicus Data Space Ecosystem)
        if not sh.get('instance_id'):
            warnings.append("No Instance ID provided - using Copernicus Data Space Ecosystem mode")
        else:
            print(f"ℹ️  Instance ID found: {sh.get('instance_id')}")
    else:
        errors.append("Missing sentinel_hub configuration section")
    # Validate directories exist
    data_dir = Path(config.get('data_directory', './sar_data'))
    if not data_dir.exists():
        warnings.append(f"Data directory does not exist: {data_dir}")
    
    # Print results
    if errors:
        print("❌ Configuration Errors:")
        for error in errors:
            print(f"  - {error}")
    
    if warnings:
        print("⚠️  Configuration Warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("✅ Configuration is valid!")
        return True
    elif not errors:
        print("✅ Configuration is valid (with warnings)")
        return True
    else:
        return False

# test_connection.py - Test API connections
async def test_connections(config_file="pipeline_config.json"):
    """Test API connections"""
    import asyncio
    import sys
    import os
    
    # Add current directory to path so we can import sar_pipeline
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    
    try:
        # Try different import methods
        try:
            import sar_pipeline
            from sar_pipeline import SARDataPipeline
        except ImportError:
            # If that fails, try importing as module
            import importlib.util
            spec = importlib.util.spec_from_file_location("sar_pipeline", "./sar_pipeline.py")
            sar_pipeline_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(sar_pipeline_module)
            SARDataPipeline = sar_pipeline_module.SARDataPipeline
    except ImportError as e:
        print(f"❌ Could not import SARDataPipeline: {e}")
        print("   Make sure sar_pipeline.py is in the same directory")
        print("   Current directory contents:")
        for f in os.listdir('.'):
            if f.endswith('.py'):
                print(f"     {f}")
        return False
    
    print("=== Testing API Connections ===\n")
    
    try:
        pipeline = SARDataPipeline(config_file)
        print("✅ Pipeline initialized successfully")
    except Exception as e:
        print(f"❌ Failed to initialize pipeline: {e}")
        return False
    
    # Test ASF connection
    print("\n1. Testing ASF connection...")
    try:
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        results = await pipeline.search_asf_data(start_date, end_date)
        if results:
            print(f"✅ ASF connection successful - Found {len(results)} results")
        else:
            print("⚠️  ASF connection successful but no results found")
    except Exception as e:
        print(f"❌ ASF connection failed: {e}")
    
    # Test Sentinel Hub connection
    print("\n2. Testing Sentinel Hub connection...")
    try:
        if pipeline.config['sentinel_hub']['client_id']:
            # Test if we can create the SH config without errors
            sh_config = pipeline._setup_sentinel_hub()
            
            if pipeline.config['sentinel_hub']['instance_id']:
                print(f"✅ Sentinel Hub configured with Instance ID: {pipeline.config['sentinel_hub']['instance_id']}")
            else:
                print("✅ Sentinel Hub configured for Copernicus Data Space Ecosystem (no Instance ID)")
            
            # Try a basic search
            results = await pipeline.search_sentinel_hub_data(start_date, end_date)
            if results:
                print(f"✅ Sentinel Hub search successful - Found {len(results)} potential datasets")
            else:
                print("⚠️  Sentinel Hub search completed but no datasets found")
        else:
            print("⚠️  Sentinel Hub not configured - skipping test")
    except Exception as e:
        print(f"❌ Sentinel Hub connection test failed: {e}")
        print("   This might be normal if credentials need verification")
    
    print("\n=== Connection Tests Complete ===")

if __name__ == "__main__":
    import sys
    import asyncio
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "setup":
            create_configuration()
        elif command == "validate":
            config_file = sys.argv[2] if len(sys.argv) > 2 else "pipeline_config.json"
            validate_config(config_file)
        elif command == "test":
            config_file = sys.argv[2] if len(sys.argv) > 2 else "pipeline_config.json"
            asyncio.run(test_connections(config_file))
        else:
            print("Usage: python setup_pipeline.py [setup|validate|test] [config_file]")
            print()
            print("Commands:")
            print("  setup    - Create configuration from hardcoded values")
            print("  validate - Validate existing configuration")
            print("  test     - Test API connections")
    else:
        # Default action: create configuration
        create_configuration()
