#!/usr/bin/env python3
"""
ASF Official Library Downloader
Uses the official asf_search library with proper ASFSession authentication
"""

import os
import logging
import asyncio
import json
from datetime import datetime
from pathlib import Path
import zipfile
import shutil
from typing import Dict, List, Optional

# FORCE load environment variables from .env file
def load_env_file():
    """Force load .env file"""
    env_path = Path('.env')
    if env_path.exists():
        print(f"Loading .env from: {env_path.absolute()}")
        with open(env_path) as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
                    print(f"Set {key}={'***' if 'TOKEN' in key or 'PASSWORD' in key else value}")
    else:
        print(f"No .env file found at: {env_path.absolute()}")

# Load environment before anything else
load_env_file()

# Try python-dotenv as backup
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("Also loaded with python-dotenv")
except ImportError:
    print("python-dotenv not available, using manual loading")

# Import asf_search
try:
    import asf_search as asf
    print("✅ asf_search available - using official ASF library")
except ImportError:
    print("❌ asf_search not available - install with: pip install asf-search")
    exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ASFOfficialDownloader:
    """ASF downloader using the official asf_search library"""
    
    def __init__(self, download_dir: Path = Path("./sar_data")):
        self.download_dir = Path(download_dir)
        self.raw_dir = self.download_dir / "raw_zip"
        self.safe_dir = self.download_dir / "safe_extracted" 
        self.session = None
        
        # Create directories
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.safe_dir.mkdir(parents=True, exist_ok=True)
        
        # Get authentication credentials
        self.edl_token = os.getenv('EDL_TOKEN')
        self.username = os.getenv('ASF_USERNAME') 
        self.password = os.getenv('ASF_PASSWORD')
        
        print(f"\n🔐 Authentication check:")
        print(f"  EDL_TOKEN: {'✅ Found' if self.edl_token else '❌ Missing'}")
        print(f"  ASF_USERNAME: {'✅ Found' if self.username else '❌ Missing'}")
        print(f"  ASF_PASSWORD: {'✅ Found' if self.password else '❌ Missing'}")
        
        if self.edl_token:
            print(f"  Token length: {len(self.edl_token)} chars")
            print(f"  Token starts: {self.edl_token[:20]}...")
        
        if not (self.edl_token or (self.username and self.password)):
            print("❌ Either EDL_TOKEN or ASF_USERNAME/ASF_PASSWORD required")
            self._show_auth_instructions()
    
    def _show_auth_instructions(self):
        """Show authentication setup instructions"""
        print("\n" + "="*60)
        print("🔐 AUTHENTICATION SETUP REQUIRED")
        print("="*60)
        print("Add to .env file:")
        print("   EDL_TOKEN=your_earthdata_token")
        print("OR")
        print("   ASF_USERNAME=your_earthdata_username")
        print("   ASF_PASSWORD=your_earthdata_password")
        print("\nRegister at: https://urs.earthdata.nasa.gov/")
        print("="*60)
    
    def authenticate(self) -> bool:
        """Authenticate using ASF's official methods"""
        print(f"\n🔐 Starting ASF authentication...")
        
        try:
            # Try token authentication first
            if self.edl_token:
                print("🔑 Authenticating with EDL token...")
                self.session = asf.ASFSession().auth_with_token(self.edl_token)
                print("✅ Token authentication successful!")
                return True
            
            # Fall back to username/password
            elif self.username and self.password:
                print(f"🔑 Authenticating with username/password...")
                print(f"   Username: {self.username}")
                self.session = asf.ASFSession().auth_with_creds(self.username, self.password)
                print("✅ Credentials authentication successful!")
                return True
            
            else:
                print("❌ No valid authentication credentials found")
                return False
                
        except asf.ASFAuthenticationError as e:
            print(f"❌ ASF authentication failed: {e}")
            print("💡 Check your credentials and try again")
            return False
        except Exception as e:
            print(f"❌ Authentication error: {e}")
            return False
    
    def download_scene_with_progress(self, scene_metadata: Dict) -> Optional[Path]:
        """Download and extract a SAR scene using ASF's official library"""
        
        granule_name = scene_metadata['granule_name']
        url = scene_metadata['url']
        size_mb = scene_metadata.get('size_mb', 0)
        
        print(f"\n🚀 DOWNLOADING: {granule_name}")
        print(f"📦 Size: {size_mb:.1f} MB ({size_mb/1024:.1f} GB)")
        print(f"🔗 URL: {url}")
        
        # Check if SAFE already exists
        safe_path = self.safe_dir / f"{granule_name}.SAFE"
        if safe_path.exists():
            print(f"✅ SAFE already exists: {safe_path}")
            return safe_path
        
        # Check if ZIP already exists
        zip_path = self.raw_dir / f"{granule_name}.zip"
        if zip_path.exists():
            print(f"✅ ZIP already exists, extracting...")
            return self.extract_safe_optimized(zip_path)
        
        try:
            # Download using ASF's official method
            print(f"⬇️  Starting download with asf_search...")
            start_time = datetime.now()
            
            # Download to raw directory
            asf.download_urls(
                urls=[url], 
                path=str(self.raw_dir),
                session=self.session
            )
            
            total_time = (datetime.now() - start_time).total_seconds()
            print(f"✅ Download complete in {total_time:.1f}s")
            
            # Verify the download
            if zip_path.exists():
                actual_size = zip_path.stat().st_size / (1024**2)  # MB
                print(f"📊 Downloaded size: {actual_size:.1f} MB")
                
                # Extract SAFE
                print(f"📂 Extracting SAFE directory...")
                extracted_safe = self.extract_safe_optimized(zip_path)
                
                if extracted_safe:
                    print(f"🎯 SAFE ready: {extracted_safe}")
                    
                    # Show analysis-ready files
                    measurement_files = self.get_measurement_files(extracted_safe)
                    print(f"📡 Ready for analysis:")
                    for pol, file_path in measurement_files.items():
                        file_size = file_path.stat().st_size / (1024**2)  # MB
                        print(f"   {pol}: {file_path.name} ({file_size:.0f} MB)")
                    
                    return extracted_safe
                else:
                    print("❌ SAFE extraction failed")
                    return None
            else:
                print("❌ Download failed - file not found")
                return None
                
        except asf.ASFAuthenticationError as e:
            print(f"❌ Authentication error during download: {e}")
            print("💡 Your token may have expired or credentials are invalid")
            return None
        except Exception as e:
            print(f"❌ Download error: {e}")
            return None
    
    def extract_safe_optimized(self, zip_path: Path) -> Optional[Path]:
        """Optimized SAFE extraction with verification"""
        try:
            print(f"📂 Extracting {zip_path.name}...")
            
            with zipfile.ZipFile(zip_path, 'r') as zf:
                # Find SAFE directory
                safe_entries = [name for name in zf.namelist() if name.endswith('.SAFE/')]
                
                if not safe_entries:
                    print("❌ No .SAFE directory found in ZIP")
                    return None
                
                safe_name = safe_entries[0].rstrip('/')
                print(f"📁 Found: {safe_name}")
                
                # Extract directly to final location
                final_safe = self.safe_dir / safe_name
                
                if final_safe.exists():
                    print("🗑️  Removing existing SAFE directory")
                    shutil.rmtree(final_safe)
                
                # Extract all SAFE contents
                safe_members = [m for m in zf.namelist() if m.startswith(safe_name)]
                
                print(f"📦 Extracting {len(safe_members)} files...")
                
                for member in safe_members:
                    zf.extract(member, self.safe_dir)
                
                # Verify structure
                if self.verify_safe_comprehensive(final_safe):
                    print(f"✅ Extraction complete: {final_safe}")
                    return final_safe
                else:
                    print("❌ SAFE verification failed")
                    return None
                    
        except Exception as e:
            print(f"❌ Extraction error: {e}")
            return None
    
    def verify_safe_comprehensive(self, safe_path: Path) -> bool:
        """Comprehensive SAFE verification"""
        try:
            print(f"🔍 Verifying SAFE structure...")
            
            # Required directories and files
            required_dirs = ['annotation', 'measurement', 'preview']
            required_files = ['manifest.safe']
            
            # Check structure
            for dir_name in required_dirs:
                dir_path = safe_path / dir_name
                if not dir_path.exists():
                    print(f"❌ Missing directory: {dir_name}")
                    return False
                
                if dir_name == 'measurement':
                    tiff_files = list(dir_path.glob("*.tiff"))
                    if not tiff_files:
                        print(f"❌ No TIFF files in {dir_name}")
                        return False
                    print(f"✅ {dir_name}: {len(tiff_files)} TIFF files")
                else:
                    file_count = len(list(dir_path.iterdir()))
                    print(f"✅ {dir_name}: {file_count} files")
            
            for file_name in required_files:
                if not (safe_path / file_name).exists():
                    print(f"❌ Missing file: {file_name}")
                    return False
            
            # Check polarizations
            measurement_dir = safe_path / "measurement"
            polarizations = set()
            total_size_gb = 0
            
            for tiff_file in measurement_dir.glob("*.tiff"):
                filename = tiff_file.name.lower()
                size_gb = tiff_file.stat().st_size / (1024**3)
                total_size_gb += size_gb
                
                if 'vv' in filename:
                    polarizations.add('VV')
                elif 'vh' in filename:
                    polarizations.add('VH')
                elif 'hh' in filename:
                    polarizations.add('HH')
                elif 'hv' in filename:
                    polarizations.add('HV')
            
            print(f"✅ Polarizations: {sorted(polarizations)}")
            print(f"✅ Total measurement data: {total_size_gb:.2f} GB")
            print(f"✅ SAFE verification complete")
            
            return len(polarizations) > 0
            
        except Exception as e:
            print(f"❌ Verification error: {e}")
            return False
    
    def get_measurement_files(self, safe_path: Path) -> Dict[str, Path]:
        """Get measurement files organized by polarization"""
        measurement_files = {}
        
        try:
            measurement_dir = safe_path / "measurement"
            
            for tiff_file in measurement_dir.glob("*.tiff"):
                filename = tiff_file.name.lower()
                
                if 'vv' in filename:
                    measurement_files['VV'] = tiff_file
                elif 'vh' in filename:
                    measurement_files['VH'] = tiff_file
                elif 'hh' in filename:
                    measurement_files['HH'] = tiff_file
                elif 'hv' in filename:
                    measurement_files['HV'] = tiff_file
            
            return measurement_files
            
        except Exception as e:
            print(f"Error getting measurement files: {e}")
            return {}

# CLI Usage
def main():
    """CLI interface for downloading SAR data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ASF Official Library Downloader')
    parser.add_argument('--metadata-dir', type=Path, default='./sar_data/asf', 
                       help='Directory containing metadata JSON files')
    parser.add_argument('--download-dir', type=Path, default='./sar_data',
                       help='Base download directory')
    parser.add_argument('--max-scenes', type=int, default=1,
                       help='Maximum scenes to download')
    parser.add_argument('--check-auth', action='store_true',
                       help='Only check authentication, don\'t download')
    
    args = parser.parse_args()
    
    print("🔧 ASF Official Library Downloader")
    print("="*50)
    
    # Initialize downloader
    downloader = ASFOfficialDownloader(args.download_dir)
    
    # Check authentication
    if args.check_auth:
        print("\n🔐 Testing ASF authentication...")
        success = downloader.authenticate()
        if success:
            print("✅ ASF authentication successful!")
            
            # Test a quick search to verify full functionality
            try:
                print("🧪 Testing search functionality...")
                test_results = asf.granule_search(['S1A_IW_SLC__1SDV_20250714T154854_20250714T154920_060082_077700_D5A5'])
                if test_results:
                    print(f"✅ Search test successful - found {len(test_results)} results")
                else:
                    print("⚠️  Search test returned no results")
            except Exception as e:
                print(f"⚠️  Search test failed: {e}")
        else:
            print("❌ ASF authentication failed!")
        return
    
    # Find metadata files
    metadata_files = list(args.metadata_dir.glob("*.json"))
    metadata_files = [f for f in metadata_files if not f.name.startswith('pipeline_summary')]
    
    if not metadata_files:
        print(f"❌ No metadata files found in {args.metadata_dir}")
        print("Run the pipeline first: python sar_pipeline.py --days-back 7")
        return
    
    # Limit scenes
    metadata_files = metadata_files[:args.max_scenes]
    
    print(f"\n📋 Found {len(metadata_files)} metadata files:")
    for f in metadata_files:
        print(f"   📄 {f.name}")
    
    # Download
    try:
        if not downloader.authenticate():
            print("❌ ASF authentication failed - cannot download")
            return
        
        for metadata_file in metadata_files:
            print(f"\n📄 Processing: {metadata_file.name}")
            
            # Load metadata
            with open(metadata_file, 'r') as f:
                scene_metadata = json.load(f)
            
            # Download and extract
            safe_path = downloader.download_scene_with_progress(scene_metadata)
            
            if safe_path:
                print(f"✅ Complete: {safe_path.name}")
            else:
                print(f"❌ Failed: {metadata_file.name}")
            
    except KeyboardInterrupt:
        print("⏹️  Download interrupted by user")
    except Exception as e:
        print(f"❌ Download error: {e}")

if __name__ == "__main__":
    main()