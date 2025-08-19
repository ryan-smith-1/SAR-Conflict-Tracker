#!/usr/bin/env python3
"""
SAR Data Pipeline - Automated Sentinel-1 Retrieval System
Supports both ASF and Copernicus/Sentinel Hub data sources
Optimized for OSINT change detection workflows
"""

import os
import json
import logging
import asyncio
import aiofiles
from datetime import datetime, timedelta
from pathlib import Path
import requests
from typing import List, Dict, Tuple, Optional
import geopandas as gpd
from shapely.geometry import Polygon
import asf_search as asf
from sentinelhub import (
    SHConfig, DataCollection, BBox, CRS, MimeType, 
    SentinelHubRequest, bbox_to_dimensions
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sar_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SARDataPipeline:
    """
    Automated SAR data retrieval pipeline supporting ASF and Copernicus sources
    """
    
    def __init__(self, config_file: str = "pipeline_config.json"):
        """Initialize pipeline with configuration"""
        self.config = self.load_config(config_file)
        self.data_dir = Path(self.config['data_directory'])
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize ASF configuration
        self.asf_session = self._setup_asf()
        
        # Initialize Sentinel Hub configuration
        self.sh_config = self._setup_sentinel_hub()
        
        logger.info("SAR Data Pipeline initialized successfully")

    def load_config(self, config_file: str) -> dict:
        """Load pipeline configuration from JSON file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            return config
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found. Creating default config.")
            return self._create_default_config(config_file)
    
    def _create_default_config(self, config_file: str) -> dict:
        """Create default configuration file"""
        default_config = {
            "data_directory": "./sar_data",
            "area_of_interest": {
                "name": "target_area",
                "coordinates": [
                    [-74.0, 40.7],  # Example: NYC area
                    [-74.0, 40.8],
                    [-73.9, 40.8],
                    [-73.9, 40.7],
                    [-74.0, 40.7]
                ]
            },
            "temporal_range": {
                "days_back": 30,
                "max_cloud_cover": 20
            },
            "sentinel_hub": {
                "instance_id": "1f313dcc-eccb-4eee-a015-681cf6efd470",
                "client_id": "",
                "client_secret": "",
                "sh_base_url": "https://services.sentinel-hub.com"
            },
            "asf": {
                "download_directory": "./sar_data/asf",
                "max_results": 100
            },
            "processing": {
                "resolution": 10,
                "bbox_size_km": 50
            }
        }
        
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        logger.info(f"Created default config file: {config_file}")
        logger.info("Please update the configuration with your credentials and area of interest")
        return default_config

    def _setup_asf(self) -> requests.Session:
        """Setup ASF session for data retrieval"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'SAR-Pipeline/1.0'
        })
        return session

    def _setup_sentinel_hub(self) -> SHConfig:
        """Setup Sentinel Hub configuration for Copernicus Data Space Ecosystem"""
        config = SHConfig()
        
        if self.config['sentinel_hub']['client_id']:
            config.sh_client_id = self.config['sentinel_hub']['client_id']
            config.sh_client_secret = self.config['sentinel_hub']['client_secret']
            
            # Configure for Copernicus Data Space Ecosystem
            config.sh_base_url = "https://sh.dataspace.copernicus.eu"
            config.sh_token_url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
            
            # Instance ID is optional for CDSE
            if self.config['sentinel_hub']['instance_id']:
                config.instance_id = self.config['sentinel_hub']['instance_id']
                logger.info(f"Using Instance ID: {config.instance_id}")
            else:
                logger.info("Using Copernicus Data Space Ecosystem (no Instance ID)")
        else:
            logger.warning("Sentinel Hub credentials not configured")
            
        return config

    def get_area_bbox(self) -> Tuple[float, float, float, float]:
        """Convert area of interest to bounding box"""
        coords = self.config['area_of_interest']['coordinates']
        polygon = Polygon(coords)
        return polygon.bounds  # (minx, miny, maxx, maxy)

    async def search_asf_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Search for Sentinel-1 data using ASF API"""
        logger.info(f"Searching ASF for data from {start_date} to {end_date}")
        
        try:
            # Create WKT from area of interest
            coords = self.config['area_of_interest']['coordinates']
            polygon = Polygon(coords)
            wkt = polygon.wkt
            
            # Search for Sentinel-1 SLC products
            results = asf.search(
                platform=[asf.PLATFORM.SENTINEL1],
                processingLevel=[asf.PRODUCT_TYPE.SLC],
                start=start_date,
                end=end_date,
                intersectsWith=wkt,
                maxResults=self.config['asf']['max_results']
            )
            
            logger.info(f"Found {len(results)} ASF results")
            
            # Convert to list of dictionaries for easier handling
            data_list = []
            for result in results:
                try:
                    # Handle different property names that might exist
                    granule_name = (
                        result.properties.get('sceneName') or
                        result.properties.get('fileName') or
                        result.properties.get('granuleName') or
                        result.properties.get('productName') or
                        'unknown_granule'
                    )
                    
                    acquisition_date = (
                        result.properties.get('startTime') or
                        result.properties.get('acquisitionDate') or
                        result.properties.get('sensingTime') or
                        'unknown_date'
                    )
                    
                    data_list.append({
                        'granule_name': granule_name,
                        'acquisition_date': str(acquisition_date),
                        'platform': result.properties.get('platform', 'SENTINEL-1'),
                        'beam_mode': result.properties.get('beamModeType', 'unknown'),
                        'url': result.properties.get('url', ''),
                        'size_mb': result.properties.get('bytes', 0) / (1024*1024) if result.properties.get('bytes') else 0,
                        'path': result.properties.get('pathNumber', 'unknown'),
                        'frame': result.properties.get('frameNumber', 'unknown'),
                        'orbit_direction': result.properties.get('flightDirection', 'unknown'),
                        'polarization': result.properties.get('polarization', 'unknown'),
                        's3_urls': result.properties.get('s3Urls', [])  # For cloud access
                    })
                except Exception as prop_error:
                    logger.warning(f"Error parsing result properties: {prop_error}")
                    logger.warning(f"Available properties: {list(result.properties.keys())}")
                    # Still add a basic entry
                    data_list.append({
                        'granule_name': 'parsing_error',
                        'acquisition_date': 'unknown',
                        'platform': 'SENTINEL-1',
                        'error': str(prop_error),
                        'all_properties': list(result.properties.keys())
                    })
            
            return data_list
            
        except Exception as e:
            logger.error(f"Error searching ASF data: {e}")
            return []

    def find_target_scenes(self, asf_results: List[Dict], days_back: int) -> List[Dict]:
        """Find the most recent scene and scene closest to target days back"""
        if not asf_results:
            logger.warning("No ASF results to filter")
            return []
        
        # Parse dates and sort
        scenes_with_dates = []
        current_time = datetime.now()
        
        for scene in asf_results:
            try:
                acq_date_str = scene.get('acquisition_date', '')
                if 'T' in acq_date_str:
                    # Handle ISO format with potential timezone
                    acq_date_str = acq_date_str.replace('Z', '+00:00')
                    if '+00:00' in acq_date_str:
                        acq_date = datetime.fromisoformat(acq_date_str.replace('+00:00', ''))
                    else:
                        acq_date = datetime.fromisoformat(acq_date_str)
                    scenes_with_dates.append((scene, acq_date))
                else:
                    logger.warning(f"Could not parse date: {acq_date_str}")
            except Exception as e:
                logger.warning(f"Error parsing date for scene {scene.get('granule_name', 'unknown')}: {e}")
        
        if not scenes_with_dates:
            logger.error("No scenes with valid dates found")
            return []
        
        # Sort by date (newest first)
        scenes_with_dates.sort(key=lambda x: x[1], reverse=True)
        
        # Find most recent scene
        most_recent = scenes_with_dates[0]
        logger.info(f"Most recent scene: {most_recent[0]['granule_name']} ({most_recent[1].date()})")
        
        # Find scene closest to target days back
        target_date = current_time - timedelta(days=days_back)
        logger.info(f"Target date ({days_back} days back): {target_date.date()}")
        
        # Find closest match to target date
        closest_scene = min(scenes_with_dates, key=lambda x: abs((x[1] - target_date).days))
        days_difference = abs((closest_scene[1] - target_date).days)
        
        logger.info(f"Closest scene to target: {closest_scene[0]['granule_name']} ({closest_scene[1].date()}) - {days_difference} days from target")
        
        # Return both scenes (avoid duplicates)
        selected_scenes = [most_recent[0]]
        if closest_scene[0]['granule_name'] != most_recent[0]['granule_name']:
            selected_scenes.append(closest_scene[0])
        else:
            logger.info("Most recent scene is also the closest to target date")
        
        logger.info(f"Selected {len(selected_scenes)} scenes for metadata generation")
        return selected_scenes

    async def search_sentinel_hub_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Search for Sentinel-1 data using Sentinel Hub API (CDSE)"""
        logger.info(f"Searching Sentinel Hub (CDSE) for data from {start_date} to {end_date}")
        
        try:
            if not self.config['sentinel_hub']['client_id']:
                logger.warning("Sentinel Hub not configured, skipping")
                return []
                
            # Define area of interest
            bbox_coords = self.get_area_bbox()
            bbox = BBox(bbox=bbox_coords, crs=CRS.WGS84)
            
            # For CDSE, we can use the Catalog API to search for data
            # This is more modern than the old Instance-based approach
            
            # Create a simple evalscript for Sentinel-1 data
            evalscript = """
            //VERSION=3
            function setup() {
                return {
                    input: ["VV", "VH"],
                    output: { 
                        id: "default",
                        bands: 2,
                        sampleType: "FLOAT32"
                    }
                };
            }
            
            function evaluatePixel(sample) {
                return [sample.VV, sample.VH];
            }
            """
            
            # Search using the modern CDSE approach
            time_interval = (start_date.isoformat(), end_date.isoformat())
            
            # Return metadata for available data
            return [{
                'source': 'sentinel_hub_cdse',
                'bbox': bbox_coords,
                'time_range': f"{start_date.isoformat()}/{end_date.isoformat()}",
                'collection': 'SENTINEL1_IW',
                'evalscript': evalscript,
                'search_ready': True,
                'note': 'Using Copernicus Data Space Ecosystem'
            }]
            
        except Exception as e:
            logger.error(f"Error with Sentinel Hub CDSE search: {e}")
            return []

    async def download_asf_data(self, data_item: Dict, download_dir: Path) -> bool:
        """Download individual ASF data item"""
        try:
            granule_name = data_item['granule_name']
            download_path = download_dir / f"{granule_name}.zip"
            
            if download_path.exists():
                logger.info(f"File already exists: {download_path}")
                return True
            
            logger.info(f"Downloading {granule_name} ({data_item['size_mb']} MB)")
            
            # Use ASF download (requires authentication for full products)
            # For now, we'll simulate the download process
            url = data_item['url']
            
            async with aiofiles.open(download_path.with_suffix('.json'), 'w') as f:
                await f.write(json.dumps(data_item, indent=2))
            
            logger.info(f"Metadata saved for {granule_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error downloading {data_item['granule_name']}: {e}")
            return False

    async def download_sentinel_hub_data(self, data_item: Dict, download_dir: Path) -> bool:
        """Download Sentinel Hub data"""
        try:
            if not data_item.get('request_ready'):
                return False
                
            # Create filename based on time range and bbox
            filename = f"sentinel_hub_{data_item['time_range'].replace('/', '_to_')}.tiff"
            download_path = download_dir / filename
            
            if download_path.exists():
                logger.info(f"File already exists: {download_path}")
                return True
            
            logger.info(f"Processing Sentinel Hub request for {data_item['time_range']}")
            
            # Save metadata for now
            async with aiofiles.open(download_path.with_suffix('.json'), 'w') as f:
                await f.write(json.dumps(data_item, indent=2))
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing Sentinel Hub data: {e}")
            return False

    async def run_pipeline(self, days_back: int = None) -> Dict:
        """Run the complete data retrieval pipeline"""
        logger.info("Starting SAR data pipeline execution")
        
        # Calculate time range
        if days_back is None:
            days_back = self.config['temporal_range']['days_back']
            
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Searching for data from {start_date} to {end_date}")
        
        # Create download directories
        asf_dir = self.data_dir / "asf"
        sh_dir = self.data_dir / "sentinel_hub"
        asf_dir.mkdir(exist_ok=True)
        sh_dir.mkdir(exist_ok=True)
        
        # Search both sources
        asf_results = await self.search_asf_data(start_date, end_date)
        sh_results = await self.search_sentinel_hub_data(start_date, end_date)
        
        # Find target scenes (most recent + closest to days_back)
        target_scenes = self.find_target_scenes(asf_results, days_back)
        
        # Download data
        asf_downloads = []
        sh_downloads = []
        
        # Download selected ASF scenes
        logger.info(f"\n{'='*60}")
        logger.info(f"DOWNLOADING SELECTED SCENES")
        logger.info(f"{'='*60}")
        
        for i, item in enumerate(target_scenes, 1):
            logger.info(f"\nDownloading scene {i}/{len(target_scenes)}: {item['granule_name']}")
            success = await self.download_asf_data(item, asf_dir)
            asf_downloads.append(success)
        
        # Download Sentinel Hub data
        for item in sh_results:
            success = await self.download_sentinel_hub_data(item, sh_dir)
            sh_downloads.append(success)
        
        # Generate summary
        summary = {
            'execution_time': datetime.now().isoformat(),
            'time_range': {
                'start': start_date.isoformat(),
                'end': end_date.isoformat()
            },
            'target_days_back': days_back,
            'asf_results': {
                'found': len(asf_results),
                'selected': len(target_scenes),
                'downloaded': sum(asf_downloads)
            },
            'sentinel_hub_results': {
                'found': len(sh_results),
                'processed': sum(sh_downloads)
            },
            'total_files': sum(asf_downloads) + sum(sh_downloads),
            'selected_scenes': [scene['granule_name'] for scene in target_scenes]
        }
        
        # Save summary
        summary_path = self.data_dir / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        async with aiofiles.open(summary_path, 'w') as f:
            await f.write(json.dumps(summary, indent=2))
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PIPELINE EXECUTION COMPLETED")
        logger.info(f"Selected scenes: {[scene['granule_name'] for scene in target_scenes]}")
        logger.info(f"Summary: {summary}")
        logger.info(f"{'='*60}")
        return summary

    def schedule_pipeline(self, interval_hours: int = 24):
        """Schedule pipeline to run at regular intervals"""
        logger.info(f"Scheduling pipeline to run every {interval_hours} hours")
        
        async def scheduled_run():
            while True:
                try:
                    await self.run_pipeline()
                    await asyncio.sleep(interval_hours * 3600)
                except Exception as e:
                    logger.error(f"Scheduled pipeline error: {e}")
                    await asyncio.sleep(3600)  # Wait 1 hour before retry
        
        return scheduled_run()

# CLI Interface
async def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='SAR Data Pipeline')
    parser.add_argument('--config', default='pipeline_config.json', help='Configuration file path')
    parser.add_argument('--days-back', type=int, default=7, help='Days back to search')
    parser.add_argument('--schedule', action='store_true', help='Run in scheduled mode')
    parser.add_argument('--interval', type=int, default=24, help='Schedule interval in hours')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = SARDataPipeline(args.config)
    
    if args.schedule:
        logger.info("Running in scheduled mode")
        await pipeline.schedule_pipeline(args.interval)
    else:
        logger.info("Running single execution")
        await pipeline.run_pipeline(args.days_back)

if __name__ == "__main__":
    asyncio.run(main())