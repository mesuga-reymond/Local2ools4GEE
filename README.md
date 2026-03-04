# Local2ools4GEE: A Desktop Interface to Download, Visualize, and Manage GEE Spatial Data Locally

---

## 🛠️ Overview
**Local2ools4GEE** is a high-utility engineering toolkit designed to bridge the gap between Google Earth Engine's cloud compute power and local geospatial workflows. Unlike standard web-based GEE interfaces, this tool focuses on the **Local Pipeline**: extracting precise satellite data, inventorying it in a private PostGIS database, and providing a high-performance desktop environment for visualization and deployment.



---

## 🚀 Key Features

### 📡 Data Acquisition (The ETL Engine)
* **Multi-Sensor Support:** Native support for Sentinel-2 (SR), Landsat 8/9, Sentinel-1 (SAR), and SRTM Elevation data.
* **Time-Series Batching:** Automated "Scouting" logic that finds the closest satellite passes across a 180-day window for scheduled intervals (e.g., monthly monitoring).
* **Smart Stitching:** Automatically handles GEE's 1x1 degree tiling limits by downloading sub-tiles and stitching them into seamless, high-resolution GeoTIFFs.
* **Strict Temporal Logic:** Option to force single-date downloads to prevent "patchwork" artifacts in large provincial maps.

### 🗺️ High-Fidelity Visualization
* **The "Pinner" Engine:** A custom threading watchdog that "glues" heavy raster previews (500MB+) onto an interactive map without UI lag.
* **Vector Integration:** Native rendering for `.shp` and `.geojson` with dynamic label visibility based on zoom levels.
* **Pixel Inspector:** Real-time metadata hover tool to inspect raw band values directly on the map.

### 🗄️ Database & Deployment
* **PostGIS Inventory:** Automatically pushes spatial metadata (acquisition date, file size, dataset, geometry) to a `satellite_inventory` table.
* **Secure Vault:** Encrypted storage for PostGIS and GeoServer credentials using AES-256 (Fernet) encryption.
* **GeoServer Sync:** Direct integration with GeoServer REST API to upload local rasters and apply specialized SLD styles.
* **Web Dashboard:** One-click generation of a standalone Leaflet.js dashboard to share your local layers via a web browser.



---

## ⚙️ Installation

### 1. Prerequisites
* **Python 3.10+**
* **Google Earth Engine Account:** A registered GCP project ID.
* **PostgreSQL/PostGIS:** (Optional) For management and inventory features.
* **GeoServer:** (Optional) For web deployment features.

### 2. Environment Setup
```bash
# Clone the repository
git clone [https://github.com/USERNAME/Local2ools4GEE.git](https://github.com/USERNAME/Local2ools4GEE.git)
cd Local2ools4GEE

# Install dependencies
pip install -r requirements.txt
