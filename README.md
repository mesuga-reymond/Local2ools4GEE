# GEE2DB: Earth Engine to PostGIS Spatial Pipeline

**GEE2DB** is a professional-grade graphical interface designed to streamline the extraction of satellite imagery from **Google Earth Engine (GEE)** and its integration into local **PostGIS** databases and **GeoServer** environments.

## 🛠 Features

* **Secure Credential Management**: Uses an encrypted SQLite "Vault" powered by the Fernet cipher to store your PostGIS and GeoServer passwords locally.
* **Flexible Area of Interest (AOI)**: 
    * Define areas by browsing for `.SHP`, `.GeoJSON`, or `.TIFF` files.
    * **Manual Annotate Mode**: Mark corners directly on the interactive map to define a custom bounding box.
    * **Global Search**: Integrated location search and coordinate "jump" functionality.
* **Multi-Sensor Support**: Native pipelines for Sentinel-2 (Cloud-masked), Landsat 8/9, Sentinel-1 (Radar), and SRTM Elevation data.
* **Advanced Batch Processing**: 
    * **Time-Series Mode**: Scout and download imagery across specific date intervals (e.g., every 30 days).
    * **Automated Stitching**: Merges downloaded Earth Engine tiles into a single seamless `.TIF` file.
* **Full Deployment Stack**: 
    * **PostGIS Sync**: Automatically pushes acquisition metadata and geometries to a spatial database.
    * **GeoServer API**: One-click sync to GeoServer with automated SLD styling and layer creation.
    * **Web Dashboard**: Generates an interactive Leaflet-based HTML map to view your synced layers in any browser.

## 🚀 Installation & Setup

1.  **Environment**: Ensure you have Python 3.x installed with the following core dependencies:
    * `tkinter`, `tkintermapview`, `tkcalendar`
    * `earthengine-api`, `geopandas`, `rasterio`
    * `psycopg2`, `cryptography`
2.  **Earth Engine**: You must have a Google Earth Engine account and an active Google Cloud Project.
3.  **Local Stack**: A running instance of PostgreSQL/PostGIS and GeoServer (optional, for deployment features).

## 📖 Usage

1.  **Connect**: Enter your GEE Project ID in the **Control** tab and click **Connect**.
2.  **AOI**: Load a shapefile or use the **Manual Annotate Mode** on the map to select your study area.
3.  **Configure**: Select your dataset, spectral bands (e.g., RGB, NIR, NDVI), and target date(s).
4.  **Download**: Click **Start Download**. The **Tasks** tab will show real-time progress.
5.  **Deploy**: Use the **Database & Deploy** tab to sync your data to PostGIS and visualize it via the generated Web Map.

## 📂 Project Structure

* `/Data`: Root folder for all downloaded projects and GeoTIFFs.
* `/Snapshots`: High-fidelity map captures.
* `vault.db`: Encrypted local database for your login credentials.
* `download_records.csv`: Master metadata log for every successful download.

---

*This tool was developed to bridge the gap between cloud-scale remote sensing and local GIS workflows.*