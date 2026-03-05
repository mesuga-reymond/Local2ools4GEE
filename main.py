# GEE2DB: A Graphical Interface for Earth Engine to PostGIS Integration
import ctypes
try:
    ctypes.windll.shcore.SetProcessDpiAwareness(1) # Forces Windows to stay 1:1 with Python
except:
    pass
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import os
import sys

if "PROJ_LIB" in os.environ:
    del os.environ["PROJ_LIB"]

try:
    import pyproj
    pyproj_path = os.path.join(os.path.dirname(pyproj.__file__), "proj_data")
    if os.path.exists(pyproj_path):
        os.environ["PROJ_LIB"] = pyproj_path
except:
    pass

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import geopandas as gpd
import rasterio
from rasterio.warp import transform_bounds
import ee
import re
import requests
import os
import csv
import threading
from datetime import datetime, timedelta
import tkintermapview
from tkcalendar import Calendar
import time
import psycopg2
from psycopg2 import extras
from PIL import Image, ImageTk
import numpy as np

import sqlite3
from cryptography.fernet import Fernet

SENTINEL_SLD = """<?xml version="1.0" encoding="UTF-8"?>
<StyledLayerDescriptor version="1.0.0" 
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
    xmlns="http://www.opengis.net/sld" 
    xmlns:ogc="http://www.opengis.net/ogc" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <NamedLayer>
    <Name>sentinel_enhance</Name>
    <UserStyle>
      <FeatureTypeStyle>
        <Rule>
          <RasterSymbolizer>
            <ChannelSelection>
              <RedChannel><SourceChannelName>1</SourceChannelName></RedChannel>
              <GreenChannel><SourceChannelName>2</SourceChannelName></GreenChannel>
              <BlueChannel><SourceChannelName>3</SourceChannelName></BlueChannel>
            </ChannelSelection>
            <ContrastEnhancement>
              <Normalize>
                <VendorOption name="algorithm">StretchToMinimumMaximum</VendorOption>
                </Normalize>
            </ContrastEnhancement>
          </RasterSymbolizer>
        </Rule>
      </FeatureTypeStyle>
    </UserStyle>
  </NamedLayer>
</StyledLayerDescriptor>
"""

class CredentialVault:
    def __init__(self, db_path="vault.db"):
        self.db_path = db_path
        # Static local key for the SQLite lockbox.
        self.key = b'vS-R5W_QYwzH8KxY8xNq_m8t_c4R-2Q1_mE7_e9xXw8='
        self.cipher = Fernet(self.key)
        self._bootstrap()

    def _bootstrap(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS secrets (key TEXT PRIMARY KEY, value BLOB)")

    def store(self, key, value):
        if not value: return
        encrypted_value = self.cipher.encrypt(value.encode())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("INSERT OR REPLACE INTO secrets VALUES (?, ?)", (key, encrypted_value))

    def retrieve(self, key):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("SELECT value FROM secrets WHERE key = ?", (key,)).fetchone()
            if row: return self.cipher.decrypt(row[0]).decode()
        return ""

class GeodatabaseManager:
    def __init__(self, host="localhost", dbname="ai4caf_db", user="postgres", password=""):
        self.params = {"host": host, "dbname": dbname, "user": user, "password": password}

    def update_params(self, host, db, user, pw):
        self.params = {"host": host, "dbname": db, "user": user, "password": pw}

    def test_connection(self):
        try:
            conn = psycopg2.connect(**self.params, connect_timeout=3)
            conn.close()
            return True, "Connected successfully!"
        except Exception as e:
            return False, str(e).split('\n')[0]

    def setup_tables(self):
        commands = [
            "CREATE EXTENSION IF NOT EXISTS postgis;",
            """CREATE TABLE IF NOT EXISTS satellite_inventory (
                id SERIAL PRIMARY KEY,
                acquisition_date DATE,
                file_name TEXT,
                dataset TEXT,
                crs TEXT,
                file_path TEXT UNIQUE,
                location_geom GEOMETRY(Polygon, 4326),
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );""",
            "CREATE INDEX IF NOT EXISTS idx_geom ON satellite_inventory USING GIST (location_geom);",
            "CREATE INDEX IF NOT EXISTS idx_date ON satellite_inventory (acquisition_date);" # Added the missing comma and index
        ]
        try:
            # We use a short timeout so the UI doesn't hang if the password is wrong
            params = self.params.copy()
            params['connect_timeout'] = 3 
            with psycopg2.connect(**params) as conn:
                with conn.cursor() as cur:
                    for cmd in commands: 
                        cur.execute(cmd)
                conn.commit()
            return True
        except Exception as e: 
            print(f"Setup Error: {e}")
            return False

    def push_metadata(self, meta, log_func=None):
        try:
            # 1. Path Handling (Ensures GeoServer can find the file relative to the 'Data' folder)
            full_path = meta["Path"]
            data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Data")
            relative_path = os.path.relpath(full_path, data_dir).replace("\\", "/")

            # 2. Date Formatting (Ensures strict YYYY-MM-DD for the DATE column)
            try:
                # Try to parse and re-format just in case GEE sends a weird string
                clean_date = datetime.strptime(meta["Date"], "%Y-%m-%d").date()
            except:
                clean_date = meta["Date"] # Fallback to raw string if already formatted

            # 3. Coordinate Parsing
            nums = re.findall(r"[-+]?\d*\.\d+|\d+", meta["Bounds"])
            y, x = float(nums[0]), float(nums[1])
            offset = 0.05 
            wkt = f"SRID=4326;POLYGON(({x-offset} {y-offset}, {x+offset} {y-offset}, {x+offset} {y+offset}, {x-offset} {y+offset}, {x-offset} {y-offset}))"

            query = """
                INSERT INTO satellite_inventory (acquisition_date, file_name, dataset, crs, file_path, location_geom)
                VALUES (%s, %s, %s, %s, %s, ST_GeomFromEWKT(%s))
                ON CONFLICT (file_path) DO UPDATE SET 
                    acquisition_date = EXCLUDED.acquisition_date,
                    location_geom = EXCLUDED.location_geom;
            """
            
            with psycopg2.connect(**self.params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (clean_date, meta["File Name"], meta["Dataset"], meta["CRS"], relative_path, wkt))
                conn.commit()
                
            if log_func: log_func(f"Database: Saved {relative_path}")
        except Exception as e:
            if log_func: log_func(f"DB Error: {str(e)[:50]}")

# Persistent Storage
HISTORY_FILE = "project_history.txt"
LOG_FILE = "session_logs.txt"
RECORDS_FILE = "download_records.csv" # New metadata database

class GEE_Local_Downloader_App:
    def __init__(self, root):
        self.root = root
        self.root.title("GEE2DB")
        
        try: self.root.state('zoomed')
        except: pass
        self.root.minsize(1150, 750)

        style = ttk.Style()
        style.theme_use('clam')
        
        # --- Variables ---
        self.input_file_path = tk.StringVar()
        self.dataset_var = tk.StringVar(value="Sentinel-2 (Cloud-Masked RGB)")
        self.yesterday_val = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.target_date_var = tk.StringVar(value=self.yesterday_val)
        self.selected_date_var = tk.StringVar(value="") # What GEE actually has
        self.project_id_var = tk.StringVar()
        self.draw_mode_active = tk.BooleanVar(value=False)
        self.stitch_tiles_var = tk.BooleanVar(value=True) # Defaults to stitching ON
        self.target_crs_var = tk.StringVar(value="EPSG:4326 (WGS84 Lat/Lon)")
        self.manual_annotate_mode = False  # The attribute that was missing!
        self.temp_start_coords = None      # To store the first click
        self.roi_polygon = None            # To store the drawn box
        self.search_history_file = "search_history.txt"
        self.search_history = self.load_search_history()
        self.raster_lock = threading.Lock()
        self.active_rasters = {}
        self.active_layer_polygons = {}
        self.tracker_running = False
        self.is_closing = False
        self.is_batch_loading = False  # The master switch for zoom behavior
        self.db_manager = GeodatabaseManager()
        self.vault = CredentialVault()
        
        # New GeoServer StringVars
        self.gs_user_var = tk.StringVar(value="admin")
        self.gs_pass_var = tk.StringVar(value="")
        self.save_creds_var = tk.BooleanVar(value=False)
        # self.db_manager = GeodatabaseManager(password="YOUR_DB_PASSWORD")
        # threading.Thread(target=self.db_manager.setup_tables, daemon=True).start()
        
        # --- Band Variables ---
        self.s2_bands = {
            "Red (B4)": tk.BooleanVar(value=True),
            "Green (B3)": tk.BooleanVar(value=True),
            "Blue (B2)": tk.BooleanVar(value=True),
            "NIR (B8)": tk.BooleanVar(value=False),
            "SWIR 1 (B11)": tk.BooleanVar(value=False)
        }
        self.l8_bands = {
            "Red (B4)": tk.BooleanVar(value=True),
            "Green (B3)": tk.BooleanVar(value=True),
            "Blue (B2)": tk.BooleanVar(value=True),
            "NIR (B5)": tk.BooleanVar(value=False),
            "SWIR 1 (B6)": tk.BooleanVar(value=False)
        }
        self.s1_bands = {
            "VV (Vertical)": tk.BooleanVar(value=True),
            "VH (Horizontal)": tk.BooleanVar(value=True)
        }
        self.index_vars = {
            "NDVI": tk.BooleanVar(value=False),
            "EVI": tk.BooleanVar(value=False),
            "NDWI": tk.BooleanVar(value=False)
        }
        
        # --- App State ---
        self.click_buffer = [] 
        self.manual_roi_bounds = None 
        self.temp_markers = [] 
        self.roi_polygon = None
        self.saved_projects = self.load_project_history()
        
        self.create_widgets()
        
        # Update UI when dataset changes
        self.dataset_dropdown.bind("<<ComboboxSelected>>", self.update_band_ui)
        self.update_band_ui() 

        # Load existing records into the table
        self.load_records_from_file()

        self.populate_layers_tree()

        if self.saved_projects:
            self.project_id_var.set(self.saved_projects[0])
            threading.Thread(target=self.check_auth_status, daemon=True).start()

        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    # --- Persistence & Logging ---
    def load_project_history(self):
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r") as f:
                return [line.strip() for line in f.readlines() if line.strip()]
        return []

    def save_project_id(self, pid):
        pid = pid.strip()
        if pid and pid not in self.saved_projects:
            self.saved_projects.insert(0, pid)
            with open(HISTORY_FILE, "w") as f: f.write("\n".join(self.saved_projects))
            self.project_dropdown['values'] = self.saved_projects

    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] {message}"
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f: f.write(formatted_msg + "\n")
        except: pass
        self.root.after(0, self._update_log_ui, formatted_msg)

    def _update_log_ui(self, msg):
        self.console.config(state="normal")
        self.console.insert(tk.END, msg + "\n")
        self.console.see(tk.END)
        self.console.config(state="disabled")

    # --- Metadata Record Handling ---
    def save_metadata_to_file(self, metadata):
        """Saves a row of metadata to the CSV record file."""
        file_exists = os.path.isfile(RECORDS_FILE)
        headers = ["Date", "File Name", "Dataset", "CRS", "Bounds", "Path"]
        with open(RECORDS_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerow(metadata)

    def load_records_from_file(self):
        """Populates the table, checks file existence, and safely preserves live download trackers."""
        # 1. Extract and save the "Live" rows so the background thread doesn't lose them
        live_rows = []
        for item in self.record_table.get_children():
            values = self.record_table.item(item, "values")
            # If the status column contains our spinning icon, it's active!
            if values and "🔄 Downloading..." in values[0]:
                # We save the specific Tkinter internal ID (item) and its current text
                live_rows.append((item, values))

        # 2. Clear existing table to prevent duplicates
        for item in self.record_table.get_children():
            self.record_table.delete(item)

        # 3. Load completed records from the CSV database
        if os.path.exists(RECORDS_FILE):
            with open(RECORDS_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Check file existence
                    path_exists = os.path.exists(row["Path"])
                    status = "✅ Found" if path_exists else "❌ Missing"
                    
                    self.record_table.insert("", "end", values=(
                        status, 
                        row["Date"], row["File Name"], row["Dataset"], 
                        row["CRS"], row["Bounds"], row["Path"]
                    ))

        # 4. Re-insert the live downloading rows exactly at the top
        # We reverse the list to maintain the original top-down visual order
        for iid, vals in reversed(live_rows):
            self.record_table.insert("", 0, iid=iid, values=vals)

        self.record_table.column("path", stretch=True)
        self.record_table.update_idletasks()

    def remove_selected_records(self):
        selected_items = self.record_table.selection()
        if not selected_items:
            messagebox.showinfo("Selection", "Please select at least one row to remove.")
            return

        if not messagebox.askyesno("Confirm", f"Remove {len(selected_items)} selected record(s) from history?"):
            return

        # 1. Collect paths of items to REMOVE
        paths_to_remove = [self.record_table.item(i, "values")[6] for i in selected_items]

        # 2. Delete from the UI table
        for item in selected_items:
            self.record_table.delete(item)

        # 3. Rewrite the CSV Database without those paths
        all_records = []
        if os.path.exists(RECORDS_FILE):
            with open(RECORDS_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row["Path"] not in paths_to_remove:
                        all_records.append(row)

            # Save the filtered list back to CSV
            headers = ["Date", "File Name", "Dataset", "CRS", "Bounds", "Path"]
            with open(RECORDS_FILE, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(all_records)

        self.log(f"Removed {len(selected_items)} records from history.")

    def update_band_ui(self, event=None):
        """Optimized grid layout for spectral bands and indices."""
        for widget in self.band_inner_frame.winfo_children():
            widget.destroy()
            
        ds = self.dataset_var.get()
        
        # --- THE FIX: Toggle Cloud Sort Checkbox State ---
        if "Sentinel-2" in ds:
            # Enable for optical data
            self.cloud_sort_check.config(state="normal")
        else:
            # Uncheck and disable for Sentinel-1 or SRTM
            self.sort_by_cloud_var.set(False)
            self.cloud_sort_check.config(state="disabled")
        
        # --- Existing Band Logic ---
        if "Sentinel-2" in ds:
            self.band_label.config(text="Spectral Bands & Vegetation Indices:")
            all_options = list(self.s2_bands.items()) + list(self.index_vars.items())
            
            for i, (name, var) in enumerate(all_options):
                ttk.Checkbutton(self.band_inner_frame, text=name, variable=var).grid(
                    row=i//4, column=i%4, sticky="w", padx=2, pady=2
                )
        elif "Landsat" in ds:
            self.cloud_sort_check.config(state="normal")
            self.band_label.config(text="Landsat Spectral Bands & Indices:")
            all_options = list(self.l8_bands.items()) + list(self.index_vars.items())
            
            for i, (name, var) in enumerate(all_options):
                ttk.Checkbutton(self.band_inner_frame, text=name, variable=var).grid(
                    row=i//4, column=i%4, sticky="w", padx=2, pady=2
                )
        elif "Sentinel-1" in ds:
            self.band_label.config(text="Radar Polarizations (SAR):")
            for i, (name, var) in enumerate(self.s1_bands.items()):
                ttk.Checkbutton(self.band_inner_frame, text=name, variable=var).grid(
                    row=i, column=0, sticky="w", padx=5, pady=2
                )
        else:
            self.band_label.config(text="No additional bands for Elevation.")

    def on_closing(self):
        """Cleanly shuts down the app and prevents 'Main thread not in main loop' errors."""
        if messagebox.askokcancel("Quit", "Do you want to close GEE2DB?"):
            # 1. Signal all loops to stop immediately
            self.is_closing = True 
            self.tracker_running = False
            
            # 2. Wipe image references to free Tcl memory while it's still alive
            self.clear_all() 
            
            # 3. Professional Nullification:
            # We manually clear all StringVars/BooleanVars so they don't 
            # trigger the error during garbage collection after root.destroy()
            for attr in list(self.__dict__.keys()):
                if isinstance(getattr(self, attr), (tk.Variable, tk.Image)):
                    setattr(self, attr, None)
            
            # 4. Final destruction
            self.root.destroy()
            
            # 5. The 'Hammer': Force kill any hanging background threads
            os._exit(0)

    def create_widgets(self):
        # --- DYNAMIC NOTEBOOK STYLING ---
        style = ttk.Style()
        style.theme_use('clam') 

        # Base style for all tabs
        style.configure("TNotebook.Tab", 
                        padding=[8, 4], 
                        font=("Segoe UI", 9),
                        background="#e1e1e1")

        # THE "EXPANDER" LOGIC: Grow the tab and bold the text when selected
        style.map("TNotebook.Tab",
                  padding=[("selected", [12, 6])], 
                  font=[("selected", ("Segoe UI", 9, "bold"))],
                  background=[("selected", "#ffffff")],
                  foreground=[("selected", "#0077b6")])

        paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill="both", expand=True, padx=5, pady=5)

        # WIDEN PANEL: Increased to 520 to allow room for the expanded "Database" name
        self.left_panel = ttk.Frame(paned, width=520) 
        self.left_panel.pack_propagate(False) 
        
        self.right_frame = ttk.Frame(paned)
        
        # Divider weights
        paned.add(self.left_panel, weight=0) 
        paned.add(self.right_frame, weight=1)

        self.notebook = ttk.Notebook(self.left_panel)
        self.notebook.pack(fill="both", expand=True)

        # ==========================================
        # --- TAB 1: CONTROLS (Gap-Killer Fix) ---
        # ==========================================
        self.tab_controls = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_controls, text=" ⚙️ Control ")

        # THE FIX: 
        # Row 0 (Settings) gets weight 0 -> It stays as small as possible (showing frames 1-4).
        # Row 1 (Log Area) gets weight 1 -> It expands to fill every single leftover pixel.
        self.tab_controls.rowconfigure(0, weight=0) 
        self.tab_controls.rowconfigure(1, weight=1) 
        self.tab_controls.columnconfigure(0, weight=1)

        # --- 1. THE SETTINGS SCROLL AREA (Top) ---
        self.canvas_wrapper = ttk.Frame(self.tab_controls)
        self.canvas_wrapper.grid(row=0, column=0, sticky="nsew") 

        self.controls_canvas = tk.Canvas(self.canvas_wrapper, highlightthickness=0)
        self.controls_scrollbar = ttk.Scrollbar(self.canvas_wrapper, orient="vertical", command=self.controls_canvas.yview)
        self.inner_settings_frame = ttk.Frame(self.controls_canvas, padding=10)
        
        self.canvas_window = self.controls_canvas.create_window((0, 0), window=self.inner_settings_frame, anchor="nw")
        self.controls_canvas.configure(yscrollcommand=self.controls_scrollbar.set)
        
        self.controls_scrollbar.pack(side="right", fill="y")
        self.controls_canvas.pack(side="left", fill="both", expand=True)
        
        # --- 2. THE ACTION & LOG AREA (Bottom) ---
        # This frame now has weight=1, so it will "push" upwards against the settings.
        self.bottom_anchor = ttk.Frame(self.tab_controls)
        self.bottom_anchor.grid(row=1, column=0, sticky="nsew")

        # Action Buttons - Packed at the TOP of the bottom frame
        self.action_frame = ttk.Frame(self.bottom_anchor, padding=(10, 5, 10, 5))
        self.action_frame.pack(side="top", fill="x")
        
        self.btn_download = ttk.Button(self.action_frame, text="⬇️ Start Download", command=self.start_download_thread, state="disabled")
        self.btn_download.pack(fill="x", pady=(0, 2))
        
        sub_btn_frame = ttk.Frame(self.action_frame)
        sub_btn_frame.pack(fill="x")
        ttk.Button(sub_btn_frame, text="🚫 Clear", command=self.clear_all).pack(side="left", expand=True, fill="x", padx=(0, 2))
        ttk.Button(sub_btn_frame, text="📦 Sync", command=self.sync_map_offline).pack(side="left", expand=True, fill="x", padx=(2, 0))

        # Console Log - Packed with expand=True to fill the remaining void
        self.log_frame = ttk.Frame(self.bottom_anchor, padding=(10, 0, 10, 10))
        self.log_frame.pack(side="top", fill="both", expand=True)
        
        self.console = tk.Text(self.log_frame, wrap="word", height=1, font=("Consolas", 8), 
                               state="disabled", bg="#f4f4f4", borderwidth=1, relief="solid")
        self.console.pack(fill="both", expand=True)

        # --- Mandatory Resize Handlers ---
        def auto_resize_canvas(event):
            # Tell the scrollbar how big the content is
            self.controls_canvas.configure(scrollregion=self.controls_canvas.bbox("all"))
            # Force the canvas to be exactly as tall as the 4 frames so there's NO internal gap
            self.controls_canvas.config(height=self.inner_settings_frame.winfo_reqheight())

        self.inner_settings_frame.bind("<Configure>", auto_resize_canvas)
        self.controls_canvas.bind("<Configure>", lambda e: self.controls_canvas.itemconfig(self.canvas_window, width=e.width))

        def _on_mousewheel(event):
            self.controls_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        self.canvas_wrapper.bind("<Enter>", lambda e: self.controls_canvas.bind_all("<MouseWheel>", _on_mousewheel))
        self.canvas_wrapper.bind("<Leave>", lambda e: self.controls_canvas.unbind_all("<MouseWheel>"))

        # Section 1: GEE Connection 
        f1 = ttk.LabelFrame(self.inner_settings_frame, text=" 1. GEE Connection ", padding=2)
        f1.pack(fill="x", pady=2)
        f1_top = ttk.Frame(f1)
        f1_top.pack(fill="x")
        self.project_dropdown = ttk.Combobox(f1_top, textvariable=self.project_id_var, values=self.saved_projects)
        self.project_dropdown.pack(side="left", fill="x", expand=True, padx=(0, 5))
        ttk.Button(f1_top, text="Connect", command=self.run_authentication).pack(side="right")
        self.lbl_status = ttk.Label(f1, text="Status: Ready", foreground="gray")
        self.lbl_status.pack(anchor="w", pady=(2, 0))

        # Section 2: Area of Interest 
        f2 = ttk.LabelFrame(self.inner_settings_frame, text=" 2. Area of Interest ", padding=2)
        f2.pack(fill="x", pady=2)
        f2_top = ttk.Frame(f2)
        f2_top.pack(fill="x")
        # THE UPDATE: We create the entry and give it an initial grey placeholder
        self.aoi_entry = ttk.Entry(f2_top, textvariable=self.input_file_path, foreground="grey")
        self.aoi_entry.pack(side="left", fill="x", expand=True, padx=(0, 5))
        
        # Set initial placeholder text
        self.input_file_path.set("-- Browse .SHP / .GeoJson / .TIFF --")

        ttk.Button(f2_top, text="Browse", command=self.browse_file, width=8).pack(side="right")

        # Track changes to the path to toggle the placeholder behavior
        self.input_file_path.trace_add("write", self._handle_aoi_placeholder)

        # THE FIX: Assign the widget to self.btn_manual so handle_manual_click can find it
        self.btn_manual = tk.Checkbutton(f2, text="Manual Annotate Mode", variable=self.draw_mode_active, 
                                        indicatoron=False, selectcolor="#caf0f8", font=("Arial", 8, "bold"))
        self.btn_manual.pack(fill="x", pady=(5, 0))


        # Section 3: Dataset & Time-Series Settings
        f3 = ttk.LabelFrame(self.inner_settings_frame, text=" 3. Dataset & Temporal Settings ", padding=5)
        f3.pack(fill="x", pady=(0, 5))

        # Dataset Selection
        self.dataset_dropdown = ttk.Combobox(f3, textvariable=self.dataset_var, 
                                            values=["Sentinel-2 (Cloud-Masked RGB)", "Landsat 8/9 (Surface Reflectance)", "Sentinel-1 (VV & VH Radar)", "SRTM Elevation (DEM)"], 
                                            state="readonly")
        self.dataset_dropdown.pack(fill="x", pady=(0, 5))

        # --- NEW: Time-Series Toggle ---
        self.timeseries_mode = tk.BooleanVar(value=False)
        ts_toggle = tk.Checkbutton(f3, text="📅 Enable Time-Series Mode (Batch Download)", 
                                   variable=self.timeseries_mode, font=("Arial", 8, "bold"), 
                                   command=self._toggle_ts_ui, fg="#0077b6")
        ts_toggle.pack(anchor="w", pady=2)

        # --- Dynamic Date Container ---
        self.date_container = ttk.Frame(f3)
        self.date_container.pack(fill="x", pady=2)
        
        # We start by initializing the "Single Date" UI inside the container
        self._setup_single_date_ui()

        # Cloud Settings
        self.sort_by_cloud_var = tk.BooleanVar(value=False)
        self.cloud_sort_check = tk.Checkbutton(f3, text="Sort by Lowest Cloud Cover", variable=self.sort_by_cloud_var, font=("Arial", 8))
        self.cloud_sort_check.pack(anchor="w", pady=2)

        self.available_dates_dropdown = ttk.Combobox(f3, textvariable=self.selected_date_var, state="readonly")
        self.available_dates_dropdown.pack(fill="x", pady=(0, 5))

        self.band_label = ttk.Label(f3, text="Spectral Bands:", font=("Arial", 8, "bold"))
        self.band_label.pack(anchor="w")
        self.band_inner_frame = ttk.Frame(f3)
        self.band_inner_frame.pack(fill="x", pady=2)

        # --- Updated Merge Tiles Section with Help Icon ---
        # --- Updated Merge Tiles & Settings Section ---
        stitch_row = ttk.Frame(f3)
        stitch_row.pack(anchor="w", pady=(5, 0), fill="x", padx=5) 

        # 1. Merge Tiles Checkbox
        self.stitch_check = tk.Checkbutton(stitch_row, text="Merge Tiles into Single File", 
                                           variable=self.stitch_tiles_var, 
                                           font=("Arial", 8, "bold"), 
                                           fg="#d62828")
        self.stitch_check.pack(side="left")

        # 2. Merge Tiles Help Icon
        stitch_help = tk.Label(stitch_row, text="ⓘ", font=("Arial", 10), fg="#0077b6", cursor="question_arrow")
        stitch_help.pack(side="left", padx=(0, 10)) 
        self.create_tooltip(stitch_help, 
            "GEE downloads large areas in multiple 'tiles' to avoid memory errors. \n"
            "If enabled, GEE2DB will automatically stitch these tiles back into \n"
            "one seamless .TIF file once the download finishes.")

        # 3. Keep Original Tiles Checkbox
        self.keep_tiles_var = tk.BooleanVar(value=True) 
        self.keep_tiles_check = tk.Checkbutton(stitch_row, text="Keep Original Tiles", 
                                           variable=self.keep_tiles_var, 
                                           font=("Arial", 8))
        self.keep_tiles_check.pack(side="left")

        # Section 4: Coordinate Reference System (CRS)
        f4 = ttk.LabelFrame(self.inner_settings_frame, text=" 4. Coordinate Reference System (CRS) ", padding=5)
        f4.pack(fill="x", pady=(0, 5))

        f4_inner = ttk.Frame(f4)
        f4_inner.pack(fill="x", padx=2, pady=2)

        ttk.Label(f4_inner, text="Target Projection:").pack(side="left", padx=(0, 2))
        
        # The "?" Help Icon
        help_icon = tk.Label(f4_inner, text="ⓘ", font=("Arial", 10, "bold"), fg="#0077b6", cursor="question_arrow")
        help_icon.pack(side="left", padx=(0, 5))
        
        # Tooltip Binding
        self.create_tooltip(help_icon, "This is automatically determined by the GEE Sensor pass or \nthe CRS of your loaded shapefile/GeoJSON.")

        self.crs_dropdown = ttk.Combobox(f4_inner, textvariable=self.target_crs_var, 
                                        values=["EPSG:4326 (WGS84 Lat/Lon)", "EPSG:3857 (Web Mercator)", "EPSG:32651 (UTM Zone 51N - PH)"], 
                                        state="readonly")
        self.crs_dropdown.pack(side="left", fill="x", expand=True)

        # ==========================================
        # --- TAB 2: SEARCH ---
        # ==========================================
        self.tab_search = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_search, text=" 🔍 Search ")

        s_frame = ttk.LabelFrame(self.tab_search, text=" Find Location or Coordinates ", padding=10)
        s_frame.pack(fill="x", pady=5)

        ttk.Label(s_frame, text="Search by Name (e.g., Metro Manila):").pack(anchor="w")
        self.search_entry = ttk.Entry(s_frame)
        self.search_entry.pack(fill="x", pady=5)
        self.search_entry.bind("<Return>", lambda e: self.run_location_search())
        ttk.Button(s_frame, text="Search Location", command=self.run_location_search).pack(fill="x", pady=2)
        
        self.search_listbox = tk.Listbox(s_frame, height=5, exportselection=False, font=("Arial", 8))
        self.search_listbox.pack(fill="x", pady=5)
        self.search_listbox.bind("<<ListboxSelect>>", self.on_search_select)
        self.current_search_results = [] 

        ttk.Separator(s_frame, orient="horizontal").pack(fill="x", pady=15)

        ttk.Label(s_frame, text="Jump to Coordinates:").pack(anchor="w")
        coord_f = ttk.Frame(s_frame)
        coord_f.pack(fill="x", pady=5)
        # --- Polished Entry UI ---
        self.lat_entry = ttk.Entry(coord_f, foreground="grey")
        self.lat_entry.insert(0, "Latitude")
        self.lat_entry.pack(side="left", expand=True, fill="x", padx=(0, 2))
        
        self.lon_entry = ttk.Entry(coord_f, foreground="grey")
        self.lon_entry.insert(0, "Longitude")
        self.lon_entry.pack(side="left", expand=True, fill="x", padx=(2, 0))

        # Lambda functions to clear the "Latitude/Longitude" hints on click
        self.lat_entry.bind("<FocusIn>", lambda e: self._clear_coord_hint(self.lat_entry, "Latitude"))
        self.lon_entry.bind("<FocusIn>", lambda e: self._clear_coord_hint(self.lon_entry, "Longitude"))
        ttk.Button(s_frame, text="Jump to Coordinates", command=self.jump_to_coords).pack(fill="x", pady=5)

        # --- Search History Section ---
        h_frame = ttk.LabelFrame(self.tab_search, text=" Recent Search History ", padding=10)
        h_frame.pack(fill="both", expand=True, pady=5)

        self.history_listbox = tk.Listbox(h_frame, height=8, font=("Arial", 8), bg="#fcfcfc")
        self.history_listbox.pack(side="left", fill="both", expand=True)
        self.history_listbox.bind("<Double-1>", lambda e: self.jump_from_history())
        
        h_scroll = ttk.Scrollbar(h_frame, orient="vertical", command=self.history_listbox.yview)
        h_scroll.pack(side="right", fill="y")
        self.history_listbox.config(yscrollcommand=h_scroll.set)

        # Buttons for History
        h_btn_frame = ttk.Frame(self.tab_search)
        h_btn_frame.pack(fill="x")
        ttk.Button(h_btn_frame, text="🗑️ Clear History", command=self.clear_search_history).pack(side="right")
        ttk.Button(h_btn_frame, text="📍 Jump to Selected", command=self.jump_from_history).pack(side="right", padx=5)

        # Populate initially
        for item in self.search_history:
            self.history_listbox.insert(tk.END, item['address'])
        
        self.history_listbox.bind("<Double-1>", lambda e: self.jump_from_history())

        # ==========================================
        # --- TAB 3: TASK MANAGER ---
        # ==========================================
        self.tab_downloads = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_downloads, text=" 📥 Tasks ")

        # --- NEW: Action Buttons Frame ---
        task_btn_frame = ttk.Frame(self.tab_downloads)
        task_btn_frame.pack(fill="x", pady=(0, 5))
        
        # Stop Button
        self.btn_cancel_dl = ttk.Button(task_btn_frame, text="🛑 Cancel Download", state="disabled", command=self.cancel_download)
        self.btn_cancel_dl.pack(side="right", padx=(5, 0))
        
        # Refresh Button
        ttk.Button(task_btn_frame, text="🔄 Refresh File Status", command=self.load_records_from_file).pack(side="right")
        
        ttk.Label(self.tab_downloads, text="Real-time Progress:", font=("Arial", 9, "bold")).pack(anchor="w")
        self.progress_bar = ttk.Progressbar(self.tab_downloads, orient="horizontal", mode="determinate")
        self.progress_bar.pack(fill="x", pady=10)
        self.lbl_progress_detail = ttk.Label(self.tab_downloads, text="No active tasks.")
        self.lbl_progress_detail.pack(anchor="w", pady=(0, 10))

        # --- NEW: Naming Convention Tooltip (Tasks Tab) ---
        naming_f1 = ttk.Frame(self.tab_downloads)
        naming_f1.pack(anchor="w", pady=(0, 2))
        ttk.Label(naming_f1, text="File Naming Convention:", font=("Arial", 8, "italic")).pack(side="left")
        naming_help1 = tk.Label(naming_f1, text="ⓘ", font=("Arial", 10), fg="#0077b6", cursor="question_arrow")
        naming_help1.pack(side="left", padx=5)
        self.create_tooltip(naming_help1, 
            "Files are automatically named using this structure:\n\n"
            "[Mode]_[Sensor]_Area_[Date]_[Bands]_[DownloadTime]\n\n"
            "• Mode: 'Mono' (Single Date) or 'TS' (Batch)\n"
            "• Sensor: 'S2' (Sentinel-2), 'L89' (Landsat), etc.\n"
            "• Area: Default project prefix (renameable in the save dialog)\n"
            "• Date: The target satellite pass date\n"
            "• Bands: Output layers (e.g., 'RGB-NDVI')\n"
            "• Time: YYYYMMDD_HHMMSS (Exact time of download)")

        table_container = ttk.Frame(self.tab_downloads)
        table_container.pack(fill="both", expand=True)

        # --- Optimized Task Manager Columns ---
        columns = ("status", "date", "file", "dataset", "crs", "bounds", "path")
        # We increase 'file' and 'date' widths to handle Time-Series strings
        column_widths = {"status": 75, "date": 140, "file": 300, "dataset": 180, "crs": 90, "bounds": 180, "path": 400}
        
        self.record_table = ttk.Treeview(table_container, columns=columns, show="headings")
        for col in columns: 
            self.record_table.heading(col, text=col.title())
            self.record_table.column(col, width=column_widths.get(col, 100), anchor="center" if col=="status" else "w")
            
        v_scroll = ttk.Scrollbar(table_container, orient="vertical", command=self.record_table.yview)
        h_scroll = ttk.Scrollbar(self.tab_downloads, orient="horizontal", command=self.record_table.xview)
        self.record_table.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        
        v_scroll.pack(side="right", fill="y")
        self.record_table.pack(side="left", fill="both", expand=True)
        h_scroll.pack(side="bottom", fill="x")

        self.record_table.bind("<Double-1>", self.on_record_double_click)
        self.record_table.bind("<Button-3>", self.show_table_context_menu)

        # ==========================================
        # --- TAB 4: LAYERS ---
        # ==========================================
        self.tab_layers = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_layers, text=" 🗂️Layers ")

        l_btn_frame = ttk.Frame(self.tab_layers)
        l_btn_frame.pack(fill="x", pady=(0, 5))

        ttk.Button(l_btn_frame, text="🔄 Refresh Workspace", command=self.populate_layers_tree).pack(fill="x", pady=(0, 5))
        ttk.Button(l_btn_frame, text="🚫 Clear Canvas", command=self.clear_all).pack(side="left", expand=True, fill="x", padx=(2, 0))
        ttk.Label(self.tab_layers, text="Double-click a file to toggle visibility on the map.", font=("Arial", 8, "italic")).pack(anchor="w", pady=2)

        # --- NEW: Naming Convention Tooltip (Layers Tab) ---
        naming_f2 = ttk.Frame(self.tab_layers)
        naming_f2.pack(anchor="w", pady=(0, 5))
        ttk.Label(naming_f2, text="File Naming Convention:", font=("Arial", 8, "italic")).pack(side="left")
        naming_help2 = tk.Label(naming_f2, text="ⓘ", font=("Arial", 10), fg="#0077b6", cursor="question_arrow")
        naming_help2.pack(side="left", padx=5)
        self.create_tooltip(naming_help2, 
            "Files are automatically named using this structure:\n\n"
            "[Mode]_[Sensor]_Area_[Date]_[Bands]_[DownloadTime]\n\n"
            "• Mode: 'Mono' (Single Date) or 'TS' (Batch)\n"
            "• Sensor: 'S2' (Sentinel-2), 'L89' (Landsat), etc.\n"
            "• Area: Default project prefix (renameable in the save dialog)\n"
            "• Date: The target satellite pass date\n"
            "• Bands: Output layers (e.g., 'RGB-NDVI')\n"
            "• Time: YYYYMMDD_HHMMSS (Exact time of download)")

        layers_container = ttk.Frame(self.tab_layers)
        layers_container.pack(fill="both", expand=True)

        self.layers_tree = ttk.Treeview(layers_container, columns=("path",), show="tree headings")
        self.layers_tree.heading("#0", text="File Name", anchor="w")
        self.layers_tree.column("#0", width=350, minwidth=250, stretch=False) 
        self.layers_tree.heading("path", text="Absolute Path", anchor="w")
        self.layers_tree.column("path", width=250, minwidth=100, stretch=False) 

        v_scroll_layers = ttk.Scrollbar(layers_container, orient="vertical", command=self.layers_tree.yview)
        h_scroll_layers = ttk.Scrollbar(layers_container, orient="horizontal", command=self.layers_tree.xview)
        self.layers_tree.configure(yscrollcommand=v_scroll_layers.set, xscrollcommand=h_scroll_layers.set)
        
        h_scroll_layers.pack(side="bottom", fill="x")
        v_scroll_layers.pack(side="right", fill="y")
        self.layers_tree.pack(side="left", fill="both", expand=True)

        self.layers_tree.bind("<ButtonRelease-1>", self.toggle_map_layer)
        self.layers_tree.bind("<Button-3>", self.show_layers_context_menu)

        # ==========================================
        # --- TAB 5: GEODATABASE & DEPLOYMENT ---
        # ==========================================
        self.tab_db = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(self.tab_db, text=" 🗄️ Database & Deploy ")

        # --- 1. Secure Authentication Settings ---
        auth_f = ttk.LabelFrame(self.tab_db, text=" 🔐 PostGIS & GeoServer Credentials ", padding=15)
        auth_f.pack(fill="x", pady=5)

        self.db_host = tk.StringVar(value="localhost")
        self.db_name = tk.StringVar(value="ai4caf_db")
        self.db_user = tk.StringVar(value="postgres")
        self.db_pass = tk.StringVar(value="")

        # PostGIS Left Column
        ttk.Label(auth_f, text="PostGIS Host:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(auth_f, textvariable=self.db_host, width=15).grid(row=0, column=1, sticky="w", padx=5)
        ttk.Label(auth_f, text="PostGIS User:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(auth_f, textvariable=self.db_user, width=15).grid(row=1, column=1, sticky="w", padx=5)
        ttk.Label(auth_f, text="PostGIS Pass:").grid(row=2, column=0, sticky="w", pady=2)
        ttk.Entry(auth_f, textvariable=self.db_pass, show="*", width=15).grid(row=2, column=1, sticky="w", padx=5)

        # GeoServer Right Column
        ttk.Label(auth_f, text="GeoServer User:").grid(row=0, column=2, sticky="w", padx=(20,0), pady=2)
        ttk.Entry(auth_f, textvariable=self.gs_user_var, width=15).grid(row=0, column=3, sticky="w", padx=5)
        ttk.Label(auth_f, text="GeoServer Pass:").grid(row=1, column=2, sticky="w", padx=(20,0), pady=2)
        ttk.Entry(auth_f, textvariable=self.gs_pass_var, show="*", width=15).grid(row=1, column=3, sticky="w", padx=5)

        ttk.Checkbutton(auth_f, text="Store passwords securely in vault.db", variable=self.save_creds_var).grid(row=3, column=0, columnspan=4, pady=10)

        # Load saved credentials instantly
        self.load_stored_credentials()

        self.lbl_db_status = ttk.Label(self.tab_db, text="Status: Disconnected", foreground="gray")
        self.lbl_db_status.pack(pady=2)

        btn_f = ttk.Frame(self.tab_db)
        btn_f.pack(fill="x", pady=5)
        ttk.Button(btn_f, text="Initialize DB", command=self.ui_setup_db).pack(side="left", expand=True, fill="x", padx=2)
        ttk.Button(btn_f, text="🧹 Clean Links", command=self.prune_ghost_records).pack(side="left", expand=True, fill="x", padx=2)

        # --- 2. Deployment & Automation ---
        deploy_f = ttk.LabelFrame(self.tab_db, text=" 🚀 GeoServer API Deployment ", padding=10)
        deploy_f.pack(fill="x", pady=10)

        ttk.Button(deploy_f, text="1. Sync AI4CAF to GeoServer", command=self.run_full_deployment).pack(fill="x", pady=2)
        
        # NEW: Dropdown menu for individual layers
        ttk.Label(deploy_f, text="Select Layer to View:").pack(anchor="w", pady=(5, 0))
        self.layer_dropdown = ttk.Combobox(deploy_f, state="readonly")
        self.layer_dropdown.pack(fill="x", pady=(0, 5))

        ttk.Button(deploy_f, text="2. Open Live Web Map (Satellite Base)", command=self.open_web_map).pack(fill="x", pady=2)

        # ==========================================
        # --- RIGHT PANEL: THE MAP ---
        # ==========================================
        script_directory = os.path.dirname(os.path.abspath(__file__))
        database_path = os.path.join(script_directory, "offline_tiles.db")

        self.map_widget = tkintermapview.TkinterMapView(
            self.right_frame, 
            corner_radius=0, 
            database_path=database_path,
            bg_color="#1a1a1a"
        )
        self.map_widget.canvas.configure(bg="#1a1a1a") 
        self.map_widget.set_tile_server("https://mt1.google.com/vt/lyrs=y&hl=en&x={x}&y={y}&z={z}", max_zoom=20)
        self.map_widget.pack(fill="both", expand=True)

        self.map_widget.canvas.bind("<ButtonRelease-1>", self.refresh_label_visibility, add="+")
        self.map_widget.canvas.bind("<MouseWheel>", lambda e: self.root.after(300, self.refresh_label_visibility), add="+")

        self.label_coords = tk.Label(self.map_widget, text="Lat: 0.0000, Lon: 0.0000", bg="black", fg="white", font=("Consolas", 10, "bold"))
        self.label_coords.place(relx=0.01, rely=0.98, anchor="sw")
        self.map_widget.canvas.bind('<Motion>', self.track_movement)
        self.map_widget.add_right_click_menu_command(label="Mark Corner", command=self.handle_manual_click, pass_coords=True)
        self.map_widget.set_position(12.87, 121.77); self.map_widget.set_zoom(6)

        # Dynamic AOI Bounding Box HUD
        self.aoi_bounds_label = tk.Label(self.map_widget, text="No AOI Drawn", bg="#333333", fg="gray", 
                                         font=("Consolas", 9, "bold"), justify="center", relief="solid", bd=1, padx=5, pady=5)
        # Placed in the bottom-right corner
        self.aoi_bounds_label.place(relx=0.98, rely=0.98, anchor="se")

        # Dynamic AOI Bounding Box HUD
        self.aoi_bounds_label = tk.Label(self.map_widget, text="No AOI Drawn", bg="#333333", fg="gray", 
                                         font=("Consolas", 9, "bold"), justify="center", relief="solid", bd=1, padx=5, pady=5)
        # Placed in the bottom-right corner
        self.aoi_bounds_label.place(relx=0.98, rely=0.98, anchor="se")

        # --- NEW: Floating Camera Snapshot Button ---
        self.btn_snapshot = tk.Button(self.map_widget, text="📸", bg="#1a1a1a", fg="#00ffff", 
                                      font=("Segoe UI", 9, "bold"), relief="solid", bd=1, cursor="hand2", 
                                      padx=10, pady=5, command=self.take_map_snapshot)
        # Placed in the top-right corner
        self.btn_snapshot.place(relx=0.98, rely=0.02, anchor="ne")

        # High-Fidelity Preview Layer
        self.preview_container = ttk.Frame(self.right_frame)
        self.preview_label = tk.Label(self.preview_container, bg="#1e1e1e")
        self.preview_label.pack(fill="both", expand=True)
        self.preview_label.bind("<Motion>", self.show_pixel_metadata)
        self.inspector_label = tk.Label(self.preview_container, text="Hover to inspect", bg="black", fg="#00ff00")
        self.inspector_label.place(relx=0.02, rely=0.02, anchor="nw")
        ttk.Button(self.preview_container, text="✖ Close", command=self.close_preview).place(relx=0.98, rely=0.02, anchor="ne")

        # --- THE ADOBE SPLASH CARD ---
        self.splash_card = tk.Frame(self.root, bg="white", padx=40, pady=40, highlightbackground="#00a8e8", highlightthickness=2)
        tk.Label(self.splash_card, text="GEE2DB", fg="#00a8e8", bg="white", font=("Segoe UI", 28, "bold")).pack()
        self.lock_label = tk.Label(self.splash_card, text="Optimizing Spatial Engine...", fg="#333333", bg="white", font=("Segoe UI", 10))
        self.lock_label.pack(pady=(5, 20))
        
        style = ttk.Style()
        style.configure("Splash.Horizontal.TProgressbar", background='#00a8e8', thickness=6)
        self.lock_progress = ttk.Progressbar(self.splash_card, orient="horizontal", length=350, mode="indeterminate", style="Splash.Horizontal.TProgressbar")
        self.lock_progress.pack(pady=5)

    def prune_ghost_records(self):
        """Deletes database rows where the physical .tif file is missing from the disk."""
        if not messagebox.askyesno("Confirm Prune", "This will scan your disk and delete database records that have no matching .tif file. Proceed?"):
            return

        def run_prune():
            try:
                # 1. Get all records from the DB
                with psycopg2.connect(**self.db_manager.params) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id, file_path FROM satellite_inventory;")
                        records = cur.fetchall()
                        
                        deleted_count = 0
                        script_dir = os.path.dirname(os.path.abspath(__file__))
                        data_root = os.path.join(script_dir, "Data")

                        for db_id, db_path in records:
                            # Reconstruct the full path to check if it exists
                            # If it's a relative path starting with 'Project_...', we join it with Data root
                            full_check_path = db_path if os.path.isabs(db_path) else os.path.join(data_root, db_path)
                            
                            if not os.path.exists(full_check_path):
                                # 2. If file is missing, DELETE the row
                                cur.execute("DELETE FROM satellite_inventory WHERE id = %s;", (db_id,))
                                deleted_count += 1
                
                self.log(f"Database Maintenance: Removed {deleted_count} ghost records.")
                messagebox.showinfo("Prune Complete", f"Cleaned up {deleted_count} records with missing files.")
            except Exception as e:
                self.log(f"Prune Error: {e}")

        threading.Thread(target=run_prune, daemon=True).start()

    def ui_migrate_paths(self):
        new_root = self.nas_root_var.get().strip()
        if not new_root: return
        
        if not messagebox.askyesno("Confirm Migration", f"This will rewrite all file paths in 'ai4caf_db' to point to {new_root}.\n\nProceed?"):
            return

        try:
            # We look for paths that don't start with / or // (meaning they are relative)
            # and prepend the new NAS root.
            query = "UPDATE satellite_inventory SET file_path = %s || file_path WHERE file_path NOT LIKE '//%' AND file_path NOT LIKE '/%';"
            
            with psycopg2.connect(**self.db_manager.params) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (new_root,))
                conn.commit()
            
            self.log(f"Migration: All database records updated to NAS root: {new_root}")
            messagebox.showinfo("Migration Complete", "Database paths are now finalized for NAS/Dashboard use.")
        except Exception as e:
            messagebox.showerror("Migration Error", str(e))

    def ui_test_db(self):
        self.db_manager.update_params(self.db_host.get(), self.db_name.get(), self.db_user.get(), self.db_pass.get())
        success, msg = self.db_manager.test_connection()
        if success:
            self.lbl_db_status.config(text="✅ Connected to PostGIS", foreground="#2a9d8f")
            self.log("Database: Connection Verified.")
        else:
            self.lbl_db_status.config(text=f"❌ Error: {msg[:30]}...", foreground="#d62828")
            messagebox.showerror("DB Error", msg)

    def ui_setup_db(self):
        # 1. First, pull the latest text from your UI input fields
        self.db_manager.update_params(
            self.db_host.get(), 
            self.db_name.get(), 
            self.db_user.get(), 
            self.db_pass.get()
        )
        
        # 2. Now run the setup
        if self.db_manager.setup_tables():
            self.lbl_db_status.config(text="✅ Database Initialized", foreground="#2a9d8f")
            messagebox.showinfo("Success", "PostGIS table 'satellite_inventory' is ready.")
            self.log("Database: Tables Initialized with UI credentials.")
        else:
            messagebox.showerror("Error", "Initialization failed. Check your Password and User in the Database tab.")

    def load_stored_credentials(self):
        """Loads decrypted passwords into the UI if they exist."""
        try:
            gs_user = self.vault.retrieve("GS_USER")
            gs_pass = self.vault.retrieve("GS_PASS")
            db_pass = self.vault.retrieve("DB_PASS")

            if gs_user: self.gs_user_var.set(gs_user)
            if gs_pass: self.gs_pass_var.set(gs_pass)
            if db_pass: self.db_pass.set(db_pass)
            
            if gs_user or db_pass:
                self.save_creds_var.set(True)
                self.log("📂 Credentials loaded securely from vault.db")
        except Exception as e:
            self.log(f"Vault Load Error: {e}")

    def run_full_deployment(self):
        """Saves credentials (if checked) and triggers the API sync."""
        if self.save_creds_var.get():
            self.vault.store("GS_USER", self.gs_user_var.get())
            self.vault.store("GS_PASS", self.gs_pass_var.get())
            self.vault.store("DB_PASS", self.db_pass.get())
            self.log("🔒 Credentials safely encrypted to vault.db")
        
        # Test Database first
        self.ui_test_db()
        
        # Then sync GeoServer
        threading.Thread(target=self.sync_to_geoserver, daemon=True).start()

    def apply_geoserver_style(self, layer_name):
        """Forcefully applies the dynamic enhanced style and recalculates statistics on GeoServer."""
        import requests
        from requests.auth import HTTPBasicAuth
        
        # ⚠️ Make sure these match your login settings frame in the UI
        gs_user = self.gs_user_var.get()
        gs_pass = self.gs_pass_var.get()
        if not gs_user or not gs_pass: return

        gs_base = "http://localhost:8080/geoserver/rest"
        auth = HTTPBasicAuth(gs_user, gs_pass)
        ws = "ai4caf"
        style_name = "sentinel_dynamic_enhance"

        try:
            # 1. Define the Style in GeoServer (XML structure)
            style_def = f"<style><name>{style_name}</name><filename>{style_name}.sld</filename></style>"
            
            # Post the style definition to create the style entry (ignoring 403/500 if it already exists)
            requests.post(f"{gs_base}/workspaces/{ws}/styles", 
                          data=style_def, headers={'Content-type': 'text/xml'}, auth=auth)

            # 2. Upload the SLD body (Force update/overwrite the colors)
            resp = requests.put(f"{gs_base}/workspaces/{ws}/styles/{style_name}", 
                         data=SENTINEL_SLD, 
                         headers={'Content-type': 'application/vnd.ogc.sld+xml'}, auth=auth)
            
            if resp.status_code != 200:
                self.log(f"SLD Upload failed: {resp.status_code}")
                return

            # 🚀 3. THE MAGIC STEP: Re-Calculate Statistics
            # We must tell GeoServer to update its XML about the coverage store 
            # so the StretchToMinimumMaximum algorithm knows the true min/max.
            # Failure to do this often results in blank layers!
            requests.post(f"{gs_base}/workspaces/{ws}/coveragestores/{layer_name}/coverages/{layer_name}.xml?recalculate=nativecoverage,latloncoverage", auth=auth)

            # 4. CRITICAL: Link this beautiful dynamic style to the layer
            link_xml = f"<layer><defaultStyle><name>{style_name}</name></defaultStyle></layer>"
            requests.put(f"{gs_base}/layers/{ws}:{layer_name}", 
                                data=link_xml, headers={'Content-type': 'text/xml'}, auth=auth)
            
            self.log(f"✅ Dynamic style applied to {layer_name}")
            
        except Exception as e:
            self.log(f"SLD Fatal Error: {str(e)}")

    def sync_to_geoserver(self):
        """High-Speed Visual Sync: Uploads previews and UPDATES the UI dropdown."""
        import requests
        from requests.auth import HTTPBasicAuth
        import os, re, threading
        import numpy as np
        import rasterio

        gs_url = "http://localhost:8080/geoserver/rest"
        auth = HTTPBasicAuth(self.gs_user_var.get(), self.gs_pass_var.get())
        ws = "ai4caf"

        self.log("🚀 Starting Optimized Visual Sync...")

        def process_and_upload():
            synced_layers = [] # 🚀 Collect names for the dropdown
            try:
                # Get all TIF paths from the layers tree
                paths_to_process = []
                def get_all_file_paths(node=""):
                    for child in self.layers_tree.get_children(node):
                        vals = self.layers_tree.item(child, "values")
                        if vals and vals[0].lower().endswith('.tif'): paths_to_process.append(vals[0])
                        else: get_all_file_paths(child)
                get_all_file_paths()

                # Ensure Workspace
                requests.post(f"{gs_url}/workspaces", data=f"<workspace><name>{ws}</name></workspace>", headers={'Content-type': 'text/xml'}, auth=auth)

                for tif_path in paths_to_process:
                    file_name = os.path.basename(tif_path)
                    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', os.path.splitext(file_name)[0])
                    
                    # Create 8-bit visual proxy
                    visual_path = tif_path.replace(".tif", "_tmp_v.tif")
                    with rasterio.open(tif_path) as src:
                        meta = src.meta.copy()
                        meta.update(dtype='uint8', count=3, compress='lzw')
                        with rasterio.open(visual_path, 'w', **meta) as dst:
                            for b_idx in range(1, 4):
                                band = src.read(b_idx)
                                # Fast approximate stretch for web
                                visual_band = np.clip(band / 11.7, 0, 255).astype(np.uint8)
                                dst.write(visual_band, b_idx)

                    # Upload to GeoServer
                    store_url = f"{gs_url}/workspaces/{ws}/coveragestores/{safe_name}"
                    requests.delete(f"{store_url}?recurse=true", auth=auth)
                    
                    with open(visual_path, 'rb') as f:
                        resp = requests.put(f"{store_url}/file.geotiff", data=f, headers={'Content-type': 'image/tiff'}, auth=auth)
                    
                    if os.path.exists(visual_path): os.remove(visual_path)
                    
                    if resp.status_code in [200, 201]:
                        synced_layers.append(safe_name) # 🚀 Add to success list
                        self.root.after(0, lambda n=file_name: self.log(f"✅ Synced: {n}"))

                # 🚀 UPDATE THE DROPDOWN HERE
                if synced_layers:
                    self.root.after(0, lambda: self.update_dropdown(synced_layers))
                
                self.root.after(0, lambda: self.log("🏁 Sync Complete! Dropdown updated."))
                
            except Exception as e:
                self.root.after(0, lambda err=str(e): self.log(f"❌ Sync Error: {err}"))

        threading.Thread(target=process_and_upload, daemon=True).start()

    def update_dropdown(self, layers):
        """Helper to safely update the dropdown in the main thread."""
        self.layer_dropdown['values'] = layers
        self.layer_dropdown.current(0) # Select the first one by default

    def open_web_map(self):
        """Generates an interactive HTML dashboard using Leaflet and opens it in your browser."""
        import os
        import webbrowser
        import pathlib

        # 1. Grab all the layers currently loaded in your dropdown
        # NOTE: Change 'self.layer_dropdown' to whatever your Combobox variable is actually named!
        layers = self.layer_dropdown['values'] 

        if not layers:
            self.root.after(0, lambda: messagebox.showwarning("Empty", "No layers available. Sync first!"))
            return

        self.root.after(0, lambda: self.log(f"🌍 Generating Interactive Dashboard with {len(layers)} layers..."))

        # The WMS endpoint for your specific workspace
        gs_wms_url = "http://localhost:8080/geoserver/ai4caf/wms"

        # 2. Start building the HTML file with Leaflet.js
        html_content = f"""<!DOCTYPE html>
        <html>
        <head>
            <title>AI4CAF Interactive Dashboard</title>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
            <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
            <style>
                body {{ padding: 0; margin: 0; }}
                #map {{ height: 100vh; width: 100vw; }}
            </style>
        </head>
        <body>
            <div id="map"></div>
            <script>
                // Initialize the map (Centered roughly on the Philippines)
                var map = L.map('map').setView([12.8797, 121.7740], 5);

                // Add a beautiful, clean base map
                var cartoDB = L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
                    attribution: '&copy; OpenStreetMap &copy; CARTO'
                }}).addTo(map);

                var baseMaps = {{
                    "Base Map": cartoDB
                }};

                var overlayMaps = {{}};
        """

        # 3. Dynamically inject every layer with high-compatibility settings
        for idx, layer_name in enumerate(layers):
            js_var = f"layer_{idx}"
            html_content += f"""
                var {js_var} = L.tileLayer.wms('{gs_wms_url}', {{
                    layers: 'ai4caf:{layer_name}',
                    format: 'image/png',
                    transparent: true,
                    version: '1.1.1',
                    crs: L.CRS.EPSG4326,
                    uppercase: true 
                }});
                overlayMaps["{layer_name}"] = {js_var};
            """
            
            # Auto-turn on the very first layer in the list so the map isn't blank
            if idx == 0:
                html_content += f"{js_var}.addTo(map);\n"

        # 4. Finish the HTML by adding the Checkbox Control Panel
        html_content += """
                // Add the layer control menu to the top right
                L.control.layers(baseMaps, overlayMaps, {collapsed: false}).addTo(map);
            </script>
        </body>
        </html>
        """

        # 5. Save this HTML to a real file and command Chrome/Edge to open it
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            dashboard_path = os.path.join(script_dir, "dashboard.html")
            
            with open(dashboard_path, "w", encoding="utf-8") as f:
                f.write(html_content)

            # Safely format the file path for the web browser
            file_uri = pathlib.Path(dashboard_path).as_uri()
            webbrowser.open(file_uri)
            
            self.root.after(0, lambda: self.log("✅ Dashboard opened in your web browser!"))
            
        except Exception as e:
            self.root.after(0, lambda err=str(e): self.log(f"❌ Failed to create map: {err}"))

    def take_map_snapshot(self):
        """Captures the map canvas with pixel-perfect accuracy using modern DPI scaling detection."""
        try:
            from PIL import ImageGrab
            import ctypes
            import os
            
            # 1. SETUP DIRECTORY
            script_dir = os.path.dirname(os.path.abspath(__file__))
            snapshot_dir = os.path.join(script_dir, "Snapshots")
            os.makedirs(snapshot_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_path = filedialog.asksaveasfilename(
                title="Save Map Snapshot", 
                initialdir=snapshot_dir, 
                initialfile=f"Map_Snapshot_{timestamp}.png",
                filetypes=[("PNG Image", "*.png"), ("JPEG", "*.jpg")]
            )
            
            if not save_path: return

            # 2. ACCURATE SCALING DETECTION
            # We query the specific scale factor for the primary monitor (100, 125, 150, etc.)
            try:
                # 0 = Primary Monitor
                scale_percent = ctypes.windll.shcore.GetScaleFactorForDevice(0) 
                scale_factor = scale_percent / 100.0
            except Exception:
                # Fallback if the above fails
                hdc = ctypes.windll.user32.GetDC(0)
                scale_factor = ctypes.windll.gdi32.GetDeviceCaps(hdc, 88) / 96.0
                ctypes.windll.user32.ReleaseDC(0, hdc)

            self.log(f"Snapshot: Detected UI Scale {int(scale_factor*100)}%")

            # 3. PREPARE UI
            self.root.lift()
            self.root.update()
            self.root.after(300) 

            # 4. CALCULATE CANVAS BOUNDS
            # We target the actual internal canvas of the map_widget
            canvas = self.map_widget.canvas
            
            # Root coordinates relative to the screen
            x = canvas.winfo_rootx()
            y = canvas.winfo_rooty()
            w = canvas.winfo_width()
            h = canvas.winfo_height()

            # Apply scaling factor
            # Sometimes winfo_rootx adds a tiny offset for window borders; 
            # we use int() to ensure we stay on pixel boundaries.
            left = int(x * scale_factor)
            top = int(y * scale_factor)
            right = int((x + w) * scale_factor)
            bottom = int((y + h) * scale_factor)

            # 5. GRAB AND SAVE
            bbox = (left, top, right, bottom)
            img = ImageGrab.grab(bbox=bbox, all_screens=True) # all_screens helps with multi-monitor setups
            img.save(save_path)
            
            self.log(f"📸 Snapshot saved: {os.path.basename(save_path)}")
            messagebox.showinfo("Snapshot Complete", f"Saved to Snapshots folder.\nScale: {int(scale_factor*100)}%")
            
        except Exception as e:
            self.log(f"Snapshot Error: {e}")
            messagebox.showerror("Capture Error", f"Failed to take snapshot:\n{e}")

    def cancel_download(self):
        """Signals the background thread to safely abort the current download sequence."""
        if messagebox.askyesno("Cancel Download", "Are you sure you want to stop the ongoing download?"):
            self.cancel_download_flag = True
            self.btn_cancel_dl.config(state="disabled", text="Stopping...")
            self.log("🛑 Cancellation requested. Finishing current tile before stopping...")

    def _open_calendar_picker(self, target_variable):
        """Creates a modern popup calendar and FORCES the date into the field."""
        cal_win = tk.Toplevel(self.root)
        cal_win.title("Select Date")
        cal_win.geometry("300x320") 
        cal_win.grab_set() # Keep focus on the calendar
        cal_win.attributes("-topmost", True) # Keep it on top of other windows

        # 1. INITIAL DATE LOGIC
        # Try to read the current date from the field so the calendar opens on it
        current_val = target_variable.get()
        try:
            dt = datetime.strptime(current_val[:10], "%Y-%m-%d")
            y, m, d = dt.year, dt.month, dt.day
        except:
            # Default to today if the field is empty or has a placeholder
            now = datetime.now()
            y, m, d = now.year, now.month, now.day

        # 2. THE CALENDAR WIDGET
        cal = Calendar(cal_win, selectmode='day', 
                       year=y, month=m, day=d,
                       date_pattern='yyyy-mm-dd',
                       font="Arial 10",
                       background="#00a8e8", foreground="white", 
                       selectbackground="#ff5722", selectforeground="white")
        cal.pack(fill="both", expand=True, padx=10, pady=10)

        # 3. THE SET DATE ENGINE
        def set_date():
            # GET: Pull the date from the calendar
            selected_date = cal.get_date()
            
            # PUSH: Force the date into the StringVar
            target_variable.set(selected_date)
            
            # TRIGGER: Manually notify the entry field it has new data 
            # (This fixes the 'Grey text' issue)
            if hasattr(self, 'date_entry'):
                self.date_entry.config(foreground="black")
                
            self.log(f"Calendar: Set date to {selected_date}")
            cal_win.destroy()

        # Add a double-click shortcut (Fast UX)
        cal.bind("<<CalendarSelected>>", lambda e: None) # Keeps it clean
        
        # 4. CONFIRM BUTTON
        btn_confirm = tk.Button(cal_win, text="CONFIRM SELECTION", 
                                bg="#00a8e8", fg="white", 
                                font=("Segoe UI", 10, "bold"), 
                                relief="flat", cursor="hand2", 
                                command=set_date)
        btn_confirm.pack(fill="x", padx=10, pady=(0, 10), ipady=5)

        # Optional: Double-click on the date to auto-confirm
        cal.bind("<Double-1>", lambda e: set_date())

    def _show_batch_review_window(self, schedule_data):
        """Schedule Editor: Features Proximity Prioritization and 'Suggested' YES/NO column."""
        self.temp_schedule = schedule_data
        review_win = tk.Toplevel(self.root)
        review_win.title("Time-Series Schedule Editor")
        review_win.geometry("900x550") 
        review_win.configure(bg="#f8f9fa")
        review_win.grab_set()

        style = ttk.Style(review_win)
        style.theme_use('clam')
        
        review_win.option_add('*TCombobox*Listbox.font', ('Segoe UI', 9))
        review_win.option_add('*TCombobox*Listbox.selectBackground', '#00a8e8')
        review_win.option_add('*TCombobox*Listbox.selectForeground', 'white')

        header_frame = tk.Frame(review_win, bg="#f8f9fa", pady=10, padx=15)
        header_frame.pack(fill="x")
        
        tk.Label(header_frame, text="Time-Series Review", font=("Segoe UI", 12, "bold"), bg="#f8f9fa", fg="#212529").pack(anchor="w")
        tk.Label(header_frame, text="The system has pre-selected the closest available pass to your target interval.", 
                 font=("Segoe UI", 9), bg="#f8f9fa", fg="#6c757d").pack(anchor="w")

        card_frame = tk.Frame(review_win, bg="#ffffff", highlightbackground="#dee2e6", highlightthickness=1)
        card_frame.pack(fill="both", expand=True, padx=15, pady=(0, 10))

        canvas = tk.Canvas(card_frame, bg="#ffffff", highlightthickness=0)
        scrollbar = ttk.Scrollbar(card_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas, bg="#ffffff")

        scrollable_frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)

        # --- 4 COLUMNS NOW ---
        header_bg = tk.Frame(scrollable_frame, bg="#f1f3f5")
        header_bg.grid(row=0, column=0, columnspan=4, sticky="nsew")
        
        tk.Label(scrollable_frame, text="Target Interval", font=("Segoe UI", 9, "bold"), bg="#f1f3f5", fg="#495057").grid(row=0, column=0, padx=15, pady=6, sticky="w")
        tk.Label(scrollable_frame, text="Available Satellite Passes (Click to Change)", font=("Segoe UI", 9, "bold"), bg="#f1f3f5", fg="#495057").grid(row=0, column=1, padx=15, pady=6, sticky="w")
        
        # --- THE HEADER CHANGE ---
        tk.Label(scrollable_frame, text="Proximity", font=("Segoe UI", 9, "bold"), bg="#f1f3f5", fg="#495057").grid(row=0, column=2, padx=15, pady=6)
        
        tk.Label(scrollable_frame, text="Status", font=("Segoe UI", 9, "bold"), bg="#f1f3f5", fg="#495057").grid(row=0, column=3, padx=15, pady=6, sticky="w")

        scrollable_frame.columnconfigure(1, weight=1)

        for idx, item in enumerate(self.temp_schedule, start=1):
            row_bg = "#ffffff" if idx % 2 != 0 else "#f8f9fa"
            
            bg_f = tk.Frame(scrollable_frame, bg=row_bg)
            bg_f.grid(row=idx, column=0, columnspan=4, sticky="nsew")

            # Column 0: Target Date
            tk.Label(scrollable_frame, text=item['target'], font=("Segoe UI", 9, "bold"), bg=row_bg, fg="#212529").grid(row=idx, column=0, padx=15, pady=4, sticky="w")
            
            # Column 1: Dropdown
            combo = ttk.Combobox(scrollable_frame, values=item['options'], state="readonly", width=50, font=("Segoe UI", 9))
            if item['options']:
                combo.set(item['current_selection'])
            else:
                combo.set("No Data Found in 180-day window")
                combo.state(['disabled'])
            combo.grid(row=idx, column=1, padx=15, pady=4, sticky="we")

            # --- Column 2: Explicit Target/Suggested Indicator ---
            target_str = item['target']
            actual_str = item['actual'][:16]
            
            if actual_str == "None":
                is_suggested = "-"
                sugg_color = "#6c757d"
            elif actual_str == target_str:
                is_suggested = "TARGET DATE"
                sugg_color = "#005577" # Dark Blue
            else:
                is_suggested = "SUGGESTED DATE"
                sugg_color = "#e76f51" # Orange

            sugg_lbl = tk.Label(scrollable_frame, text=is_suggested, font=("Segoe UI", 9, "bold"), bg=row_bg, fg=sugg_color)
            sugg_lbl.grid(row=idx, column=2, padx=15, pady=4)

            # --- Column 3: Status ---
            status_text = "✅ Ready" if item['actual'] != "None" else "❌ Skip"
            status_lbl = tk.Label(scrollable_frame, text=status_text, font=("Segoe UI", 9), bg=row_bg, fg="#212529")
            status_lbl.grid(row=idx, column=3, padx=15, pady=4, sticky="w")

            # --- Dropdown Change Logic ---
            def on_combo_change(event, i=idx-1, cb=combo, sl=status_lbl, s_lbl=sugg_lbl):
                val = cb.get()
                self.temp_schedule[i]['current_selection'] = val
                self.temp_schedule[i]['actual'] = val[:16]
                
                curr_actual = self.temp_schedule[i]['actual']
                target_date = self.temp_schedule[i]['target']
                
                if curr_actual == "None":
                    s_lbl.config(text="-", fg="#6c757d")
                elif curr_actual == target_date:
                    s_lbl.config(text="TARGET DATE", fg="#005577")
                else:
                    s_lbl.config(text="SUGGESTED DATE", fg="#e76f51")
                
                sl.config(text="✅ Ready" if self.temp_schedule[i]['actual'] != "None" else "❌ Skip")

            combo.bind("<<ComboboxSelected>>", on_combo_change)

        action_bar = tk.Frame(review_win, bg="#f8f9fa", pady=10, padx=15)
        action_bar.pack(fill="x", side="bottom")
        
        self.confirmed = False
        def confirm(): 
            canvas.unbind_all("<MouseWheel>")
            self.confirmed = True
            review_win.destroy()
            
        def cancel():
            canvas.unbind_all("<MouseWheel>")
            review_win.destroy()
        
        btn_confirm = tk.Button(action_bar, text="Confirm & Start Batch", bg="#00a8e8", fg="white", 
                                font=("Segoe UI", 9, "bold"), relief="flat", padx=15, pady=5, cursor="hand2", command=confirm)
        btn_confirm.pack(side="right", padx=(10, 0))
        
        btn_cancel = tk.Button(action_bar, text="Cancel", bg="#e9ecef", fg="#495057", 
                               font=("Segoe UI", 9), relief="flat", padx=15, pady=5, cursor="hand2", command=cancel)
        btn_cancel.pack(side="right")
        
        self.root.wait_window(review_win)
        return [d['actual'][:16] for d in self.temp_schedule if d['actual'] != "None"] if self.confirmed else None
    
    def _evaluate_download_button(self):
        """Smart toggle: Download is ONLY ready when GEE is connected AND an AOI exists."""
        # 1. Check GEE Connection
        gee_ready = getattr(self, 'gee_connected', False)
        
        # 2. Check AOI (Is there a valid file path OR a manual drawing?)
        path = self.input_file_path.get()
        has_file = bool(path and not path.startswith("--"))
        has_manual = self.manual_roi_bounds is not None
        
        # 3. Apply State
        if gee_ready and (has_file or has_manual):
            self.btn_download.config(state="normal")
        else:
            self.btn_download.config(state="disabled")
    
    def _show_live_scout_progress(self, target_intervals):
        """Hardened monitor for the GEE scouting phase with existence tracking."""
        progress_win = tk.Toplevel(self.root)
        progress_win.title("GEE Satellite Scout")
        progress_win.geometry("550x400")
        progress_win.configure(bg="#f8f9fa")
        progress_win.grab_set()

        style = ttk.Style(progress_win)
        style.theme_use('clam')

        main_f = tk.Frame(progress_win, bg="#f8f9fa", padx=25, pady=20)
        main_f.pack(fill="both", expand=True)

        tk.Label(main_f, text="SATELLITE SCOUT", font=("Segoe UI Semibold", 10), fg="#212529", bg="#f8f9fa").pack(anchor="w")
        
        pb = ttk.Progressbar(main_f, orient="horizontal", mode="determinate", maximum=len(target_intervals))
        pb.pack(fill="x", pady=(10, 20))
        
        log_f = tk.Frame(main_f, bg="#300a24", highlightbackground="#555555", highlightthickness=1)
        log_f.pack(fill="both", expand=True)

        status_box = tk.Text(log_f, height=12, font=("Consolas", 9, "bold"), state="disabled", 
                             bg="#300a24", fg="#4af626", padx=10, pady=10, borderwidth=0)
        status_box.pack(fill="both", expand=True)
        
        # --- THREAD-SAFE UPDATE WRAPPERS ---
        def safe_update(idx, msg):
            # Check if the window was closed by the user mid-scan
            if not progress_win.winfo_exists(): return
            
            pb["value"] = idx + 1
            status_box.config(state="normal")
            status_box.insert(tk.END, f"[SYS] {msg}\n")
            status_box.see(tk.END)
            status_box.config(state="disabled")
            progress_win.update()

        return progress_win, safe_update

    def _generate_date_series(self):
        """Creates a list of YYYY-MM-DD strings based on start, end, and interval."""
        try:
            start = datetime.strptime(self.ts_start_var.get(), "%Y-%m-%d")
            end = datetime.strptime(self.ts_end_var.get(), "%Y-%m-%d")
            step = int(self.ts_step_var.get())
            
            date_list = []
            curr = start
            while curr <= end:
                date_list.append(curr.strftime("%Y-%m-%d"))
                curr += timedelta(days=step)
            return date_list
        except Exception as e:
            self.log(f"Date Error: {e}")
            return []

    def _toggle_ts_ui(self):
        """Switches between Single Date and Time-Series Range UI, hiding irrelevant controls."""
        for widget in self.date_container.winfo_children():
            widget.destroy()
            
        if self.timeseries_mode.get():
            self._setup_timeseries_ui()
            
            # --- HIDE THE SINGLE-DATE CONTROLS ---
            self.cloud_sort_check.pack_forget()
            self.available_dates_dropdown.pack_forget()
            
            # Uncheck the cloud sort just in case, so it doesn't mess with background logic
            self.sort_by_cloud_var.set(False) 
            
            self.log("Time-Series Mode: Select Start/End dates and Interval.")
        else:
            self._setup_single_date_ui()
            
            # --- SHOW THEM AGAIN (in the exact correct order) ---
            # By using 'before=self.band_label', they snap perfectly back into place
            self.cloud_sort_check.pack(anchor="w", pady=2, before=self.band_label)
            self.available_dates_dropdown.pack(fill="x", pady=(0, 5), before=self.band_label)
            
            self.log("Single-Temporal Mode: Select a specific pass.")

    def _setup_single_date_ui(self):
        """UI for Single Date selection."""
        ttk.Label(self.date_container, text="Target Date:").pack(side="left")
        
        # Use a slightly wider entry to prevent text clipping
        self.date_entry = ttk.Entry(self.date_container, textvariable=self.target_date_var, width=14)
        self.date_entry.pack(side="left", padx=(5, 2))
        
        # Set initial state
        if self.target_date_var.get() == self.yesterday_val:
            self.date_entry.config(foreground="grey")

        tk.Button(self.date_container, text="📅", cursor="hand2", relief="flat", bg="#f0f0f0",
                  command=lambda: self._open_calendar_picker(self.target_date_var)).pack(side="left", padx=(0, 5))
        
        self.date_entry.bind("<FocusIn>", self.clear_placeholder)
        self.date_entry.bind("<FocusOut>", self.restore_placeholder)
        
        # Force black text whenever the variable changes (e.g., from the Calendar)
        self.target_date_var.trace_add("write", lambda *args: self.date_entry.config(foreground="black"))
        
        ttk.Button(self.date_container, text="🔍 Check Pass", command=self.find_available_dates).pack(side="left", fill="x", expand=True)

    def _setup_timeseries_ui(self):
        """UI for Multi-temporal batch download with compact Calendar Pickers."""
        self.ts_start_var = tk.StringVar(value="2025-01-01")
        self.ts_end_var = tk.StringVar(value="2025-12-31")
        self.ts_step_var = tk.StringVar(value="30") # Days between images

        # Reduced outer padding to pull the UI together
        grid_f = ttk.Frame(self.date_container, padding=2)
        grid_f.pack(fill="x")
        
        # Define columns: Label, Entry, Calendar Button, Help/Blank
        grid_f.columnconfigure(0, weight=0, minsize=80)  # Labels (Start/End)
        grid_f.columnconfigure(1, weight=1)              # Entry boxes
        grid_f.columnconfigure(2, weight=0, minsize=45)  # Calendar buttons
        grid_f.columnconfigure(3, weight=0, minsize=30)

        # --- ROW 1: START DATE ---
        ttk.Label(grid_f, text="Start:").grid(row=0, column=0, sticky="w", pady=2)
        ttk.Entry(grid_f, textvariable=self.ts_start_var).grid(row=0, column=1, sticky="we", pady=2)
        
        # THE FIX: Swapped to tk.Button for strict height control
        tk.Button(grid_f, text="📅", cursor="hand2", relief="groove", bd=1, bg="#ffffff",
                  command=lambda: self._open_calendar_picker(self.ts_start_var)).grid(row=0, column=2, padx=(3, 0), pady=1, sticky="we")

        # --- ROW 2: END DATE ---
        ttk.Label(grid_f, text="End:").grid(row=1, column=0, sticky="w", pady=2)
        ttk.Entry(grid_f, textvariable=self.ts_end_var).grid(row=1, column=1, sticky="we", pady=2)
        
        # THE FIX: Compact tk.Button
        tk.Button(grid_f, text="📅", cursor="hand2", relief="groove", bd=1, bg="#ffffff",
                  command=lambda: self._open_calendar_picker(self.ts_end_var)).grid(row=1, column=2, padx=(3, 0), pady=1, sticky="we")

        # --- ROW 3: INTERVAL ---
        ttk.Label(grid_f, text="Interval:").grid(row=2, column=0, sticky="w", pady=(2, 0))
        
        interval_f = ttk.Frame(grid_f)
        interval_f.grid(row=2, column=1, sticky="w", pady=(2, 0))
    def _setup_timeseries_ui(self):
        """UI for Multi-temporal range selection."""
        self.ts_start_var = tk.StringVar(value=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'))
        self.ts_end_var = tk.StringVar(value=self.yesterday_val)
        self.ts_step_var = tk.StringVar(value="30")

        grid_f = ttk.Frame(self.date_container, padding=2)
        grid_f.pack(fill="x")
        
        grid_f.columnconfigure(1, weight=1)

        # Row 1: Start
        ttk.Label(grid_f, text="Start:").grid(row=0, column=0, sticky="w", pady=2)
        ent_start = ttk.Entry(grid_f, textvariable=self.ts_start_var)
        ent_start.grid(row=0, column=1, sticky="we", pady=2, padx=(5, 0))
        tk.Button(grid_f, text="📅", relief="flat", bg="#f0f0f0", 
                  command=lambda: self._open_calendar_picker(self.ts_start_var)).grid(row=0, column=2, padx=2)

        # Row 2: End
        ttk.Label(grid_f, text="End:").grid(row=1, column=0, sticky="w", pady=2)
        ent_end = ttk.Entry(grid_f, textvariable=self.ts_end_var)
        ent_end.grid(row=1, column=1, sticky="we", pady=2, padx=(5, 0))
        tk.Button(grid_f, text="📅", relief="flat", bg="#f0f0f0", 
                  command=lambda: self._open_calendar_picker(self.ts_end_var)).grid(row=1, column=2, padx=2)

        # Trace logic to ensure color stays correct
        self.ts_start_var.trace_add("write", lambda *args: ent_start.config(foreground="black"))
        self.ts_end_var.trace_add("write", lambda *args: ent_end.config(foreground="black"))

        # Interval Row
        interval_f = ttk.Frame(grid_f)
        interval_f.grid(row=2, column=0, columnspan=3, sticky="w", pady=5)
        ttk.Label(interval_f, text="Interval:").pack(side="left")
        ttk.Entry(interval_f, textvariable=self.ts_step_var, width=5).pack(side="left", padx=5)
        ttk.Label(interval_f, text="days").pack(side="left")

    def _clear_coord_hint(self, widget, hint_text):
        """Clears the 'Latitude/Longitude' text when the user clicks to type."""
        if widget.get() == hint_text:
            widget.delete(0, tk.END)
            widget.config(foreground="black")

    def load_search_history(self):
        import json
        if os.path.exists(self.search_history_file):
            try:
                with open(self.search_history_file, "r") as f:
                    return json.load(f)
            except: return []
        return []

    def save_search_to_history(self, data):
        import json
        # Don't add duplicates
        if any(h['address'] == data['address'] for h in self.search_history):
            return
        
        self.search_history.insert(0, data)
        self.search_history = self.search_history[:20] # Keep last 20 searches
        
        with open(self.search_history_file, "w") as f:
            json.dump(self.search_history, f)
        
        # Update UI
        self.history_listbox.delete(0, tk.END)
        for item in self.search_history:
            self.history_listbox.insert(tk.END, item['address'])

    def jump_from_history(self):
        idx = self.history_listbox.curselection()
        if not idx: return
        
        data = self.search_history[idx[0]]
        b = [float(x) for x in data["bounds"]]
        
        self.map_widget.set_position(float(data["coords"][0]), float(data["coords"][1]))
        self._draw_roi(b[0], b[1], b[2], b[3])
        self.manual_roi_bounds = (b[0], b[1], b[2], b[3])
        self._evaluate_download_button()
        self.map_widget.fit_bounding_box((b[3], b[0]), (b[1], b[2]))
        self._calculate_aoi_hectares(b)
        self.log(f"History Jump: {data['address']}")

    def clear_search_history(self):
        if messagebox.askyesno("Clear History", "Delete all saved search history?"):
            self.search_history = []
            if os.path.exists(self.search_history_file):
                os.remove(self.search_history_file)
            self.history_listbox.delete(0, tk.END)
            self.log("Search history cleared.")
    
    def create_tooltip(self, widget, text):
        """Creates a hover-over tooltip for any widget."""
        def enter(event):
            self.tooltip_win = tk.Toplevel(widget)
            self.tooltip_win.wm_overrideredirect(True)
            self.tooltip_win.wm_geometry(f"+{event.x_root+10}+{event.y_root+10}")
            label = tk.Label(self.tooltip_win, text=text, justify='left',
                             background="#ffffe0", relief='solid', borderwidth=1,
                             font=("tahoma", "8", "normal"))
            label.pack(ipadx=1)
        def leave(event):
            if hasattr(self, 'tooltip_win'):
                self.tooltip_win.destroy()
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)
        
    def show_loading_curtain(self, message="Crunching Data..."):
        """Shows the splash card and locks the UI using modal grabbing."""
        self.lock_label.config(text=message)
        
        # 1. Place the card exactly in the center of the app
        self.splash_card.place(relx=0.5, rely=0.5, anchor="center")
        self.splash_card.lift() 
        
        # 2. THE MAGIC LOCK: This routes ALL clicks and keyboard inputs to the card.
        # You literally cannot click the map, buttons, or treeview while this is active.
        self.splash_card.grab_set() 
        
        self.lock_progress.start(10)
        self.root.update()

    def hide_loading_curtain(self):
        """Removes the splash and releases the UI lock."""
        self.lock_progress.stop()
        
        # 1. Release the magic lock so you can click the app again
        self.splash_card.grab_release() 
        
        # 2. Hide the card
        self.splash_card.place_forget()
        self.root.update()

    def run_location_search(self):
        query = self.search_entry.get().strip()
        if not query: return
        
        self.log(f"Searching for: {query}...")
        self.search_listbox.delete(0, tk.END)
        self.search_listbox.insert(tk.END, "Searching...")
        
        # Run in a separate thread to keep the UI from freezing during the network call
        threading.Thread(target=self._location_worker, args=(query,), daemon=True).start()

    def _location_worker(self, query):
        from geopy.geocoders import Nominatim
        try:
            # We explicitly do NOT request 'geometry' to keep the response tiny and fast
            geolocator = Nominatim(user_agent="GEE2DB_Scout", timeout=10)
            locations = geolocator.geocode(query, exactly_one=False, limit=5)
            
            if locations:
                self.root.after(0, lambda: self._update_search_listbox(locations))
            else:
                self.root.after(0, lambda: (
                    self.search_listbox.delete(0, tk.END),
                    self.search_listbox.insert(tk.END, "No results found.")
                ))
        except Exception as e:
            self.root.after(0, lambda err=str(e): self.log(f"Search Error: {err}"))

    def _update_search_listbox(self, locations):
        self.search_listbox.delete(0, tk.END)
        self.current_search_results = []
        
        for loc in locations:
            # We extract the bounding box provided by the search engine
            # Format is usually [min_lat, max_lat, min_lon, max_lon]
            raw_bounds = loc.raw.get('boundingbox')
            
            if raw_bounds:
                # Re-order to our internal format: (minx, miny, maxx, maxy)
                clean_bounds = (float(raw_bounds[2]), float(raw_bounds[0]), 
                                float(raw_bounds[3]), float(raw_bounds[1]))
                
                self.search_listbox.insert(tk.END, loc.address)
                self.current_search_results.append({
                    "coords": (loc.latitude, loc.longitude),
                    "bounds": clean_bounds,
                    "address": loc.address
                })

    def on_search_select(self, event):
        """Unified Search Logic: Jumps, Draws AOI, Saves History, and Enables Download."""
        selection = self.search_listbox.curselection()
        if not selection or selection[0] >= len(self.current_search_results): return
        
        data = self.current_search_results[selection[0]]
        
        try:
            # 1. DATA CONVERSION: Ensure everything is a float to prevent math errors
            lat = float(data["coords"][0])
            lon = float(data["coords"][1])
            b = [float(x) for x in data["bounds"]] # [minx, miny, maxx, maxy]
            
            # 2. MAP JUMP & DRAW: Center the map and show the Cyan AOI
            self.map_widget.set_position(lat, lon)
            self._draw_roi(b[0], b[1], b[2], b[3])
            
            # 3. DOWNLOAD LOCK: Set the bounds and enable the 'Start Download' button
            self.manual_roi_bounds = (b[0], b[1], b[2], b[3])
            self._evaluate_download_button()
            self.input_file_path.set("") # Clear path to prioritize the search area
            
            # 4. VIEWPORT & MEASUREMENT: Fit the map and update Hectares
            self.map_widget.fit_bounding_box((b[3], b[0]), (b[1], b[2]))
            self._calculate_aoi_hectares(b)
            
            # 5. THE HISTORY FIX: Save this result to your persistent history file
            self.save_search_to_history(data)
            
            self.log(f"Search Area Locked & Saved: {data['address']}")
            
        except Exception as e:
            self.log(f"Selection Error: {e}")

    def _fetch_polygon_async(self, address):
        """Background worker to get the heavy zigzag lines without freezing the UI."""
        from geopy.geocoders import Nominatim
        try:
            geolocator = Nominatim(user_agent="GEE2DB_Scout")
            # Here we request the heavy GeoJSON data
            location = geolocator.geocode(address, exactly_one=True, geometry='geojson')
            
            if location and 'geojson' in location.raw:
                # Send the drawing command back to the main thread
                self.root.after(0, lambda: self._draw_admin_boundary(location.raw['geojson']))
        except:
            pass # If the polygon fails, the user still has the Cyan Box

    def _draw_admin_boundary(self, geojson):
        """Deep-dive drawing for complex administrative borders."""
        if not hasattr(self, 'search_boundary_objs'): self.search_boundary_objs = []
        
        geom_type = geojson.get('type')
        coords_data = geojson.get('coordinates')

        if not coords_data:
            self.log("Warning: No polygon data returned for this location.")
            return

        def draw_recursive(item):
            # Check if we are at the bottom level: a list of [lon, lat] pairs
            if isinstance(item, list) and len(item) > 0:
                if isinstance(item[0], (int, float)):
                    # This is just a point [lon, lat], we shouldn't be here alone
                    return None
                
                # Check if this is a list of points (the actual ring)
                if isinstance(item[0], list) and isinstance(item[0][0], (int, float)):
                    path = [(float(pt[1]), float(pt[0])) for pt in item]
                    poly = self.map_widget.set_polygon(path, outline_color="#ff00ff", 
                                                       fill_color=None, border_width=2)
                    self.search_boundary_objs.append(poly)
                else:
                    # It's another nested list (MultiPolygon or Hole), keep digging
                    for sub_item in item:
                        draw_recursive(sub_item)

        try:
            draw_recursive(coords_data)
        except Exception as e:
            self.log(f"Boundary Draw Error: {e}")

    def _calculate_aoi_hectares(self, b):
        """Calculates approximate area in hectares for the AOI using UTM Zone 51N."""
        try:
            from shapely.geometry import box
            import pyproj
            from shapely.ops import transform

            # Prevent crash on tiny points/lines
            if abs(b[2] - b[0]) < 0.0001 or abs(b[3] - b[1]) < 0.0001:
                self.aoi_bounds_label.config(text="Point Location (No Area)", bg="#333333", fg="gray")
                return

            geom = box(b[0], b[1], b[2], b[3])
            
            # Philippine-specific projection for accurate measurement
            wgs84 = pyproj.CRS('EPSG:4326')
            utm51n = pyproj.CRS('EPSG:32651')
            
            project = pyproj.Transformer.from_crs(wgs84, utm51n, always_xy=True).transform
            geom_utm = transform(project, geom)
            
            hectares = geom_utm.area / 10000
            
            # Update the HUD with the specific measurement
            hud_text = (
                f"Top Lat: {b[3]:.5f} | Bot Lat: {b[1]:.5f}\n"
                f"L-Lon: {b[0]:.5f} | R-Lon: {b[2]:.5f}\n"
                f"ESTIMATED AREA: {hectares:,.2f} Hectares"
            )
            self.aoi_bounds_label.config(text=hud_text, bg="#1a1a1a", fg="#00ffff")
        except Exception as e:
            self.log(f"Hectare Calc Error: {e}")

    def jump_to_coords(self):
        """Pro-Grade Coordinate Jumper: Validates, Jumps, Draws AOI, and Enables Download."""
        try:
            # 1. CLEAN & VALIDATE: Remove spaces or symbols common in GPS copies
            raw_lat = self.lat_entry.get().replace('°', '').strip()
            raw_lon = self.lon_entry.get().replace('°', '').strip()
            
            lat = float(raw_lat)
            lon = float(raw_lon)
            
            # 2. DEFINE A 1KM SEARCH BOX: 
            # In PH latitudes, 0.0045 degrees is roughly 500m. 
            # We create a 1km x 1km AOI centered on your point.
            offset = 0.0045 
            b = [lon - offset, lat - offset, lon + offset, lat + offset]
            
            # 3. MAP JUMP: Move the map and set a high-detail zoom
            self.map_widget.set_position(lat, lon)
            self.map_widget.set_zoom(16)
            
            # 4. DRAW & LOCK: Draw the Cyan AOI and enable the download button
            self._draw_roi(b[0], b[1], b[2], b[3])
            self.manual_roi_bounds = (b[0], b[1], b[2], b[3])
            self._evaluate_download_button()
            
            # 5. CLEAR FILE PATH: Prioritize the coordinate jump
            self.input_file_path.set("")
            
            # 6. CALCULATE AREA: Update the HUD with the new 1km box info
            self._calculate_aoi_hectares(b)
            
            self.log(f"Coordinate Scout: Locked 1km box at {lat:.5f}, {lon:.5f}")
            
        except ValueError:
            messagebox.showerror("Input Error", "Please enter valid numerical coordinates (e.g., 14.58, 120.98).")
        except Exception as e:
            self.log(f"Jump Error: {e}")

    def clear_placeholder(self, event):
        """Turn text black when user clicks. Only clear if it's the 'hint' value."""
        widget = event.widget
        current_val = widget.get()
        widget.config(foreground="black")
        
        # If it's the 'yesterday' hint or the 'YYYY-MM-DD' hint, clear it for typing
        if current_val == self.yesterday_val or "2025-" in current_val:
            # We don't wipe it if it looks like a real date the user might want to edit
            if current_val == self.yesterday_val:
                widget.delete(0, tk.END)

    def restore_placeholder(self, event):
        """Restore grey hint only if the field is completely empty."""
        widget = event.widget
        if not widget.get().strip():
            widget.insert(0, self.yesterday_val)
            widget.config(foreground="grey")

    def sync_map_offline(self):
        """Final corrected sync: Handles existing tiles gracefully."""
        self.log("Syncing map area... Connecting to tile server.")
        
        script_directory = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(script_directory, "offline_tiles.db")

        lat, lon = self.map_widget.get_position()
        top_left = (lat + 0.1, lon - 0.1)
        bottom_right = (lat - 0.1, lon + 0.1)

        def run_sync():
            try:
                loader = tkintermapview.OfflineLoader(path=db_path)
                
                try:
                    loader.save_offline_tiles(top_left, bottom_right, 6, 15)
                except Exception as inner_e:
                    # Catch the duplicate tile error and translate it for the user
                    if "UNIQUE constraint failed" in str(inner_e):
                        self.log("Note: Tiles in this area are ALREADY saved offline! Skipping duplicates.")
                    else:
                        raise inner_e 

                if os.path.exists(db_path):
                    size_mb = os.path.getsize(db_path) / (1024 * 1024)
                    self.log(f"SUCCESS: Database active. Size: {size_mb:.2f} MB.")
                
                messagebox.showinfo("Offline Sync", "Area verified and cached! The map is running locally.")
            except Exception as e:
                self.log(f"Sync Error: {e}")

        threading.Thread(target=run_sync, daemon=True).start()

    def preview_image(self):
        """Fix: Renders high-fidelity preview in a dedicated popup."""
        item = self.record_table.selection()
        if not item: return
        path = self.record_table.item(item, "values")[6] 
        
        # Open in a separate Toplevel window to avoid main-loop freezing
        top = tk.Toplevel(self.root)
        top.title(f"Preview: {os.path.basename(path)}")
        
        try:
            import matplotlib.pyplot as plt
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            
            with rasterio.open(path) as src:
                data = src.read(out_shape=(src.count, 800, 800))
                # ... apply stretch logic here ...
                fig, ax = plt.subplots(figsize=(6, 6))
                ax.imshow(np.dstack([data[0], data[1], data[2]])) # Simple RGB
                canvas = FigureCanvasTkAgg(fig, master=top)
                canvas.draw()
                canvas.get_tk_widget().pack(fill="both", expand=True)
        except Exception as e: self.log(f"Popup Error: {e}")

    def get_safe_pixel_pos(self, lat, lon):
        """Helper to find pixel coordinates regardless of library version."""
        for method_name in ['get_canvas_pos', 'convert_decimal_coords_to_canvas_coords', 'get_canvas_position']:
            if hasattr(self.map_widget, method_name):
                method = getattr(self.map_widget, method_name)
                return method(lat, lon)
        # Fallback manual calculation if all else fails
        return self.map_widget.canvas_coords(lat, lon)

    def overlay_on_map(self):
        """Unified Toggle: Ensures the Pinner Engine starts guarding the image."""
        item = self.record_table.selection()
        if not item: return
        path = self.record_table.item(item, "values")[6] 
        
        with self.raster_lock:
            if path in self.active_layer_polygons:
                # TURN OFF logic
                if path in self.active_rasters:
                    raster = self.active_rasters[path]
                    self.map_widget.canvas.delete(raster["img_item"])
                    try: raster["box_obj"].delete()
                    except: pass
                    del self.active_rasters[path]
                
                if path in self.active_layer_polygons:
                    del self.active_layer_polygons[path]
            else:
                # TURN ON logic
                self.active_layer_polygons[path] = True
                self._show_tif_preview(path)
        
        self.populate_layers_tree()
        
        # 🚀 THE ENGINE START: Trigger the Watchdog
        if self.active_rasters and not self.tracker_running:
            self.tracker_running = True
            self._keep_image_pinned()

    def _keep_image_pinned(self):
        """Watchdog loop: Forces the satellite image to the ABSOLUTE top layer."""
        if getattr(self, 'is_closing', False) or not self.active_rasters:
            self.tracker_running = False
            return 
            
        try:
            from PIL import ImageTk
            with self.raster_lock: 
                for path, raster in list(self.active_rasters.items()):
                    box = raster["box_obj"]
                    
                    # 1. Get the Anchor ID (The Red Box)
                    poly_id = getattr(box, "canvas_polygon", getattr(box, "polygon", None))
                    if not poly_id: continue 
                    
                    # 2. Get the screen location
                    bbox = self.map_widget.canvas.bbox(poly_id)
                    
                    if bbox:
                        x1, y1, x2, y2 = bbox
                        w, h = max(x2 - x1, 5), max(y2 - y1, 5)
                        
                        # 🚀 THE ENVIRONMENT FIX: If the map wiped the canvas, respawn the image
                        if not self.map_widget.canvas.find_withtag(raster["img_item"]):
                            raster["img_item"] = self.map_widget.canvas.create_image(x1, y1, anchor="nw", tags="sat")
                            raster["last_size"] = (0, 0) 

                        # 3. Resize if zoomed
                        if (w, h) != raster["last_size"] and w < 6000:
                            resized = raster["master_img"].resize((w, h), Image.Resampling.NEAREST)
                            raster["photo_img"] = ImageTk.PhotoImage(resized)
                            self.map_widget.canvas.itemconfig(raster["img_item"], image=raster["photo_img"])
                            raster["last_size"] = (w, h)

                        # 4. PIN & LIFT: Force the image to the front of the Z-stack
                        self.map_widget.canvas.coords(raster["img_item"], x1, y1)
                        
                        # 🔥 This is the critical line to fight the new Map configuration:
                        self.map_widget.canvas.tag_raise(raster["img_item"]) # Bring to front of all items
                        self.map_widget.canvas.lift(raster["img_item"])      # Force lift above tiles
                        
        except Exception: 
            pass 
        
        # Fast 30ms heartbeat to keep the image 'glued' to the screen
        if not getattr(self, 'is_closing', False):
            self.root.after(10, self._keep_image_pinned)

    def _show_tif_preview(self, path):
        """High-Speed Rendering: Uses sampling and background threading to prevent UI lag."""
        if not os.path.exists(path): return
        
        # 1. Calculate File Size
        file_size_mb = os.path.getsize(path) / (1024 * 1024)
        
        # 2. Show size in the Loading Curtain and the Console
        self.show_loading_curtain(f"Processing {file_size_mb:.1f}MB Satellite Image...")
        self.log(f"Map View: Injecting {os.path.basename(path)} ({file_size_mb:.1f} MB)")
        
        def processing_task():
            try:
                with rasterio.open(path) as src:
                    # 1. Coordinate Transform
                    bounds = transform_bounds(src.crs, 'EPSG:4326', *src.bounds)
                    min_lon, min_lat, max_lon, max_lat = bounds
                    
                    # 2. Optimized Reading: Read a smaller overview for the preview
                    # 512x512 is plenty for a map overlay and 4x faster than 1024x1024
                    data = src.read(out_shape=(src.count, 512, 512), resampling=rasterio.enums.Resampling.bilinear)
                    
                    # 3. FAST STRETCH: Use a 10% sample to find percentiles (Massive speed gain)
                    def fast_stretch(b):
                        valid = b[b > 0]
                        if valid.size == 0: return (b * 0).astype(np.uint8)
                        
                        # Only use every 10th pixel to calculate the histogram/percentile
                        sample = valid[::10] 
                        low, high = np.percentile(sample, (2, 98))
                        return np.clip((b - low) / (max(high - low, 1)) * 255, 0, 255).astype(np.uint8)

                    # Create Alpha mask from the first band
                    mask = (data[0] == 0).astype(np.uint8) * 255
                    alpha = 255 - mask

                    if src.count >= 3:
                        r = fast_stretch(data[0])
                        g = fast_stretch(data[1])
                        b_band = fast_stretch(data[2])
                        img_array = np.dstack((r, g, b_band, alpha))
                    else:
                        gray = fast_stretch(data[0])
                        img_array = np.dstack((gray, gray, gray, alpha))

                    base_pil_image = Image.fromarray(img_array, 'RGBA')

                # 4. Schedule UI update on the main thread
                self.root.after(0, lambda: self._finalize_preview_ui(path, base_pil_image, max_lat, min_lon, min_lat, max_lon))

            except Exception as e:
                # We 'capture' the error message as a string (err) immediately
                self.root.after(0, lambda err=str(e): self.log(f"Render Error: {err}"))
            finally:
                self.root.after(0, self.hide_loading_curtain)

        threading.Thread(target=processing_task, daemon=True).start()

    def _finalize_preview_ui(self, path, base_pil_image, max_lat, min_lon, min_lat, max_lon):
        """Updates the map canvas once the background processing is done."""
        with self.raster_lock:
            # Fit Map
            if not getattr(self, 'is_batch_loading', False):
                self.map_widget.fit_bounding_box((max_lat, min_lon), (min_lat, max_lon))

            # Create the Red Anchor Box
            box_coords = [(max_lat, min_lon), (max_lat, max_lon), (min_lat, max_lon), (min_lat, min_lon)]
            box_obj = self.map_widget.set_polygon(box_coords, outline_color="red", border_width=2, fill_color=None)
            
            # Create the Canvas Item
            img_item = self.map_widget.canvas.create_image(0, 0, anchor="nw", tags="persistent_sat")

            self.active_rasters[path] = {
                "img_item": img_item,
                "box_obj": box_obj,
                "master_img": base_pil_image,
                "last_size": (0, 0),
                "photo_img": None
            }

        if not self.tracker_running:
            self.tracker_running = True
            self._keep_image_pinned()

    def close_preview(self):
        """Unselects the current record and restores the Map view."""
        self.preview_container.pack_forget()
        self.map_widget.pack(fill="both", expand=True)
        
        # Clear cache to save memory
        if hasattr(self, 'current_preview_array'):
            del self.current_preview_array
            
        self.log("Record unselected. Returned to Map View.")

    def zoom_to_folder_extent(self, folder_iid):
        """Calculates the collective bounding box of all layers in a folder and zooms once."""
        all_bounds = []
        for child in self.layers_tree.get_children(folder_iid):
            path = self.layers_tree.item(child, "values")[0]
            try:
                if path.endswith('.tif'):
                    with rasterio.open(path) as s: 
                        b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
                        all_bounds.append(b)
                elif path.endswith(('.shp', '.geojson')):
                    b = gpd.read_file(path).to_crs("EPSG:4326").total_bounds
                    all_bounds.append(b)
            except: continue

        if all_bounds:
            # Calculate the "envelope" that contains all boxes
            min_x = min(b[0] for b in all_bounds)
            min_y = min(b[1] for b in all_bounds)
            max_x = max(b[2] for b in all_bounds)
            max_y = max(b[3] for b in all_bounds)
            self.map_widget.fit_bounding_box((max_y, min_x), (min_y, max_x))
            self.log("Map View: Optimized for group extent.")

    def show_layers_context_menu(self, event):
        """Right-click menu for both batch folder processing and individual file toggling."""
        iid = self.layers_tree.identify_row(event.y)
        if not iid: return
        
        self.layers_tree.selection_set(iid)
        menu = tk.Menu(self.root, tearoff=0)
        
        children = self.layers_tree.get_children(iid)
        if children:
            # --- USER CLICKED A FOLDER ---
            folder_name = self.layers_tree.item(iid, "text").replace("📁 ", "")
            menu.add_command(label="👁️ Render All in Folder", command=lambda: self.toggle_folder_contents(iid, True, folder_name))
            menu.add_command(label="🚫 Hide All in Folder", command=lambda: self.toggle_folder_contents(iid, False, folder_name))
            menu.add_separator()
            menu.add_command(label="🔍 Zoom to Group Extent", command=lambda: self.zoom_to_folder_extent(iid))
        else:
            # --- USER CLICKED A SPECIFIC FILE ---
            values = self.layers_tree.item(iid, "values")
            if not values: return
            path = values[0]
            
            # Check if this specific file is currently rendered
            is_active = path in self.active_layer_polygons
            
            if is_active:
                menu.add_command(label="🚫 Remove Layer from Canvas", command=lambda: self._force_toggle_single(path, False))
            else:
                menu.add_command(label="👁️ Render Layer to Canvas", command=lambda: self._force_toggle_single(path, True))

        menu.post(event.x_root, event.y_root)

    def _force_toggle_single(self, path, turn_on):
        """Programmatically renders or hides a single layer via the right-click menu."""
        if turn_on:
            if path.endswith('.tif'):
                self._show_tif_preview(path)
                self.active_layer_polygons[path] = True
            elif path.endswith('.geojson') or path.endswith('.shp'):
                self._draw_layer_to_map(path)
        else:
            # Hiding logic
            if path.endswith('.tif') and hasattr(self, 'active_rasters'):
                if path in self.active_rasters:
                    raster = self.active_rasters[path]
                    self.map_widget.canvas.delete(raster["img_item"])
                    try: raster["box_obj"].delete()
                    except: pass
                    del self.active_rasters[path]
                    
            elif path.endswith('.geojson') or path.endswith('.shp'):
                layer_data = self.active_layer_polygons.get(path)
                if isinstance(layer_data, dict):
                    for poly in layer_data.get("polygons", []):
                        try: poly.delete()
                        except: pass
                    for lbl in layer_data.get("active_labels", []):
                        try: lbl.delete()
                        except: pass
                elif isinstance(layer_data, list): # Fallback
                    for poly in layer_data:
                        try: poly.delete()
                        except: pass
                        
            if path in self.active_layer_polygons:
                del self.active_layer_polygons[path]
                self.log(f"Layer manually hidden: {os.path.basename(path)}")
                
        # Instantly update the checkboxes
        self.populate_layers_tree()

    def toggle_folder_contents(self, folder_iid, turn_on, folder_name):
        """Batch processes all files and optimizes the map view for the entire group."""
        self.log(f"Batch processing '{folder_name}'...")
        
        # 1. Start Batch Mode
        self.is_batch_loading = True
        
        paths_to_process = []
        def get_all_file_paths(node):
            for child in self.layers_tree.get_children(node):
                vals = self.layers_tree.item(child, "values")
                if vals: paths_to_process.append(vals[0])
                else: get_all_file_paths(child)
        get_all_file_paths(folder_iid)
        
        if not paths_to_process: 
            self.is_batch_loading = False
            return

        # 2. ZOOM TO THE "BIG PICTURE" FIRST
        if turn_on:
            all_bounds = []
            for path in paths_to_process:
                try:
                    if path.endswith('.tif'):
                        with rasterio.open(path) as s:
                            b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
                            all_bounds.append(b)
                    elif path.endswith(('.shp', '.geojson')):
                        # Efficiently get bounds without loading the whole heavy file
                        b = gpd.read_file(path, rows=1).to_crs("EPSG:4326").total_bounds
                        all_bounds.append(b)
                except: continue

            if all_bounds:
                min_x = min(b[0] for b in all_bounds)
                min_y = min(b[1] for b in all_bounds)
                max_x = max(b[2] for b in all_bounds)
                max_y = max(b[3] for b in all_bounds)
                # Force the map to the "Envelope" of all layers
                self.map_widget.fit_bounding_box((max_y, min_x), (min_y, max_x))

        # 3. RENDER WITHOUT INDIVIDUAL INTERRUPTION
        changed = False
        for path in paths_to_process:
            is_currently_on = path in self.active_layer_polygons
            
            if turn_on and not is_currently_on:
                if path.endswith('.tif'):
                    self._show_tif_preview(path)
                    self.active_layer_polygons[path] = True 
                elif path.endswith(('.geojson', '.shp')):
                    self._draw_layer_to_map(path)
                changed = True
            elif not turn_on and is_currently_on:
                self._force_toggle_single(path, False)
                changed = True
                
        # 4. End Batch Mode
        self.is_batch_loading = False
        if changed:
            self.populate_layers_tree()
    
    def populate_layers_tree(self):
        """Recursively scans the Data folder and builds a multi-level File Explorer tree."""
        # 1. Snapshot open folders
        open_folders = set()
        def get_open_nodes(node=""):
            for child in self.layers_tree.get_children(node):
                if self.layers_tree.item(child, "open"):
                    open_folders.add(child)
                get_open_nodes(child)
        get_open_nodes()
                
        # Clear existing items
        for item in self.layers_tree.get_children():
            self.layers_tree.delete(item)
            
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, "Data")
        
        if not os.path.exists(data_dir):
            self.log("Data folder is empty or not created yet.")
            return

        active_paths = []
        if hasattr(self, 'active_layer_polygons'):
            active_paths = [os.path.normpath(p) for p in self.active_layer_polygons.keys()]

        # 2. Recursive function
        def add_nodes(parent_path, parent_node):
            try:
                entries = sorted(os.listdir(parent_path))
                entries = [e for e in entries if not e.startswith(('.', '$', 'Thumbs.db'))]
                folders = [e for e in entries if os.path.isdir(os.path.join(parent_path, e))]
                files = [e for e in entries if os.path.isfile(os.path.join(parent_path, e))]
                
                for folder_name in folders:
                    folder_path = os.path.normpath(os.path.join(parent_path, folder_name))
                    is_open = folder_path in open_folders
                    f_node = self.layers_tree.insert(parent_node, "end", iid=folder_path, text=f"📁 {folder_name}", open=is_open)
                    add_nodes(folder_path, f_node)
                    
                # --- ENHANCED FILE SCANNER ---
                for file_name in files:
                    # Use .lower() to ensure .TIF and .tiff are caught
                    ext = os.path.splitext(file_name)[1].lower()
                    
                    if ext in ('.geojson', '.tif', '.tiff', '.shp'):
                        file_path = os.path.normpath(os.path.join(parent_path, file_name))
                        
                        # Dynamic Icon Mapping
                        if ext == '.shp': 
                            icon = "📐" 
                        elif ext == '.geojson': 
                            icon = "📍"
                        else: 
                            icon = "🗺️" # For .tif and .tiff
                            
                        checkbox = "☑ " if file_path in active_paths else "☐ "
                        
                        # We use the full path in 'values' so toggle_map_layer knows exactly what to load
                        self.layers_tree.insert(parent_node, "end", text=f"{checkbox}{icon} {file_name}", values=(file_path,))
                        
            except PermissionError:
                pass
        
        add_nodes(data_dir, "")

    def toggle_map_layer(self, event):
        """Routes files to the interactive map. Handles both Vectors and Rasters."""
        # 1. Identify exactly what row the mouse was hovering over
        item_id = self.layers_tree.identify_row(event.y)
        if not item_id: return
        
        values = self.layers_tree.item(item_id, "values")
        if not values: return # Folder clicked
        
        # Path Normalization is key for the "is it already on?" check
        path = os.path.normpath(values[0])
        ext = os.path.splitext(path)[1].lower()
        
        # --- IF TIFF: Handle Native Image Tracking Overlay ---
        if ext in ('.tif', '.tiff'):
            if not hasattr(self, 'active_rasters'): self.active_rasters = {}
            
            if path in self.active_layer_polygons:
                # TURN OFF logic
                if path in self.active_rasters:
                    raster = self.active_rasters[path]
                    self.map_widget.canvas.delete(raster["img_item"])
                    try: raster["box_obj"].delete()
                    except: pass
                    del self.active_rasters[path]
                
                del self.active_layer_polygons[path]
                self.log(f"Satellite overlay hidden: {os.path.basename(path)}")
            else:
                # TURN ON logic
                # This calls _show_tif_preview which uses rasterio to auto-calculate 
                # bounds, making it compatible with GEE AND manual files.
                self._show_tif_preview(path)
                self.active_layer_polygons[path] = True 
            
            self.populate_layers_tree()
            return

        # --- IF GEOJSON / SHP: Toggle interactive map polygon ---
        if path in self.active_layer_polygons:
            layer_data = self.active_layer_polygons[path]
            # Handle structured dictionary or raw list for backward compatibility
            to_delete = []
            if isinstance(layer_data, dict):
                to_delete = layer_data.get("polygons", []) + layer_data.get("active_labels", [])
            else:
                to_delete = layer_data
            
            for obj in to_delete:
                try: obj.delete()
                except: pass

            del self.active_layer_polygons[path]
            self.log(f"Layer hidden: {os.path.basename(path)}")
            self.populate_layers_tree() 
        else:
            self._draw_layer_to_map(path)

    def _draw_layer_to_map(self, path):
        """Universal Vector Engine with Global UI Lockout."""
        # Check file size (approximate)
        file_size_mb = os.path.getsize(path) / (1024 * 1024)
        
        if file_size_mb > 50:
            self.show_loading_curtain(f"Rendering {file_size_mb:.1f}MB Vector...")
        
        threading.Thread(target=self._threaded_vector_render, args=(path,), daemon=True).start()

    def _threaded_vector_render(self, path):
        """Optimized render: Draws boundaries in background, defers labels to zoom events."""
        try:
            import shapely
            # STRUCTURED STORAGE: Separates fixed polygons from dynamic label data
            layer_data = {
                "polygons": [],     # Actual boundary objects on map
                "label_data": [],   # Raw (lat, lon, text) tuples
                "active_labels": [] # Marker objects currently visible
            }
            
            self.log(f"Processing large vector: {os.path.basename(path)}... Please wait.")

            # 1. Load the file
            gdf = gpd.read_file(path)
            if gdf.crs and gdf.crs.to_string() != "EPSG:4326":
                gdf = gdf.to_crs("EPSG:4326")
            
            bounds = gdf.total_bounds 
            
            # --- PERFORMANCE BOOST: SIMPLIFICATION ---
            if len(gdf) > 1000:
                self.log("Large dataset detected: Simplifying geometry for performance...")
                gdf['geometry'] = gdf['geometry'].simplify(0.001, preserve_topology=True)

            # 2. Label logic
            potential_cols = ['name', 'label', 'ID', 'Name', 'id', 'crop_type']
            label_col = next((c for c in potential_cols if c in gdf.columns), None)

            # 3. Draw Boundaries & Store Label Data
            for i, (_, row) in enumerate(gdf.iterrows()):
                geom = row.geometry
                if geom is None: continue

                # A. Queue Boundaries for Main Thread
                sub_geoms = [geom] if geom.geom_type == 'Polygon' else list(geom.geoms)
                for poly in sub_geoms:
                    if hasattr(poly, 'exterior'):
                        coords = [(lat, lon) for lon, lat in poly.exterior.coords]
                        self.root.after(0, self._ui_draw_polygon, coords, layer_data["polygons"])

                # B. Store Label Coordinates (Don't draw yet!)
                if label_col and row[label_col]:
                    center = geom.centroid
                    layer_data["label_data"].append((center.y, center.x, str(row[label_col])))

            # Store the structured data
            self.active_layer_polygons[path] = layer_data
            
            # 4. Finalize UI
            # Only auto-zoom if this is a single file load, NOT a batch load
            if not getattr(self, 'is_batch_loading', False):
                if len(self.active_layer_polygons) <= 1:
                    self.root.after(0, lambda: self.map_widget.fit_bounding_box((bounds[3], bounds[0]), (bounds[1], bounds[2])))
            
            self.root.after(500, self.refresh_label_visibility) 
            self.log(f"SUCCESS: Layer {os.path.basename(path)} ready.")
            
            # THE FIX: Tell the Navigator to update its checkboxes NOW
            self.root.after(0, self.populate_layers_tree) 
            
            self.root.after(0, self.hide_loading_curtain)

        except Exception as e:
            self.log(f"Vector Error: {e}")
            self.root.after(0, self.hide_loading_curtain) # Ensure splash hides on error

    def refresh_label_visibility(self, event=None):
        """Manages labels based on Zoom Level and Map Viewport with version safety."""
        if not hasattr(self, 'active_layer_polygons'): return
        
        current_zoom = self.map_widget.zoom
        threshold = 14 
        
        # --- VERSION SAFETY CHECK: Find the correct bounds method ---
        m_view = None
        for method_name in ['get_bounds', 'get_position_bounds', 'get_bounding_box']:
            if hasattr(self.map_widget, method_name):
                m_view = getattr(self.map_widget, method_name)()
                break
        
        if not m_view:
            # Fallback: if library methods fail, don't crash the whole app
            return
        
        # Extract coordinates based on standard (min_lat, min_lon, max_lat, max_lon)
        # Note: some versions return (lat1, lon1, lat2, lon2), we handle both
        v_min_lat, v_min_lon, v_max_lat, v_max_lon = m_view[0], m_view[1], m_view[2], m_view[3]

        for path, data in self.active_layer_polygons.items():
            # Ensure data is the new structured dictionary format
            if not isinstance(data, dict) or "label_data" not in data: continue
            
            # CASE A: Too far out -> Wipe labels
            if current_zoom < threshold:
                if data.get("active_labels"):
                    for lbl in data["active_labels"]: 
                        try: lbl.delete()
                        except: pass
                    data["active_labels"] = []
            
            # CASE B: Zoomed in -> Draw labels only for what's currently on-screen
            else:
                # 1. Clear old labels to prevent duplicates
                for lbl in data.get("active_labels", []):
                    try: lbl.delete()
                    except: pass
                data["active_labels"] = []
                
                # 2. Draw new ones (Capped for performance)
                count = 0
                for lat, lon, text in data["label_data"]:
                    # Viewport Filter: check if point is within current visible square
                    if v_min_lat <= lat <= v_max_lat and v_min_lon <= lon <= v_max_lon:
                        lbl = self.map_widget.set_marker(lat, lon, text=text)
                        data["active_labels"].append(lbl)
                        count += 1
                    if count > 150: break # Safety cap to keep the 300MB file smooth

    def _ui_draw_polygon(self, coords, storage_list):
        obj = self.map_widget.set_polygon(coords, outline_color="yellow", border_width=1, fill_color=None)
        storage_list.append(obj)

    def _ui_draw_label(self, lat, lon, text, storage_list):
        lbl = self.map_widget.set_marker(lat, lon, text=text)
        storage_list.append(lbl)

    def show_pixel_metadata(self, event):
        """Metadata Inspector: Displays raw band values on hover."""
        if not hasattr(self, 'current_preview_array'): return
        
        # Get dimensions of the display label
        lbl_w = self.preview_label.winfo_width()
        lbl_h = self.preview_label.winfo_height()
        
        # Get dimensions of the loaded data array
        _, img_h, img_w = self.current_preview_array.shape
        
        # Map mouse position to array indices
        px_x = int((event.x / lbl_w) * img_w)
        px_y = int((event.y / lbl_h) * img_h)
        
        # Stay within bounds
        if 0 <= px_x < img_w and 0 <= px_y < img_h:
            # Extract values for all bands at this point
            vals = self.current_preview_array[:, px_y, px_x]
            
            if len(vals) >= 3:
                info = f"Red: {vals[0]} | Green: {vals[1]} | Blue: {vals[2]}"
            else:
                info = f"Value: {vals[0]}"
                
            self.inspector_label.config(text=f"Pixel [{px_x}, {px_y}] | {info}")

    def show_table_context_menu(self, event):
        iid = self.record_table.identify_row(event.y)
        if iid:
            # If the user right-clicks a row not currently selected, select it
            if iid not in self.record_table.selection():
                self.record_table.selection_set(iid)
                
            menu = tk.Menu(self.root, tearoff=0)
            menu.add_command(label="🗺️ Preview Satellite Image", command=self.overlay_on_map)
            menu.add_command(label="🔍 Preview in Popup Window", command=self.preview_image)
            menu.add_separator()
            menu.add_command(label="📂 Open File Location", command=self.open_file_folder)
            menu.add_separator()
            # NEW: The Remove Option
            menu.add_command(label="🗑️ Remove Selected from History", 
                             command=self.remove_selected_records, 
                             foreground="red")
            menu.post(event.x_root, event.y_root)

    def open_file_folder(self):
        item = self.record_table.selection()
        # FIXED: Index changed from 5 to 6
        path = self.record_table.item(item, "values")[6] 
        folder = os.path.dirname(path)
        if os.path.exists(folder):
            os.startfile(folder)

    def on_record_double_click(self, event):
        """Fix: Moves map FIRST, then draws the Cyan box after a slight delay."""
        item = self.record_table.selection()
        if not item: return
        values = self.record_table.item(item, "values")
        path = values[6]
        
        try:
            with rasterio.open(path) as s: 
                b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
            
            # 1. Move the map
            self.map_widget.fit_bounding_box((b[3], b[0]), (b[1], b[2]))
            
            # 2. Wait 250ms for the map to 'settle' so it doesn't wipe the drawing
            self.root.after(250, lambda: self._draw_roi(b[0], b[1], b[2], b[3]))
            
            self.manual_roi_bounds = (b[0], b[1], b[2], b[3])
            self._calculate_aoi_hectares(b)
        except Exception as e: self.log(f"Nav Error: {e}")

    # --- Mapping Interaction ---
    def track_movement(self, event):
        """Throttled coordinate tracking to keep map response snappy."""
        try:
            # We only calculate coordinates if the map is actually visible
            if self.map_widget.winfo_viewable():
                lat, lon = self.map_widget.convert_canvas_coords_to_decimal_coords(event.x, event.y)
                # Update the label without blocking the map redraw thread
                self.label_coords.config(text=f"Lat: {lat:.4f}, Lon: {lon:.4f}")
        except: 
            pass

    def handle_manual_click(self, coords):
        # 1. AUTO-ENABLE: If the user clicked "Mark Corner", they clearly want to annotate.
        if not self.manual_annotate_mode:
            self.manual_annotate_mode = True
            
            # THE FIX: Also update the BooleanVar so the button stays "pushed" visually
            self.draw_mode_active.set(True) 
            
            # Now this line will work because self.btn_manual was defined in create_widgets
            self.btn_manual.config(text="Manual Mode: ON", bg="#005555", fg="white")
            self.log("Manual Annotate Mode auto-enabled by corner selection.")

        # 2. PROCEED WITH MARKING:
        if self.temp_start_coords is None:
            # First click: Set the first corner
            self.temp_start_coords = coords
            self.log(f"Point 1 Set: {coords[0]:.5f}, {coords[1]:.5f}. Now mark the opposite corner.")
            # Add a small visual marker so the user knows where they clicked
            self.map_widget.set_marker(coords[0], coords[1], text="Point 1")
        else:
            # Second click: We have two points, let's build the box!
            p1 = self.temp_start_coords
            p2 = coords
            
            miny, maxy = sorted([p1[0], p2[0]])
            minx, maxx = sorted([p1[1], p2[1]])
            
            # --- THE FIX: Actually save the bounds for the downloader ---
            self.manual_roi_bounds = (minx, miny, maxx, maxy)
            self._evaluate_download_button() # Enable download immediately
            # -------------------------------------------------------------

            self._draw_roi(minx, miny, maxx, maxy)
            self._calculate_aoi_hectares([minx, miny, maxx, maxy]) # Update the HUD
            
            self.temp_start_coords = None
            self.map_widget.delete_all_marker()
            self.log(f"AOI Finalized: {maxx-minx:.4f}° width x {maxy-miny:.4f}° height")

    def clear_temp_markers(self):
        for m in self.temp_markers: m.delete()
        self.temp_markers = []

    def clear_all(self):
        """Universal panic button to wipe all drawings and kill raster references safely."""
        with self.raster_lock:
            # 1. Explicitly clear all active raster images
            if hasattr(self, 'active_rasters'):
                for path, raster in list(self.active_rasters.items()):
                    self.map_widget.canvas.delete(raster["img_item"])
                    try: raster["box_obj"].delete()
                    except: pass
                    # THE FIX: Nullify the reference so garbage collection is clean
                    raster["photo_img"] = None 
                    raster["master_img"] = None
                self.active_rasters.clear()
        
        # 2. Clear manual drawings and UI states
        if self.roi_polygon: self.roi_polygon.delete()
        self.roi_polygon = None
        self.manual_roi_bounds = None
        self.input_file_path.set("")
        self.aoi_bounds_label.config(text="No AOI Drawn", bg="#333333", fg="gray")

        # 3. Clear Vectors
        if hasattr(self, 'active_layer_polygons'):
            for path, items in list(self.active_layer_polygons.items()):
                if isinstance(items, dict):
                    for poly in items.get("polygons", []):
                        try: poly.delete()
                        except: pass
                    for lbl in items.get("active_labels", []):
                        try: lbl.delete()
                        except: pass
            self.active_layer_polygons.clear()
            
        self.populate_layers_tree()
        self._evaluate_download_button()
        self.log("UI and Map overlays completely cleared.")

    def _draw_roi(self, minx, miny, maxx, maxy):
        if self.roi_polygon: self.roi_polygon.delete()
        c = [(maxy, minx), (maxy, maxx), (miny, maxx), (miny, minx)]
        self.roi_polygon = self.map_widget.set_polygon(c, outline_color="cyan", border_width=3, fill_color=None)

        # --- THE NEW VISUALIZATION HUD ---
        # Format the coordinates to 5 decimal places so they align nicely
        hud_text = (
            f"          maxy (Top Lat): {maxy:.5f}\n"
            f"minx (L-Lon): {minx:.5f}  |  maxx (R-Lon): {maxx:.5f}\n"
            f"          miny (Bot Lat): {miny:.5f}"
        )
        # Turn the label cyan so it matches the bounding box color!
        self.aoi_bounds_label.config(text=hud_text, bg="#1a1a1a", fg="#00ffff")

    def browse_file(self):
        # Added *.geojson to the list
        fn = filedialog.askopenfilename(filetypes=[
            ("Spatial Files", "*.shp *.tif *.geojson"), 
            ("GeoJSON", "*.geojson"),
            ("Shapefile", "*.shp"),
            ("GeoTIFF", "*.tif"),
            ("All Files", "*.*")
        ])
        if fn:
            self.input_file_path.set(fn)
            threading.Thread(target=self._update_map, args=(fn,), daemon=True).start()

    def _handle_aoi_placeholder(self, *args):
        """Manages the grey placeholder text in the AOI entry."""
        current_val = self.input_file_path.get()
        placeholder = "-- Browse .SHP / .GeoJson / .TIFF --"

        if current_val == placeholder or not current_val:
            self.aoi_entry.config(foreground="grey")
        else:
            self.aoi_entry.config(foreground="black")
            
        self._evaluate_download_button() # <--- THE FIX

    def _update_map(self, path):
        if path == "-- Browse .SHP / .GeoJson / .TIFF --" or not path:
            return
        try:
            if path.endswith('.shp') or path.endswith('.geojson'): b = gpd.read_file(path).to_crs("EPSG:4326").total_bounds
            else: 
                with rasterio.open(path) as s: b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
            self.root.after(0, lambda: self._draw_roi(*b))
            self.root.after(0, lambda: self.map_widget.fit_bounding_box((b[3], b[0]), (b[1], b[2])))
        except Exception as e: self.log(f"Mapping Error: {e}")

    def check_auth_status(self):
        # 1. Get the "Live" text directly from the entry
        pid = self.project_id_var.get().strip()
        if not pid: return

        def run_validation():
            try:
                # Force Initialize with the typed ID
                ee.Initialize(project=pid)
                
                # THE TRUTH TEST: Request metadata from GEE
                # If '...u' is wrong, this is where the code jumps to 'except'
                ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").limit(1).getInfo()
                
                # SUCCESS PATH
                self.save_project_id(pid)
                self.root.after(0, lambda: self._update_connection_ui(True, pid))

            except Exception as e:
                # FAILURE PATH: Convert error to a safe string immediately
                error_detail = str(e).split('\n')[0] # Get only the first line of the error
                self.root.after(0, lambda: self._update_connection_ui(False, pid, error_detail))

        # Run the validation in a background thread to keep UI snappy
        threading.Thread(target=run_validation, daemon=True).start()

    def _update_connection_ui(self, success, pid, error_msg=None):
        """Dedicated UI updater to ensure logs always print (No collapsing)."""
        if success:
            self.lbl_status.config(text=f"✅ Status: Connected to {pid}", foreground="#2a9d8f")
            self.gee_connected = True
            self._evaluate_download_button() # <--- THE FIX
            self.log(f"Verified & Connected: {pid}")
        else:
            self.lbl_status.config(text="❌ Status: Connection Failed", foreground="#d62828")
            self.gee_connected = False
            self._evaluate_download_button() # <--- THE FIX
            self.log(f"Invalid Project ID [{pid}]: {error_msg}")

    def reset_connection_view(self):
        """Expands the connection UI to allow changing the project ID."""
        # Hide the Compact View
        self.f1_compact.pack_forget()
        
        # Restore the Full View
        self.f1_full.pack(fill="x")
        self.lbl_status.config(text="Status: Ready", foreground="gray")
        self.lbl_status.pack(anchor="w", pady=(2, 0))
        
        # Disable download until they reconnect with the new ID
        self.btn_download.config(state="disabled")
        self.log("Connection view expanded. Please connect to a new Project ID.")
            
    def run_authentication(self):
        self.log("Opening authentication browser...")
        threading.Thread(target=lambda: (ee.Authenticate(auth_mode='localhost'), self.check_auth_status()), daemon=True).start()

    def find_available_dates(self):
        # 1. Capture current values from the UI (IN THE MAIN THREAD)
        path = self.input_file_path.get()
        m_roi = self.manual_roi_bounds
        raw_date_text = self.target_date_var.get().strip()
        clean_date_text = raw_date_text[:10]
        
        # ---> THE THREAD-SAFETY FIX: Extract Tkinter variables HERE <---
        ds_name = self.dataset_var.get()
        sort_clouds = self.sort_by_cloud_var.get()

        has_file = bool(path and not path.startswith("--"))
        has_manual = m_roi is not None

        # 2. VALIDATION: Do not start thread if data is missing
        if not has_file and not has_manual:
            messagebox.showwarning("Missing Area", 
                                   "Please annotate an Area of Interest (AOI) or load a spatial file first.\n\n"
                                   "Satellites pass over different areas on different days!")
            return

        try:
            target_date = datetime.strptime(clean_date_text, "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Invalid Date", "Please format the date exactly as YYYY-MM-DD.")
            return

        # 3. UI RESET: Clear stale dates and show scanning status
        self.available_dates_dropdown.config(values=[])
        self.available_dates_dropdown.set("Scanning Earth Engine servers. Please wait...")
        
        # 4. Start the background worker (passing the safe variables)
        valid_path = path if has_file else None
        threading.Thread(target=self._search_dates_worker, 
                         args=(valid_path, m_roi, target_date, ds_name, sort_clouds), 
                         daemon=True).start()

    def _search_dates_worker(self, path, m_roi, target_date, ds, sort_clouds):
        try:
            # --- 0. UI RESET ---
            self.root.after(0, lambda: self.available_dates_dropdown.config(values=[]))

            # 1. Setup AOI
            if path:
                if path.endswith('.shp') or path.endswith('.geojson'): 
                    b = gpd.read_file(path).to_crs("EPSG:4326").total_bounds
                else: 
                    with rasterio.open(path) as s: 
                        b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
            else: 
                b = m_roi

            roi = ee.Geometry.Rectangle([b[0], b[1], b[2], b[3]])
            self.log(f"Scanning for {ds} near {target_date.strftime('%Y-%m-%d')}...")

            # 2. Set Dynamic Window (+/- 90 days for a 180-day window)
            start_date = (target_date - timedelta(days=90)).strftime('%Y-%m-%d')
            end_date = (target_date + timedelta(days=90)).strftime('%Y-%m-%d')

            # 3. Fetch Collection
            def get_info(img):
                d = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd HH:mm')
                if "Sentinel-2" in ds:
                    c = ee.Number(img.get('CLOUDY_PIXEL_PERCENTAGE')).format('%.1f')
                    return ee.Feature(None, {'info': ee.String(d).cat(' (Cloud: ').cat(c).cat('%)'), 'raw_date': d})
                elif "Landsat" in ds:
                    c = ee.Number(img.get('CLOUD_COVER')).format('%.1f')
                    return ee.Feature(None, {'info': ee.String(d).cat(' (Cloud: ').cat(c).cat('%)'), 'raw_date': d})
                else:
                    return ee.Feature(None, {'info': d, 'raw_date': d})

            if "Sentinel-2" in ds:
                col = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterBounds(roi).filterDate(start_date, end_date)
            elif "Landsat" in ds:
                l8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").filterBounds(roi).filterDate(start_date, end_date)
                l9 = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").filterBounds(roi).filterDate(start_date, end_date)
                col = l8.merge(l9)
            else:
                col = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(roi).filterDate(start_date, end_date)

            info_list = col.map(get_info).aggregate_array('info').getInfo()
            raw_dates = col.map(get_info).aggregate_array('raw_date').getInfo()

            if not info_list:
                self.root.after(0, lambda: (
                    self.available_dates_dropdown.set("No passes found in window."),
                    self.available_dates_dropdown.config(values=[]),
                    self.log("No passes found in window.")
                ))
                return

            # --- 4. CLEAN LIST & ADD TIMELINE LABELS ---
            combined = []
            target_dt_only = target_date.date()

            for info, raw in zip(info_list, raw_dates):
                full_timestamp = str(raw)[:16]
                pass_dt_only = datetime.strptime(full_timestamp[:10], "%Y-%m-%d").date()
                
                pass_dt = datetime.strptime(full_timestamp, "%Y-%m-%d %H:%M")
                target_noon = target_date.replace(hour=12, minute=0)
                proximity = abs((pass_dt - target_noon).total_seconds())

                if pass_dt_only < target_dt_only:
                    base_label = " (PAST)"
                elif pass_dt_only > target_dt_only:
                    base_label = " (FUTURE)"
                else:
                    base_label = " (TARGET)"

                combined.append((info, full_timestamp, base_label, proximity))
            
            # Remove exact duplicates by timestamp
            unique_dict = {}
            for item in combined:
                if item[1] not in unique_dict:
                    unique_dict[item[1]] = item
            unique_combined = list(unique_dict.values())

            # Find the absolute closest pass across the entire +/- 90 day window
            closest_item = min(unique_combined, key=lambda x: x[3])
            closest_ts = closest_item[1]

            final_list = []
            for info, ts, label, prox in unique_combined:
                # If there's no exact TARGET, highlight the CLOSEST one instead
                if ts == closest_ts and label != " (TARGET)":
                    label = " (CLOSEST MATCH)"
                
                display_text = f"{info}{label}"
                final_list.append((display_text, prox))

            # --- 5. CATEGORICAL HIERARCHY SORT ---
            def master_sort_key(item):
                display_text, proximity = item
                
                # 1. Assign Rank (0 is top, 3 is bottom)
                if "(TARGET)" in display_text: rank = 0
                elif "(CLOSEST MATCH)" in display_text: rank = 1
                elif "(FUTURE)" in display_text: rank = 2
                else: rank = 3 # (PAST)
                
                # 2. Extract Clouds for secondary sorting
                cloud_val = 100
                if sort_clouds and "Cloud: " in display_text:
                    try:
                        cloud_val = float(display_text.split("Cloud: ")[1].split("%")[0])
                    except: pass 

                if sort_clouds:
                    # Sort by Rank, then Cloud quality, then Proximity
                    return (rank, cloud_val, proximity)
                # Sort by Rank, then Proximity
                return (rank, proximity)

            final_list.sort(key=master_sort_key)
            display_values = [x[0] for x in final_list]

            # --- 6. UPDATE UI ---
            def update_ui():
                if display_values:
                    self.available_dates_dropdown.config(values=display_values)
                    self.available_dates_dropdown.set(display_values[0])
                    # Auto-expand the dropdown!
                    self.available_dates_dropdown.focus_set()
                    self.available_dates_dropdown.event_generate('<Down>')
                
            self.root.after(0, update_ui)
            self.log(f"Found {len(display_values)} unique passes with timestamps.")

        except Exception as e:
            self.log(f"Scanner Error: {e}")
            self.root.after(0, lambda: self.available_dates_dropdown.set("Error during scan. Check logs."))

    def _pick_alternative_pass(self, schedule_idx, callback):
        """Allows choosing between Target, Past, and Future candidates for a specific month."""
        data = self.temp_schedule[schedule_idx]
        if not data['all_options']: return

        pick_win = tk.Toplevel(self.root)
        pick_win.title(f"Options for {data['target']}")
        pick_win.geometry("400x300")
        pick_win.grab_set()

        lb = tk.Listbox(pick_win, font=("Consolas", 9))
        lb.pack(fill="both", expand=True, padx=10, pady=10)

        target_dt = datetime.strptime(data['target'], "%Y-%m-%d").date()
        for opt in data['all_options']:
            opt_dt = datetime.strptime(opt['date'][:10], "%Y-%m-%d").date()
            indicator = "(TARGET)" if opt_dt == target_dt else "(PAST)" if opt_dt < target_dt else "(FUTURE)"
            lb.insert(tk.END, f"{opt['date']} | {opt['clouds']}% {indicator}")

        def select():
            idx = lb.curselection()
            if idx:
                selected = data['all_options'][idx[0]]
                self.temp_schedule[schedule_idx]['actual'], self.temp_schedule[schedule_idx]['clouds'] = selected['date'], f"{selected['clouds']}%"
                callback(); pick_win.destroy()

        ttk.Button(pick_win, text="Select This Date", command=select).pack(pady=10)

    def sync_all_to_db(self):
        """Pushes every record from your RECORDS_FILE (CSV) into PostGIS."""
        if not os.path.exists(RECORDS_FILE):
            messagebox.showerror("Error", "No download history found to sync.")
            return
            
        self.log("Database: Starting bulk sync from CSV...")
        
        def run_sync():
            count = 0
            with open(RECORDS_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 'row' matches the 'meta' format exactly
                    self.db_manager.push_metadata(row, self.log)
                    count += 1
            self.log(f"Database: Bulk sync complete. {count} rows processed.")
            messagebox.showinfo("Sync Complete", f"Successfully processed {count} records into ai4caf_db.")

        threading.Thread(target=run_sync, daemon=True).start()

    def start_download_thread(self):
        """PSA-Grade Downloader: Features Live Scouting and 180-day 'Freedom' Dropdowns."""
        path, m_roi = self.input_file_path.get(), self.manual_roi_bounds
        
        # --- 1. SPATIAL VALIDATION ---
        if not path and not m_roi: 
            messagebox.showwarning("Missing Area", "Please annotate an AOI first.")
            return

        self.db_manager.update_params(
            self.db_host.get(), 
            self.db_name.get(), 
            self.db_user.get(), 
            self.db_pass.get()
        )

        # THE CRITICAL RESTORE: Defining 'b' so 'roi' doesn't crash
        try:
            if path and not path.startswith("--"):
                if path.endswith('.shp') or path.endswith('.geojson'): 
                    b = gpd.read_file(path).to_crs("EPSG:4326").total_bounds
                else: 
                    with rasterio.open(path) as s: b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
            else: 
                b = m_roi 
            
            # Now 'b' exists, so 'roi' is safe
            roi = ee.Geometry.Rectangle([b[0], b[1], b[2], b[3]])
        except Exception as e:
            messagebox.showerror("AOI Error", f"Could not determine area: {e}")
            return

        # --- 2. TIME-SERIES SCOUTING WITH LIVE FEED ---
        is_ts = self.timeseries_mode.get()
        if is_ts:
            target_intervals = self._generate_date_series()
            scouted_schedule = []
            
            # Start the Live Monitor Popup
            prog_win, safe_log = self._show_live_scout_progress(target_intervals)

            for idx, t_date in enumerate(target_intervals):
                # EXIT SCAN IF WINDOW CLOSED
                if not prog_win.winfo_exists(): 
                    self.log("Scouting aborted by user.")
                    return

                dt = datetime.strptime(t_date, "%Y-%m-%d")
                self.root.after(0, lambda d=t_date: safe_log(idx, f"Scouting window for {d}..."))
                
                # Extended 180-day search (+/- 90 days)
                start = (dt - timedelta(days=90)).strftime('%Y-%m-%d')
                end = (dt + timedelta(days=90)).strftime('%Y-%m-%d')
                
                # Dynamic Dataset Selection for the Scout
                ds = self.dataset_var.get()
                if "Sentinel-2" in ds:
                    col = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED").filterBounds(roi).filterDate(start, end)
                    cloud_tag = 'CLOUDY_PIXEL_PERCENTAGE'
                elif "Landsat" in ds:
                    l8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").filterBounds(roi).filterDate(start, end)
                    l9 = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").filterBounds(roi).filterDate(start, end)
                    col = l8.merge(l9)
                    cloud_tag = 'CLOUD_COVER'
                else:
                    col = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(roi).filterDate(start, end)
                    cloud_tag = None

                def get_metadata(img):
                    d = ee.Date(img.get('system:time_start')).format('YYYY-MM-dd HH:mm')
                    c = ee.Number(img.get(cloud_tag)).format('%.1f') if cloud_tag else "Radar"
                    return ee.Feature(None, {'date': d, 'clouds': c})

                try:
                    # Capture EE info
                    raw = col.map(get_metadata).getInfo()['features']
                    candidates = [f['properties'] for f in raw]
                    
                    if candidates:
                        target_dt_only = dt.date()
                        target_noon = dt.replace(hour=12, minute=0)
                        
                        processed_candidates = []
                        for c in candidates:
                            pass_dt = datetime.strptime(c['date'], "%Y-%m-%d %H:%M")
                            pass_dt_only = pass_dt.date()
                            proximity = abs((pass_dt - target_noon).total_seconds())
                            
                            if pass_dt_only < target_dt_only:
                                base_label = " (PAST)"
                            elif pass_dt_only > target_dt_only:
                                base_label = " (FUTURE)"
                            else:
                                base_label = " (TARGET)"
                                
                            processed_candidates.append({
                                'date': c['date'],
                                'clouds': c['clouds'],
                                'label': base_label,
                                'proximity': proximity
                            })
                            
                        # Sort by proximity to target to find the absolute closest
                        processed_candidates.sort(key=lambda x: x['proximity'])
                        
                        # Identify closest
                        closest = processed_candidates[0]
                        if closest['label'] != " (TARGET)":
                            closest['label'] = " (CLOSEST MATCH)"

                        # --- THE CATEGORICAL HIERARCHY SORT ---
                        sort_clouds = self.sort_by_cloud_var.get()
                        
                        def master_sort(item):
                            # 1. Strict Rank Assignment
                            if "(TARGET)" in item['label']: rank = 0
                            elif "(CLOSEST MATCH)" in item['label']: rank = 1
                            elif "(FUTURE)" in item['label']: rank = 2
                            else: rank = 3 # (PAST)
                                
                            # 2. Extract Clouds safely
                            try: cloud_val = float(item['clouds'])
                            except: cloud_val = 100
                            
                            # 3. Apply sorting
                            if sort_clouds:
                                return (rank, cloud_val, item['proximity'])
                            else:
                                return (rank, item['proximity'])

                        # Apply the master sort
                        processed_candidates.sort(key=master_sort)

                        best = processed_candidates[0]
                        scouted_schedule.append({
                            'target': t_date, 'actual': best['date'], 
                            'current_selection': f"{best['date']} | {best['clouds']}% {best['label']}",
                            'options': [f"{c['date']} | {c['clouds']}% {c['label']}" for c in processed_candidates]
                        })
                    else:
                        scouted_schedule.append({'target': t_date, 'actual': "None", 'options': []})
                        
                except Exception as ee_err:
                    self.root.after(0, lambda e=ee_err: safe_log(idx, f"EE ERROR: {str(e)[:50]}..."))
                    scouted_schedule.append({'target': t_date, 'actual': "None", 'options': []})

            # Close the scout window only if it's still open
            if prog_win.winfo_exists(): prog_win.destroy()
            
            # Show the Dropdown Review Table
            date_list = self._show_batch_review_window(scouted_schedule)
            if not date_list: return
            display_date = date_list[0]
            confirm_msg = f"Start batch download for {len(date_list)} dates?"
        else:
            # Mono-temporal Mode
            selected_date_text = self.selected_date_var.get()
            if not selected_date_text or "Scanning" in selected_date_text:
                messagebox.showwarning("Select Date", "Please select a specific date from the dropdown.")
                return
            display_date = selected_date_text[:16]
            date_list = [display_date]
            confirm_msg = f"Download image for {display_date}?"

        if not messagebox.askyesno("Confirm Download", confirm_msg): return

        safe_date_for_file = display_date.replace(" ", "_").replace(":", "-")

        # --- 3. PROFESSIONAL NAMING LOGIC ---
        ds = self.dataset_var.get()
        sensor_tag = "S2" if "Sentinel-2" in ds else "L89" if "Landsat" in ds else "S1" if "Sentinel-1" in ds else "SRTM"
        
        # [Preserve your existing band_tag assembly logic here...]
        bands_selected = []
        if "Sentinel-2" in ds:
            if self.s2_bands["Red (B4)"].get() and self.s2_bands["Green (B3)"].get() and self.s2_bands["Blue (B2)"].get(): bands_selected.append("RGB")
        elif "Landsat" in ds:
            if self.l8_bands["Red (B4)"].get() and self.l8_bands["Green (B3)"].get() and self.l8_bands["Blue (B2)"].get(): bands_selected.append("RGB")
        elif "Sentinel-1" in ds:
            if self.s1_bands["VV (Vertical)"].get(): bands_selected.append("VV")
            if self.s1_bands["VH (Horizontal)"].get(): bands_selected.append("VH")
        
        for idx_name, var in self.index_vars.items():
            if var.get(): bands_selected.append(idx_name)
        band_tag = "-".join(bands_selected) if bands_selected else "Data"

        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        mode_prefix = "TS" if is_ts else "Mono"
        suggested_name = f"Area_{mode_prefix}_{sensor_tag}_{safe_date_for_file}_{band_tag}_{timestamp_str}"

        # --- 4. PROJECT SETUP & THREAD START ---
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, "Data")
        os.makedirs(data_dir, exist_ok=True)
        
        save_p = filedialog.asksaveasfilename(title="Create Project Workspace", initialdir=data_dir, initialfile=suggested_name)
        
        if save_p:
            self.notebook.select(self.tab_downloads)
            self.progress_bar.config(mode='indeterminate')
            self.progress_bar.start()

            # --- NEW: Reset Cancel Flag and Enable Button ---
            self.cancel_download_flag = False
            self.btn_cancel_dl.config(state="normal", text="🛑 Cancel Download")

            threading.Thread(target=self._download_worker, args=(path, save_p, date_list), daemon=True).start()

    def _download_worker(self, path, save_p, date_list):
        """High-Fidelity Batch Downloader: Preserves all swath-checks and stitching logic."""
        try:
            import tempfile
            import shutil
            from rasterio.merge import merge
            import shapely.geometry

            # 1. ROOT WORKSPACE SETUP
            target_dir = os.path.dirname(save_p)
            raw_project_name = os.path.basename(save_p)
            original_project_name = raw_project_name.replace(":", "-").replace(" ", "_")
            # Create a main project folder to hold all timestamps
            project_root_folder = os.path.join(target_dir, original_project_name)
            os.makedirs(project_root_folder, exist_ok=True)

            is_timeseries = len(date_list) > 1

            # 2. AOI GEOMETRY (Calculated once for the whole batch)
            if path and not path.startswith("--"):
                if path.endswith('.shp') or path.endswith('.geojson'): 
                    b = gpd.read_file(path).to_crs("EPSG:4326").total_bounds
                else: 
                    with rasterio.open(path) as s: b = transform_bounds(s.crs, 'EPSG:4326', *s.bounds)
            else: b = self.manual_roi_bounds
            
            minx, miny, maxx, maxy = b[0], b[1], b[2], b[3]
            roi = ee.Geometry.Rectangle([minx, miny, maxx, maxy])
            
            # Save the master AOI GeoJSON in the root folder
            aoi_path = os.path.join(project_root_folder, f"{original_project_name}_AOI.geojson")
            geom = shapely.geometry.box(minx, miny, maxx, maxy)
            gpd.GeoDataFrame({'id': [1], 'geometry': [geom]}, crs="EPSG:4326").to_file(aoi_path, driver="GeoJSON")

            ds_name = self.dataset_var.get()
            target_crs = self.target_crs_var.get().split(" ")[0]
            do_stitch = self.stitch_tiles_var.get()
            
            # 3. GRID CALCULATOR (Calculated once)
            STEP = 0.1 
            x_edges, curr_x = [], minx
            while curr_x < maxx: x_edges.append(curr_x); curr_x += STEP
            if x_edges and x_edges[-1] < maxx: x_edges.append(maxx)
            y_edges, curr_y = [], miny
            while curr_y < maxy: y_edges.append(curr_y); curr_y += STEP
            if y_edges and y_edges[-1] < maxy: y_edges.append(maxy)
            total_tiles_per_date = (len(x_edges) - 1) * (len(y_edges) - 1)

            # ==========================================
            # --- THE BATCH TEMPORAL LOOP ---
            # ==========================================
            for date_idx, current_target_date in enumerate(date_list):
                if getattr(self, 'cancel_download_flag', False): break

                self.root.after(0, lambda: self.progress_bar.config(mode='determinate'))

                safe_date_filename = current_target_date.replace(" ", "_").replace(":", "-")
                
                self.log(f"PROGRESS: Starting Date {date_idx+1}/{len(date_list)} -> {current_target_date}")
                
                # --- THE FLAT FOLDER FIX ---
                if is_timeseries:
                    # Multi-date: Create subfolders for organization
                    date_folder_name = f"Data_{safe_date_filename}"
                    current_date_dir = os.path.join(project_root_folder, date_folder_name)
                    os.makedirs(current_date_dir, exist_ok=True)
                else:
                    # Single-date: Save directly in the project root
                    current_date_dir = project_root_folder

                row_iid = f"dl_{date_idx}_{int(time.time())}"
                self.root.after(0, lambda rid=row_iid, dt=current_target_date: 
                    self.record_table.insert("", 0, iid=rid, values=("🔄 Downloading...", dt, f"[{original_project_name}] {dt}", ds_name, target_crs, f"Rect({miny:.2f}, {minx:.2f})", "Pending...")))
                
                # BUILD GEE IMAGE 
                target_dt = datetime.strptime(current_target_date[:10], "%Y-%m-%d")
                # d_start = (target_dt - timedelta(days=1)).strftime("%Y-%m-%d")
                # d_end = (target_dt + timedelta(days=2)).strftime("%Y-%m-%d")
                d_start = target_dt.strftime("%Y-%m-%d")
                d_end = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d")

                # Dataset Selection Logic (Preserved)
                if "Sentinel-2" in ds_name:
                    col = ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED") \
                            .filterBounds(roi) \
                            .filterDate(d_start, d_end) \
                            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 20))
                    if col.size().getInfo() == 0: 
                        self.log(f"⚠️ No data found for strictly {current_target_date}. Skipping.")
                        continue
                    img = col.mosaic().clip(roi)
                    mapping = {"Red (B4)":'B4', "Green (B3)":'B3', "Blue (B2)":'B2', "NIR (B8)":'B8', "SWIR 1 (B11)":'B11'}
                    selected = [mapping[n] for n, v in self.s2_bands.items() if v.get()]
                    if self.index_vars["NDVI"].get():
                        img = img.addBands(img.normalizedDifference(['B8', 'B4']).rename('NDVI'))
                        selected.append('NDVI')
                    img = img.select(selected if selected else ['B4', 'B3', 'B2'])
                    scale = 10
                elif "Landsat" in ds_name:
                    l8 = ee.ImageCollection("LANDSAT/LC08/C02/T1_L2").filterBounds(roi).filterDate(d_start, d_end)
                    l9 = ee.ImageCollection("LANDSAT/LC09/C02/T1_L2").filterBounds(roi).filterDate(d_start, d_end)
                    col = l8.merge(l9)
                    if col.size().getInfo() == 0: continue
                    img = col.sort('CLOUD_COVER').mosaic()
                    mapping = {"Red (B4)":'SR_B4', "Green (B3)":'SR_B3', "Blue (B2)":'SR_B2', "NIR (B5)":'SR_B5', "SWIR 1 (B6)":'SR_B6'}
                    selected = [mapping[n] for n, v in self.l8_bands.items() if v.get()]
                    if self.index_vars["NDVI"].get():
                        img = img.addBands(img.normalizedDifference(['SR_B5', 'SR_B4']).rename('NDVI'))
                        selected.append('NDVI')
                    img = img.select(selected if selected else ['SR_B4', 'SR_B3', 'SR_B2'])
                    scale = 30
                elif "Sentinel-1" in ds_name:
                    col = ee.ImageCollection('COPERNICUS/S1_GRD').filterBounds(roi).filterDate(d_start, d_end)
                    if col.size().getInfo() == 0: continue
                    img = col.mosaic()
                    selected = [n for n, v in {"VV":self.s1_bands["VV (Vertical)"], "VH":self.s1_bands["VH (Horizontal)"]}.items() if v.get()]
                    img = img.select(selected if selected else ['VV'])
                    scale = 10
                else: # SRTM
                    img = ee.Image("USGS/SRTMGL1_003").clip(roi)
                    scale = 30

                # --- TILE DOWNLOADER (With Cancellation Engine) ---
                temp_files = []
                tile_count = 0
                cancel_triggered = False 
                
                for i in range(len(x_edges) - 1):
                    if getattr(self, 'cancel_download_flag', False): cancel_triggered = True; break
                    for j in range(len(y_edges) - 1):
                        if getattr(self, 'cancel_download_flag', False): cancel_triggered = True; break
                        
                        tile_count += 1
                        t_roi = ee.Geometry.Rectangle([x_edges[i], y_edges[j], x_edges[i+1], y_edges[j+1]])
                        tile_img = img.clip(t_roi)
                        
                        self.root.after(0, lambda tc=tile_count, td=total_tiles_per_date, dt=current_target_date: 
                                        self.lbl_progress_detail.config(text=f"[{dt}] Tile {tc}/{td}..."))
                        
                        try:
                            pixel_count = tile_img.select(0).reduceRegion(reducer=ee.Reducer.count(), geometry=t_roi, scale=100).getInfo()
                            if not pixel_count or list(pixel_count.values())[0] == 0: continue

                            url = tile_img.getDownloadURL({'scale': scale, 'crs': target_crs, 'region': t_roi, 'format': 'GEO_TIFF'})
                            resp = requests.get(url, stream=True)
                            resp.raise_for_status()
                            
                            tile_filename = f"{original_project_name}_{safe_date_filename}_T{tile_count}.tif"
                            temp_fp = os.path.join(current_date_dir, tile_filename)
                            with open(temp_fp, 'wb') as f:
                                for chunk in resp.iter_content(chunk_size=32768): f.write(chunk)
                            temp_files.append(temp_fp)
                        except Exception as e:
                            self.log(f"Tile Error ({current_target_date}): {e}")

                # --- IF CANCELLED: Cleanup partial files ---
                if cancel_triggered:
                    self.log("Cleaning up partial downloaded files...")
                    for fp in temp_files:
                        if os.path.exists(fp): os.remove(fp)
                    # Update live row to cancelled
                    self.root.after(0, lambda rid=row_iid, dt=current_target_date: 
                        self.record_table.item(rid, values=("❌ Cancelled", dt, f"[{original_project_name}] {dt}", ds_name, target_crs, f"Rect({miny:.2f}, {minx:.2f})", "Aborted")))
                    break # Escape the entire Date Loop

                # --- STITCHER & RECORDING (Preserved) ---
                if temp_files:
                    final_tif_name = f"{original_project_name}_{safe_date_filename}.tif"
                    final_date_path = os.path.join(current_date_dir, final_tif_name)
                    
                    if do_stitch and len(temp_files) > 1:
                        self.root.after(0, lambda: self.lbl_progress_detail.config(text=f"Stitching {current_target_date}..."))
                        src_files = [rasterio.open(fp) for fp in temp_files]
                        mosaic, out_trans = merge(src_files)
                        out_meta = src_files[0].meta.copy()
                        out_meta.update({"driver": "GTiff", "height": mosaic.shape[1], "width": mosaic.shape[2], "transform": out_trans})
                        with rasterio.open(final_date_path, "w", **out_meta) as dest:
                            dest.write(mosaic)
                        for s in src_files: s.close()
                        
                        if not self.keep_tiles_var.get():
                            for fp in temp_files: os.remove(fp) 
                    elif len(temp_files) == 1:
                        shutil.copy(temp_files[0], final_date_path)
                        if not self.keep_tiles_var.get(): os.remove(temp_files[0])

                    meta = {
                        "Date": current_target_date, 
                        "File Name": f"[{original_project_name}] {current_target_date}",
                        "Dataset": ds_name, "CRS": target_crs, 
                        "Bounds": f"Rect({miny:.2f}, {minx:.2f})", "Path": final_date_path
                    }
                    self.save_metadata_to_file(meta)
                    threading.Thread(target=self.db_manager.push_metadata, args=(meta, self.log), daemon=True).start()
                    self.root.after(0, lambda rid=row_iid, m=meta: self.record_table.item(rid, values=("✅ Found", m["Date"], m["File Name"], m["Dataset"], m["CRS"], m["Bounds"], m["Path"])))
                
                batch_pct = ((date_idx + 1) / len(date_list)) * 100
                self.root.after(0, lambda v=batch_pct: self.progress_bar.config(value=v))

            if not getattr(self, 'cancel_download_flag', False):
                self.log(f"SUCCESS: Files saved in {project_root_folder}")
                messagebox.showinfo("Success", f"Task Complete!\nProcessed {len(date_list)} dates.")

        except Exception as e:
            self.log(f"BATCH CRITICAL ERROR: {e}")
            self.root.after(0, lambda err=e: messagebox.showerror("Worker Crash", f"Error: {err}"))
        finally:
            self.root.after(0, lambda: (
                self.btn_download.config(state="normal"), 
                self.btn_cancel_dl.config(state="disabled", text="🛑 Cancel Download"), 
                self.progress_bar.stop(), 
                self.progress_bar.config(mode='determinate', value=0), 
                self.lbl_progress_detail.config(text="Done.")
            ))
if __name__ == "__main__":
    root = tk.Tk()
    app = GEE_Local_Downloader_App(root)
    root.mainloop()