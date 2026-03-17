import requests
import json
import csv
import os
import time

# California Bounding Box
# S, W, N, E
CA_BBOX = (32.5, -124.5, 42.1, -114.1)

OVERPASS_URL = "http://overpass-api.de/api/interpreter"

def get_query(bbox):
    s, w, n, e = bbox
    bbox_str = f"{s},{w},{n},{e}"
    return f"""
    [out:json][timeout:180];
    (
      node["amenity"~"hospital|clinic|doctors|school|college|university|fire_station|police|pharmacy"]({bbox_str});
      way["amenity"~"hospital|clinic|doctors|school|college|university|fire_station|police|pharmacy"]({bbox_str});
      relation["amenity"~"hospital|clinic|doctors|school|college|university|fire_station|police|pharmacy"]({bbox_str});
      
      node["emergency"~"hospital|ambulance_station|fire_station"]({bbox_str});
      way["emergency"~"hospital|ambulance_station|fire_station"]({bbox_str});
      
      node["building"~"hospital|school|university|college|clinic|fire_station"]({bbox_str});
      way["building"~"hospital|school|university|college|clinic|fire_station"]({bbox_str});
    );
    out center;
    """

def fetch_pois():
    print("Fetching California Essential POIs using chunked grid strategy...", flush=True)
    
    # 5x5 grid = 25 chunks
    lat_steps = 5
    lon_steps = 5
    
    s_total, w_total, n_total, e_total = CA_BBOX
    lat_delta = (n_total - s_total) / lat_steps
    lon_delta = (e_total - w_total) / lon_steps
    
    all_elements = []
    
    out_path = os.path.join("data", "riverside_pois.csv") # Kept same name for compatibility
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "lat", "lon", "type", "name"])
        
        for i in range(lat_steps):
            for j in range(lon_steps):
                s = s_total + i * lat_delta
                n = s + lat_delta
                w = w_total + j * lon_delta
                e = w + lon_delta
                
                print(f"--- Chunk [{i+1}/{lat_steps}, {j+1}/{lon_steps}] Area: {s:.2f},{w:.2f} to {n:.2f},{e:.2f} ---", flush=True)
                query = get_query((s, w, n, e))
                
                retries = 3
                success = False
                while retries > 0 and not success:
                    print(f"  Requesting Overpass (Attempt {4-retries}/3)...", flush=True)
                    try:
                        response = requests.post(OVERPASS_URL, data={'data': query}, timeout=180)
                        if response.status_code == 200:
                            data = response.json()
                            elements = data.get("elements", [])
                            print(f"  SUCCESS! Fetched {len(elements)} elements.", flush=True)
                            
                            for el in elements:
                                el_id = el.get("id")
                                if el["type"] == "node":
                                    lat, lon = el.get("lat"), el.get("lon")
                                else:
                                    center = el.get("center", {})
                                    lat, lon = center.get("lat"), center.get("lon")
                                    
                                tags = el.get("tags", {})
                                name = tags.get("name", "Unknown")
                                
                                poi_type = "Unknown"
                                if "amenity" in tags:
                                    poi_type = tags["amenity"].replace("_", " ").title()
                                elif "building" in tags:
                                    poi_type = tags["building"].replace("_", " ").title()
                                elif "emergency" in tags:
                                    poi_type = tags["emergency"].replace("_", " ").title()
                                    
                                writer.writerow([el_id, lat, lon, poi_type, name])
                            
                            success = True
                            # Sleep to be polite to the server
                            time.sleep(2)
                        elif response.status_code == 429:
                            print("  Rate limited (429). Sleeping for 30s...", flush=True)
                            time.sleep(30)
                            retries -= 1
                        else:
                            print(f"  Server error {response.status_code}. Retrying in 10s...", flush=True)
                            time.sleep(10)
                            retries -= 1
                    except Exception as ex:
                        print(f"  Request failed: {ex}. Retrying in 10s...", flush=True)
                        time.sleep(10)
                        retries -= 1
                
                if not success:
                    print(f"  FAILED to fetch chunk [{i+1},{j+1}] after all retries.", flush=True)

    print(f"State-wide fetch completed. Data saved to {out_path}", flush=True)

if __name__ == "__main__":
    fetch_pois()
