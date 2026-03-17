import requests
import zipfile
import os
import sys

def download_file(url, dest_path):
    print(f"Downloading {url} to {dest_path}...")
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 * 1024 # 1MB
    
    downloaded = 0
    with open(dest_path, 'wb') as f:
        for data in response.iter_content(block_size):
            f.write(data)
            downloaded += len(data)
            if total_size > 0:
                percent = (downloaded / total_size) * 100
                sys.stdout.write(f"\rProgress: {percent:.2f}% ({downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB)")
                sys.stdout.flush()
    print("\nDownload complete.")

def main():
    url = "https://minedbuildings.z5.web.core.windows.net/legacy/usbuildings-v2/California.geojson.zip"
    data_dir = os.path.join(os.getcwd(), "data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    zip_path = os.path.join(data_dir, "California.zip")
    
    # Download
    if not os.path.exists(zip_path):
        try:
            download_file(url, zip_path)
        except Exception as e:
            print(f"Error downloading file: {e}")
            return

    # Extract
    print(f"Extracting {zip_path}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(data_dir)
        print(f"Extraction complete to {data_dir}")
    except Exception as e:
        print(f"Error extracting zip: {e}")

if __name__ == "__main__":
    main()
