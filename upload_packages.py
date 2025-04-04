import time
import requests
import os
import json

# Constants
KBPLUS_URL = "https://www.kbplus.ac.uk/kbplus7/publicExport/idx?format=json&max=3"
MICROKB_UPLOAD_URL = "http://127.0.0.1:5000/upload"

def fetch_packages():
    response = requests.get(KBPLUS_URL)
    response.raise_for_status()
    return response.json().get("packages", [])

def download_csv(url, filename):
    response = requests.get(url)
    response.raise_for_status()
    with open(filename, 'wb') as file:
        file.write(response.content)

def upload_package(file_path, package_id, package_name):
    with open(file_path, 'rb') as file:
        response = requests.post(
            MICROKB_UPLOAD_URL,
            files={"file": file},
            data={"additional_identifiers": json.dumps([{"type": "kbplus", "identifier": package_id}]), "package_name": package_name}
        )
    response.raise_for_status()
    return response.json()

def main():
    packages = fetch_packages()
    for package in packages:
        time.sleep(1)
        package_id = package["identifier"]
        package_name = package["name"]
        csv_url = package["packageContentAsCsv"]
        csv_filename = f"{package_name}.csv"

        print(f"Processing package: {package_name} (ID: {package_id})")

        # Download the CSV
        try:
            download_csv(f"https://www.kbplus.ac.uk/test2/publicExport/pkg/{package_id}?format=xml&transformId=kbart2", csv_filename)
        except Exception as e:
            print(f"Failed to download CSV for package {package_name}: {e}")

        # Upload the package
        count = 0
        # while count < 500:
        try:
            response = upload_package(csv_filename, package_id, package_name)
            count += 1
            print(f"Uploaded package {package_name} successfully: {response}")
        except Exception as e:
            print(f"Failed to upload package {package_name}: {e}")

        # Clean up the downloaded CSV file
        try:
            os.remove(csv_filename)
        except Exception as e:
            print(f"Failed to remove CSV file {csv_filename}: {e}")

if __name__ == "__main__":
    main()