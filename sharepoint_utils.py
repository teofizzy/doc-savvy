import os
import argparse
import requests
from msal import ConfidentialClientApplication
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# Authentication details
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
PINECONE_ENVIRONMENT = os.getenv("PINECONE_ENVIRONMENT")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID")
SHAREPOINT_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET")

def get_graph_token():
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    scope = ["https://graph.microsoft.com/.default"]

    app = ConfidentialClientApplication(
        client_id=SHAREPOINT_CLIENT_ID,
        authority=authority,
        client_credential=SHAREPOINT_CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" not in result:
        raise Exception("Could not obtain Microsoft Graph token.")
    return result["access_token"]

def get_site_id(domain: str, site_name: str, token: str) -> str:
    url = f'https://graph.microsoft.com/v1.0/sites/{domain}:/sites/{site_name}'
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching site ID: {response.json()}")
    return response.json()["id"]

def list_files_recursive(site_id: str, folder_path: str, token: str) -> list[dict]:
    folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_path}:/children"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(folder_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error listing folder contents: {response.json()}")
    
    items = response.json().get("value", [])
    all_files = []

    for item in items:
        if "folder" in item:
            subfolder_path = os.path.join(folder_path, item["name"]).replace("\\", "/")
            all_files.extend(list_files_recursive(site_id, subfolder_path, token))
        else:
            all_files.append({
                "name": item["name"],
                "path": os.path.join(folder_path, item["name"]).replace("\\", "/")
            })

    return all_files

def get_file_content(site_id: str, file_path: str, token: str) -> BytesIO:
    file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(file_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Could not fetch file {file_path}: {response.json()}")
    return BytesIO(response.content)

def fetch_all_files(domain: str, site_name: str, folder_path: str) -> dict[str, BytesIO]:
    token = get_graph_token()
    site_id = get_site_id(domain, site_name, token)
    files_info = list_files_recursive(site_id, folder_path, token)

    file_contents = {}
    for file_info in files_info:
        try:
            file_data = get_file_content(site_id, file_info["path"], token)
            file_contents[file_info["path"]] = file_data
        except Exception as e:
            print(f"Warning: Could not fetch {file_info['path']}: {e}")
    
    return file_contents

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from SharePoint.")
    parser.add_argument("--domain", required=True, help="SharePoint domain")
    parser.add_argument("--site", required=True, help="Site name")
    parser.add_argument("--folder", required=True, help="Root folder path")

    args = parser.parse_args()

    files = fetch_all_files(
        domain=args.domain,
        site_name=args.site,
        folder_path=args.folder
    )

    for fname, file_io in files.items():
        print(f"Fetched: {fname} ({len(file_io.getvalue())} bytes)")
