import os
import argparse
import requests
from msal import ConfidentialClientApplication
from io import BytesIO

class SharePointFetcher:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.get_graph_token()

    def get_graph_token(self) -> str:
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = ["https://graph.microsoft.com/.default"]

        app = ConfidentialClientApplication(
            client_id=self.client_id,
            authority=authority,
            client_credential=self.client_secret
        )
        result = app.acquire_token_for_client(scopes=scope)
        if "access_token" not in result:
            raise Exception("Could not obtain Microsoft Graph token.")
        return result["access_token"]

    def get_site_id(self, domain: str, site_name: str) -> str:
        url = f'https://graph.microsoft.com/v1.0/sites/{domain}:/sites/{site_name}'
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error fetching site ID: {response.json()}")
        return response.json()["id"]

    def list_files_recursive(self, site_id: str, folder_path: str) -> list[dict]:
        folder_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{folder_path}:/children"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(folder_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Error listing folder contents: {response.json()}")

        items = response.json().get("value", [])
        all_files = []

        for item in items:
            if "folder" in item:
                subfolder_path = os.path.join(folder_path, item["name"]).replace("\\", "/")
                all_files.extend(self.list_files_recursive(site_id, subfolder_path))
            else:
                all_files.append({
                    "name": item["name"],
                    "path": os.path.join(folder_path, item["name"]).replace("\\", "/")
                })

        return all_files

    def get_file_content(self, site_id: str, file_path: str) -> BytesIO:
        file_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:/{file_path}:/content"
        headers = {"Authorization": f"Bearer {self.token}"}
        response = requests.get(file_url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Could not fetch file {file_path}: {response.json()}")
        return BytesIO(response.content)

    def fetch_all_files(self, domain: str, site_name: str, folder_path: str) -> dict[str, BytesIO]:
        site_id = self.get_site_id(domain, site_name)
        files_info = self.list_files_recursive(site_id, folder_path)

        file_contents = {}
        for file_info in files_info:
            try:
                file_data = self.get_file_content(site_id, file_info["path"])
                file_contents[file_info["path"]] = file_data
            except Exception as e:
                print(f"Warning: Could not fetch {file_info['path']}: {e}")
        
        return file_contents

# Optional CLI usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download files from SharePoint.")
    parser.add_argument("--domain", required=True, help="SharePoint domain")
    parser.add_argument("--site", required=True, help="Site name")
    parser.add_argument("--folder", required=True, help="Root folder path")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--client_id", required=True, help="Client ID")
    parser.add_argument("--client_secret", required=True, help="Client Secret")

    args = parser.parse_args()

    fetcher = SharePointFetcher(
        tenant_id=args.tenant,
        client_id=args.client_id,
        client_secret=args.client_secret
    )

    files = fetcher.fetch_all_files(
        domain=args.domain,
        site_name=args.site,
        folder_path=args.folder
    )

    for fname, file_io in files.items():
        print(f"Fetched: {fname} ({len(file_io.getvalue())} bytes)")
