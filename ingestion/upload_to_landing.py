import os
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Step 1: prove who you are
credential = DefaultAzureCredential()

# Step 2: connect to the storage account
account_url = "https://stlakehousenyctx2.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

# Step 3: get a handle on the "landing" container
file_system_client = service_client.get_file_system_client(file_system="landing")

# Step 4: point at the local folder containing your parquet files
local_data_folder = "data"

# Step 5: loop over every file in that folder and upload each one
for filename in os.listdir(local_data_folder):
    if filename.endswith(".parquet"):
        local_path = os.path.join(local_data_folder, filename)
        print(f"Uploading {filename}...")

        file_client = file_system_client.get_file_client(filename)

        with open(local_path, "rb") as local_file:
            file_data = local_file.read()
            file_client.upload_data(file_data, overwrite=True)

        print(f"{filename} uploaded successfully.")

print("All files uploaded.")