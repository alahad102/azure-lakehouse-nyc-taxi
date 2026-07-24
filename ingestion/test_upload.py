from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Step 1: prove who you are
credential = DefaultAzureCredential()

# Step 2: connect to the storage account
account_url = "https://stlakehousenyctx.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

# Step 3: get a handle on the "landing" container
file_system_client = service_client.get_file_system_client(file_system="landing")

# Step 4: get a handle on ONE new file inside that container
file_client = file_system_client.get_file_client("hello.txt")

# Step 5: read the actual bytes of your local file, then upload them
with open("hello.txt", "rb") as local_file:
    file_data = local_file.read()
    file_client.upload_data(file_data, overwrite=True)

print("Upload complete!")