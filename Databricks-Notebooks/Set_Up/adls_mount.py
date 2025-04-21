# Databricks notebook source
storageAccountname = 'emrstorageaccdev'
storageAccountKey = 'tqKEUoKE2dN14wJ0wGwhSXsDGau+/EtnjzXnB6iq5NpcDG8SlHvYleDoKmLNFeyuyFcxljMrsXCZ+ASt7+7LiQ=='
mountPoints = ["gold","silver","bronze","landing","configs"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
                source = f"wasbs://{mountPoint}@{storageAccountname}.blob.core.windows.net",
                mount_point = f"/mnt/{mountPoint}",
                extra_configs = {f"fs.azure.account.key.{storageAccountname}.blob.core.windows.net": storageAccountKey})
            print(f"Mount point {mountPoint} created")
        except Exception as e:
            print(f"Error creating mount point {mountPoint}: {e}")

# COMMAND ----------

dbutils.fs.mounts()