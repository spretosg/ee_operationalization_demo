# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Uploads images from a GCS bucket to EE, calculates the mean, and saves the result back to a GCS bucket."""

import ee
import google.auth
from google.cloud import storage

def gcs_to_ee(event, context):
  """Handles file creation in GCS bucket and triggers EE tasks.

  Args:
    event: Event payload.
    context: Metadata for the event.
  """
  # Set up auth.
  credentials, project_id = google.auth.default()
  ee.Initialize(credentials)
  
  # List all files in the bucket.
  storage_client = storage.Client()
  bucket_name = event['bucket']
  bucket = storage_client.bucket(bucket_name)
  blobs = bucket.list_blobs()
  
  image_collection = []
  for blob in blobs:
    path = "gs://" + bucket_name + "/" + blob.name
    file_title = blob.name.rsplit(".", 1)[0]
    print(f"Ingesting {file_title} from {path}")
    image = upload_image_to_ee(project_id, file_title, path)
    image_collection.append(image)
  
  if image_collection:
    # Calculate mean of all images.
    mean_image = calculate_mean_image(image_collection)
    
    # Export mean image to GCS.
    export_to_gcs(mean_image, bucket_name, 'mean_image')

def upload_image_to_ee(project_id, file_title, path):
  """Uploads an image to Earth Engine."""
  asset_id = f"projects/{project_id}/assets/{file_title}"
  task_id = ee.data.newTaskId()[0]
  
  # Define the ingestion parameters.
  params = {
    'id': asset_id,
    'sources': [{'uris': [path]}]
  }
  
  # Start the ingestion task.
  ee.data.startIngestion(task_id, params, allow_overwrite=True)
  
  # Return the EE image.
  return ee.Image(asset_id)

def calculate_mean_image(image_collection):
  """Calculates the mean of a list of images."""
  ee_collection = ee.ImageCollection(image_collection)
  return ee_collection.mean()

def export_to_gcs(image, bucket_name, output_name):
  """Exports an EE image to GCS."""
  task = ee.batch.Export.image.toCloudStorage(
    image=image,
    description='Mean Image Export',
    bucket=bucket_name,
    fileNamePrefix=output_name,
    scale=30,
    region=image.geometry().bounds().getInfo()['coordinates']
  )
  task.start()
  print(f"Exporting mean image to gs://{bucket_name}/{output_name}")

