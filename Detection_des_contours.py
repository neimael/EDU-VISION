from pyspark.sql import SparkSession
import cv2
import numpy as np
import os
from hdfs import InsecureClient

# Charger les images une par une et appliquer les filtres
local_input_path = "/home/hadoopuser/mon_projet/images_filtrees"
image_files = os.listdir(local_input_path)

for image_file in image_files:
    if "_median_filter.jpg" in image_file:
        image_path = os.path.join(local_input_path, image_file)
        image = cv2.imread(image_path)
        
        # Appliquer les filtres
        canny_contours = cv2.Canny(image, 50, 100) 

        # Enregistrer les images traitées localement dans un répertoire temporaire
        output_local_path = "/home/hadoopuser/mon_projet/Detections_contours"
        os.makedirs(output_local_path, exist_ok=True)
        
        # Enregistrer les images
        cv2.imwrite(os.path.join(output_local_path, f"{image_file}_canny_contours.jpg"), canny_contours)

local_output_directory = "/home/hadoopuser/mon_projet/Detections_contours"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
