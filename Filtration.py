from pyspark.sql import SparkSession
import cv2
import numpy as np
import os
from hdfs import InsecureClient
import io


def filtrage_median(image):
    # Get the dimensions of the image
    height, width, channels = image.shape
    
    # Create an empty image to store the filtered result
    filtered_image = np.zeros((height, width, channels), dtype=np.uint8)
    
    # Define the kernel size for the median filter
    kernel_size = 3  # Adjust this value as needed
    
    # Calculate the margin of the kernel
    margin_m = kernel_size // 2
    margin_n = kernel_size // 2
    
    # Apply the median filter step by step
    for i in range(margin_m, height - margin_m):
        for j in range(margin_n, width - margin_n):
            # Extract the values from the 3x3 region around the current pixel for each channel
            region_values = []
            for k in range(-margin_m, margin_m + 1):
                for l in range(-margin_n, margin_n + 1):
                    pixel_values = [image[i + k, j + l, c] for c in range(channels)]
                    region_values.append(pixel_values)
            
            # Calculate the median value for each channel separately
            median_values = np.median(region_values, axis=0)
            
            # Assign the median values to the corresponding pixel in the filtered image
            filtered_image[i, j] = median_values.astype(np.uint8)
    
    return filtered_image

# Fonction de filtrage gaussien
def apply_gaussian_blur(image,sigma):
    return cv2.GaussianBlur(image, (0,0), sigma)


# Charger les images une par une et appliquer les filtres
local_input_path = "/home/hadoopuser/mon_projet/images_from_videos"
image_files = os.listdir(local_input_path)
sigma = 4

for image_file in image_files:
    image_path = os.path.join(local_input_path, image_file)
    image = cv2.imread(image_path)
    
    # Appliquer les filtres
    median_filtered_image = filtrage_median(image)
    blurred_image = apply_gaussian_blur(image,sigma)

    # Enregistrer les images traitées localement dans un répertoire temporaire
    output_local_path = "/home/hadoopuser/mon_projet/images_filtrees"
    os.makedirs(output_local_path, exist_ok=True)
    
    # Enregistrer les images
    cv2.imwrite(os.path.join(output_local_path, f"{image_file}_gauss_filtered_image.jpg"), blurred_image)
    cv2.imwrite(os.path.join(output_local_path, f"{image_file}_median_filter.jpg"), median_filtered_image)

local_output_directory = "/home/hadoopuser/mon_projet/images_filtrees"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
