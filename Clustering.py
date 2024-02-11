import os
import cv2
import numpy as np
from hdfs import InsecureClient

def apply_kmeans_to_images(input_directory, output_directory, k=5):
    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)
    
    # Loop over the images in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith(".jpg") or filename.endswith(".png"):
            # Read the image
            image_path = os.path.join(input_directory, filename)
            image = cv2.imread(image_path)
            
            # Convert the image to the RGB color space
            image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            
            # Reshape the image to a 2D array of pixels
            pixels = image_rgb.reshape((-1, 3))
            
            # Convert to float32
            pixels = np.float32(pixels)
            
            # Define criteria and apply K-means clustering
            criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 100, 0.2)
            _, labels, centers = cv2.kmeans(pixels, k, None, criteria, 10, cv2.KMEANS_RANDOM_CENTERS)
            
            # Convert back to uint8 and reshape to the original image shape
            centers = np.uint8(centers)
            segmented_image = centers[labels.flatten()]
            segmented_image = segmented_image.reshape(image_rgb.shape)
            
            # Save the segmented image
            output_path = os.path.join(output_directory, filename)
            cv2.imwrite(output_path, cv2.cvtColor(segmented_image, cv2.COLOR_RGB2BGR))


# Example usage:
input_directory = "/home/hadoopuser/mon_projet/images_from_videos"
output_directory = "/home/hadoopuser/mon_projet/Clustering"
apply_kmeans_to_images(input_directory, output_directory, k=5)


local_output_directory = "/home/hadoopuser/mon_projet/Clustering"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
