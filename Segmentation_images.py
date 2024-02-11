import os
import cv2
from skimage.segmentation import slic
from skimage.color import label2rgb
import matplotlib.pyplot as plt
from hdfs import InsecureClient


# Input and output paths
input_path = "/home/hadoopuser/mon_projet/images_from_videos"
output_path = "/home/hadoopuser/mon_projet/segments"

# Create the output directory if it doesn't exist
os.makedirs(output_path, exist_ok=True)

# Loop over the images and apply segmentation
for image_file in os.listdir(input_path):
            
    image_path = os.path.join(input_path, image_file)
    image = cv2.imread(image_path)

    # Convert the image to RGB
    image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)

    # Apply SLIC segmentation
    segments = slic(image_rgb, n_segments=50, compactness=10)

    # Convert segments to RGB for visualization
    segments_rgb = label2rgb(segments, image_rgb, kind='avg')

    # Save the segmented image
    output_image_path = os.path.join(output_path, f"{image_file}_segments.jpg")
    plt.imsave(output_image_path, segments_rgb)


local_output_directory = "/home/hadoopuser/mon_projet/segments"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
