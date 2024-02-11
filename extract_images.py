from pyspark.sql import SparkSession
import cv2
import numpy as np
import os
from hdfs import InsecureClient
import io

def extract_images_from_video(video_path, local_output_directory, num_frames=6):
    try:
        # Open the video capture
        cap = cv2.VideoCapture(video_path)

        # Check if the video capture is successful
        if not cap.isOpened():
            print(f"Error opening video: {video_path}")
            return

        # Get total number of frames in the video
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Calculate the skip interval to get the desired number of frames
        skip_interval = max(total_frames // num_frames, 1)

        # Create the local output directory if it doesn't exist
        os.makedirs(local_output_directory, exist_ok=True)

        # Extract frames from the video
        for i in range(0, total_frames, skip_interval):
            cap.set(cv2.CAP_PROP_POS_FRAMES, i)
            ret, frame = cap.read()
            if ret:
                # Save the frame as a local image
                frame_path = os.path.join(local_output_directory, f"frame_{i}.png")
                cv2.imwrite(frame_path, frame)

        # Release the video capture
        cap.release()

    except Exception as e:
        print(f"Error processing video: {video_path}")
        print(e)

local_output_directory = "/home/hadoopuser/mon_projet/images_from_videos"
video_directory = "/home/hadoopuser/mon_projet/videos"
num_frames_to_extract = 15

# List all video files in the video directory
video_files = os.listdir(video_directory)

# Iterate through each video file and extract images
for video_file in video_files:
    video_path = os.path.join(video_directory, video_file)
    extract_images_from_video(video_path, local_output_directory, num_frames_to_extract)



local_output_directory = "/home/hadoopuser/mon_projet/images_from_videos"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
