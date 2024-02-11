import os
import cv2
import numpy as np
from hdfs import InsecureClient

def detection_objets(image):
    # Loading classes from the COCO dataset
    with open("/home/hadoopuser/mon_projet/coco.names", 'rt') as f:
        classes = f.read().rstrip('\n').split('\n')

    # Loading the YOLO model and pretrained weights
    net = cv2.dnn.readNet("yolov3.weights", "yolov3.cfg")

    # Getting the output layer names
    layer_names = net.getLayerNames()
    output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

    height, width, channels = image.shape

    # Preprocessing the image for input to the neural network
    blob = cv2.dnn.blobFromImage(image, 0.00392, (416, 416), (0, 0, 0), True, crop=False)

    # Passing the preprocessed image through the neural network
    net.setInput(blob)
    outs = net.forward(output_layers)

    # Processing the detections and drawing bounding boxes on the image
    for out in outs:
        for detection in out:
            scores = detection[5:]
            class_id = np.argmax(scores)
            confidence = scores[class_id]

            if confidence > 0.5:  # Confidence threshold
                # Calculating bounding box coordinates
                center_x = int(detection[0] * width)
                center_y = int(detection[1] * height)
                w = int(detection[2] * width)
                h = int(detection[3] * height)

                # Coordinates of top-left corner
                x = int(center_x - w / 2)
                y = int(center_y - h / 2)

                # Drawing bounding box and class label on the image
                cv2.rectangle(image, (x, y), (x + w, y + h), (255, 0, 0), 2)
                cv2.putText(image, classes[class_id], (x, y - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 255, 0), 2)

    # Returning the image with detections
    return image

# Loop over the images and apply object detection
input_path = "/home/hadoopuser/mon_projet/images_from_videos"
output_path = "/home/hadoopuser/mon_projet/Detections_Objets"

# Create the output directory if it doesn't exist
os.makedirs(output_path, exist_ok=True)

for image_file in os.listdir(input_path):
    image_path = os.path.join(input_path, image_file)
    image = cv2.imread(image_path)

    # Apply object detection
    detected_objects = detection_objets(image)

    # Save the processed image
    output_image_path = os.path.join(output_path, f"{image_file}_detected_objects.jpg")
    cv2.imwrite(output_image_path, detected_objects)

local_output_directory = "/home/hadoopuser/mon_projet/Detections_Objets"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
