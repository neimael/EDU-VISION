import cv2
import numpy as np
import os
from hdfs import InsecureClient

def detect_faces(image):
    # Convertir l'image en niveaux de gris
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    
    # Charger le classifieur de détection de visages pré-entraîné
    face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + 'haarcascade_frontalface_default.xml')
    
    # Détecter les visages dans l'image
    faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
    
    # Dessiner des rectangles autour des visages détectés
    for (x, y, w, h) in faces:
        cv2.rectangle(image, (x, y), (x+w, y+h), (255, 0, 0), 2)
    
    return image


# Charger les images une par une et appliquer les filtres
local_input_path = "/home/hadoopuser/mon_projet/images_from_videos"
image_files = os.listdir(local_input_path)

for image_file in image_files:
    image_path = os.path.join(local_input_path, image_file)
    image = cv2.imread(image_path)
    
    # Appliquer les filtres
    face_detected = detect_faces(image)

    # Enregistrer les images traitées localement dans un répertoire temporaire
    output_local_path = "/home/hadoopuser/mon_projet/faces_detected"
    os.makedirs(output_local_path, exist_ok=True)
    
    # Enregistrer les images
    cv2.imwrite(os.path.join(output_local_path, f"{image_file}_face_detected.jpg"), face_detected)

local_output_directory = "/home/hadoopuser/mon_projet/faces_detected"
hdfs_output_directory = "/project_videos"

# After extraction, move the files to HDFS
client = InsecureClient("http://localhost:9870", user="hadoopuser")

# Check if the HDFS directory exists; create it if not
if not client.status(hdfs_output_directory, strict=False):
    client.makedirs(hdfs_output_directory)

# Copy files from local to HDFS
client.upload(hdfs_output_directory, local_output_directory, overwrite=True)
