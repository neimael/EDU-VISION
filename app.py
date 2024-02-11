import streamlit as st
import os
from PIL import Image

def main():
    st.title("Computer Vision Operations")


    # Navigation Bar
    page_options = {
        "Ingestion Videos": "📹",
        "Extracted Images": "📸",
        "Filtered Images": "🌈",
        "Contours Detection": "🔄",
        "Object Detection": "🔍",
        "Facial Recognition": "😷",
        "Segmentation": "🔲"
    }

    st.sidebar.title("Select Operation")

    # Initialize session state
    if 'selected_page' not in st.session_state:
        st.session_state.selected_page = "Ingestion Videos"

    for page, icon in page_options.items():
        if st.sidebar.button(f"{icon} {page}", key=page):
            st.session_state.selected_page = page

    navigate_to(st.session_state.selected_page)

def navigate_to(page=None):
    if page == "Ingestion Videos":
        ingestion_videos()
    elif page == "Extracted Images":
        extract_images()
    elif page == "Filtered Images":
        filtered_images()
    elif page == "Contours Detection":
        contours_detection()
    elif page == "Object Detection":
        object_detection()
    elif page == "Facial Recognition":
        facial_recognition()
    elif page == "Segmentation":
        segmentation()

def ingestion_videos():
    st.header("Ingestion Videos")

    video_link = st.text_input("Enter Video Link:")
    if st.button("Ingest Video"):
        # Add code for ingesting video
        st.success("Video ingested successfully!")

def extract_images():
    st.header("Extracted Images")

    # Get the list of image files in the specified directory
    image_dir = "/home/hadoopuser/mon_projet/images_from_videos"
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        st.warning("No images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

def filtered_images():
    st.header("Filtered Images")

    # Horizontal buttons for Filtered Images page
    page_options_filtered = {
        "Gaussian Filter": "gauss",
        "Median Filter": "median"
    }

    selected_option = st.selectbox("Select Filter Option:", list(page_options_filtered.keys()))

    # Display images immediately after selecting the filter
    image_dir = "/home/hadoopuser/mon_projet/images_filtrees/"
    filter_type = page_options_filtered[selected_option]
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg')) and filter_type in f.lower()]

    if not image_files:
        st.warning(f"No {filter_type} images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

def contours_detection():
    st.header("Contours Detection")

    # Get the list of image files in the specified directory
    image_dir = "/home/hadoopuser/mon_projet/Detections_contours"
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        st.warning("No images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

def object_detection():
    st.header("Object Detection")

    # Get the list of image files in the specified directory
    image_dir = "/home/hadoopuser/mon_projet/Detections_Objets"
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        st.warning("No images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

def facial_recognition():
    st.header("Facial Recognition")

    # Get the list of image files in the specified directory
    image_dir = "/home/hadoopuser/mon_projet/faces_detected"
    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        st.warning("No images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

def segmentation():
    st.header("Segmented Images")

    # Horizontal buttons for segmented Images page
    page_options_segmentation = {
        "Clustering Segmentation": "Clustering",
        "Superpixel Segmentation": "segments"
    }

    selected_option = st.selectbox("Select Segmentation Option:", list(page_options_segmentation.keys()))

    # Display images immediately after selecting the filter
    base_dir = "/home/hadoopuser/mon_projet/"
    segmentation_type = page_options_segmentation[selected_option]
    image_dir = os.path.join(base_dir, segmentation_type)

    image_files = [f for f in os.listdir(image_dir) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]

    if not image_files:
        st.warning(f"No {segmentation_type} images found in the directory.")
    else:
        # Display images in two columns
        columns = st.columns(2)

        for i, image_file in enumerate(image_files):
            image_path = os.path.join(image_dir, image_file)
            image = Image.open(image_path)

            # Alternate between columns for each image
            columns[i % 2].image(image, use_column_width=True)

if __name__ == "__main__":
    st.set_page_config(
        page_title="EDU-VISION",
        page_icon=":camera:",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    main()