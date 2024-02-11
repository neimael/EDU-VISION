import os
import json
from kafka import KafkaProducer, KafkaConsumer
from pytube import YouTube
from hdfs import InsecureClient

# Kafka configuration for producer
kafka_producer_config = {
    'bootstrap_servers': 'localhost:9092',
}

# Kafka producer
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'), **kafka_producer_config)
youtube_topic = 'youtube-videos-topic'

# Kafka configuration for consumer
kafka_consumer_config = {
    'bootstrap_servers': 'localhost:9092',
    'group_id': 'video-consumer-group',  # Provide a unique group id
    'auto_offset_reset': 'earliest',     # Start consuming from the earliest message in the topic
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
}

# Kafka consumer
consumer = KafkaConsumer(youtube_topic, **kafka_consumer_config)

# HDFS configuration
hdfs_config = {
    'url': 'http://localhost:9870',
    'user': 'hadoopuser',
}

# HDFS client
hdfs_client = InsecureClient(**hdfs_config)

# Local directory to store downloaded videos temporarily
local_video_directory = 'videos'

# Create local directory if it doesn't exist
os.makedirs(local_video_directory, exist_ok=True)

def download_video_from_youtube(youtube_url):
    try:
        # Fetch video details
        yt = YouTube(youtube_url)

        local_file_path = local_video_directory

        video_stream = yt.streams.get_highest_resolution()
        video_stream.download(local_file_path)

        print(f"Video downloaded to {local_file_path}")

        return local_file_path, f'{yt.title}.mp4'

    except Exception as e:
        print(f"Error downloading video from YouTube: {e}")
        return None


def upload_video_to_hdfs(local_file_path, base_name):
    try:
        file_path = os.path.join(local_file_path, base_name)

        if local_file_path and os.path.isfile(file_path):
            # Create the HDFS path by using the base name
            hdfs_path = f'/project_videos/{base_name}'

            with open(file_path, 'rb') as local_file:
                hdfs_client.write(hdfs_path, local_file)

            print(f"Video uploaded to HDFS: {hdfs_path}")

            return hdfs_path
        elif local_file_path and os.path.isdir(local_file_path):
            print(f"Error uploading video to HDFS: The specified path is a directory - {base_name}")
            return None
        else:
            print(f"Error uploading video to HDFS: The specified path is not a file - {base_name}")
            return None

    except Exception as e:
        print(f"Error uploading video to HDFS: {e}")
        return None


# Fetch and publish videos to Kafka
youtube_links = [
    'https://www.youtube.com/watch?v=gTs6VC-FzDE',
    # Add more YouTube links as needed
]

for link in youtube_links:
    local_file_path, base_name = download_video_from_youtube(link)

    if local_file_path:
        upload_video_to_hdfs(local_file_path, base_name)

# Cleanup: Close Kafka producer and consumer
producer.close()
consumer.close()
