import datetime
import isodate
import pytz
import pandas as pd
from dotenv import dotenv_values
from googleapiclient.discovery import build
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

config = dotenv_values(".env")
api_key = config["API_key"]

channel_ids = ['UC8butISFwT-Wl7EV0hUK0BQ', ]
api_service_name = "youtube"
api_version = "v3"
playlist_id = "UU8butISFwT-Wl7EV0hUK0BQ"

youtube = build(
    api_service_name, api_version, developerKey=api_key)


def get_channel_stats(youtube, channel_ids):
    all_data = []

    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=','.join(channel_ids)
    )
    response = request.execute()

    # loop through items
    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
                'subscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalVideos': item['statistics']['videoCount'],
                'playlistId': item['contentDetails']['relatedPlaylists']['uploads']
                }

        all_data.append(data)

    return pd.DataFrame(all_data)


def get_video_ids(youtube, playlist_id):

    video_ids = []

    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part='contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')

    return video_ids


def get_video_details(youtube, video_ids):

    all_video_info = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i+50])
        )
        response = request.execute()

        for video in response['items']:
            stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                             }
            video_info = {}
            video_info['video_id'] = video['id']

            for k in stats_to_keep.keys():
                for v in stats_to_keep[k]:
                    try:
                        video_info[v] = video[k][v]
                    except:
                        video_info[v] = None

            all_video_info.append(video_info)

    return pd.DataFrame(all_video_info)


def ingest_from_youtube_api():
    # Get video IDs
    video_ids = get_video_ids(youtube, playlist_id)
    # Store into a dataframe
    video_df = get_video_details(youtube, video_ids)
    return video_df


def preprocess(df):
    # Timestamp for published date
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    sgt_timezone = pytz.timezone('Asia/Singapore')
    df['publishedAt'] = df['publishedAt'].dt.tz_convert(sgt_timezone)
    df['publishedAt'] = pd.to_datetime(
        df['publishedAt']).dt.strftime('%Y-%m-%d %H:%M:%S')

    # Duration
    df['durationSecs'] = df['duration'].apply(
        lambda x: isodate.parse_duration(x))

    df['durationSecs'] = df['durationSecs'].astype(
        'timedelta64[s]')

    return df


def run_youtube_etl():
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = f"s3://rafpang-airflow-youtube-bucket/freecodecamp_{date}.csv"
    df = ingest_from_youtube_api()
    transformed_df = preprocess(df)
    transformed_df.to_csv(filename)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}

dag = DAG(
    'youtube_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval=datetime.timedelta(days=30),
)

run_etl = PythonOperator(
    task_id='aws_task_id',
    python_callable=run_youtube_etl,
    dag=dag,
)

run_etl
