import pandas as pd
import pytz
import isodate


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
