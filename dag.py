
import preprocessing
import ingest

if __name__ == '__main__':
    df = ingest.ingest_from_youtube_api()
    print(preprocessing.preprocess(df))
