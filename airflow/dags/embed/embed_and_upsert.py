from embed.connections import snowflake_connection, pinecone_connection, aws_connection
from pinecone import Pinecone
from sentence_transformers import SentenceTransformer
import torch
import pandas as pd
from io import BytesIO, StringIO
import numpy as np


def fetch_table_from_s3(csv_key):
    '''
    function to fetch latest(past 24 hours) jobs data from S3
    '''
    try:
        s3_client, bucket_name = aws_connection()
        print("AWS connection established. Fetching data")

        # fetching raw csv
        response = s3_client.get_object(Bucket=bucket_name, Key=f"validated_jobs/{csv_key}")
        csv_obj_content = response['Body'].read()
        
        # file like object creation
        pdfFileObj = BytesIO(csv_obj_content)
        pdf_df = pd.read_csv(pdfFileObj, sep="|")
        pdf_df = pdf_df.replace(np.nan, None)
        print("CSV fetched. Length of data: ", len(pdf_df))

        return pdf_df

    except Exception as e:
        print("Exception in fetch_data_from_snowflake function", e)
        return ''


def storing_pinecone(csv_key):
    '''
    function to store set a in pinecone
    '''
    try:
        df = fetch_table_from_s3(csv_key)
        df.drop(columns=['min_salary', 'max_salary', 'employment_type', 'source'], inplace=True)
        print(df.head())
        
        # Pinecone
        pinecone_api_key, index_name = pinecone_connection()
        pinecone = Pinecone(api_key=pinecone_api_key)
        index = pinecone.Index(name=index_name)

        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        if device != 'cuda':
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

        all_embeddings = []

        # iterating over pandas dataframe
        print("Generating embeddings..")
        for _, row in df.iterrows():
            _id = str(row['job_id'])
            job_title = row['job_title']
            company = row['company']
            location = row['job_location']
            url = row['job_url']
            date_posted = row['date_posted']
            description = row['job_desc']

            # Concatenating all relevant text data for embedding with new lines
            full_text = f"{job_title}\n{company}\n{location}\n{url}\n{date_posted}\n{description}"
            display_text = f"job_title: {job_title}\ncompany: {company}\nlocation: {location}\nurl: {url}\ndate_posted: {date_posted}"

            # embedding data
            embedding = model.encode(full_text)

            print(f"Embedded job for id: {_id}")

            embedding_data = {
                "id": _id,
                "values": embedding,
                "metadata": {
                    "id": _id,
                    "file_name": 'jobmatch_data.csv',
                    "text": display_text
                }
            }

            # embedding question and answer separately
            all_embeddings.append(embedding_data)
        print(len(embedding))
        print("Embeddings generated")

        # upserting the embeddings to pinecone namespace
        index.upsert(all_embeddings)

        return "successful"

    except Exception as e:
        print("Exception in storing_pinecone() function: ", e)
        return "failed"

# storing_pinecone("simplyhired_jobs.csv")