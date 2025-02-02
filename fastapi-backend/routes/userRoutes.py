from fastapi import APIRouter, Depends, HTTPException, status, File, UploadFile
from fastapi.security import OAuth2PasswordBearer
from typing import List
import configparser
import jwt
from connections import aws_connection, mongo_connection
from botocore.exceptions import NoCredentialsError, ClientError
from starlette.responses import StreamingResponse
from sentence_transformers import SentenceTransformer
import torch
from pinecone import Pinecone
from connections import aws_connection, pinecone_connection, snowflake_connection, openai_connection
import os
from PyPDF2 import PdfReader
import re
from openai import OpenAI

router = APIRouter()

config = configparser.ConfigParser()
config.read('./configuration.properties')

# JWT config
SECRET_KEY = config['auth-api']['SECRET_KEY']
ALGORITHM = config['auth-api']['ALGORITHM']

# OAuth2 password bearer token
tokenUrl = config['password']['tokenUrl']
oauth2_scheme = OAuth2PasswordBearer(tokenUrl=tokenUrl)

# Function to get current user from token
async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=400, detail="Invalid token")
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    db = mongo_connection()
    collection = db[config['MONGODB']["COLLECTION_USER"]]
    user = collection.find_one({"username": username})
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return user

def mapUserAndFile(user, file_name):
    try:
        db = mongo_connection()
        collection = db[config['MONGODB']["COLLECTION_USER_FILE"]]
        user_file_data = {"userid": user["userid"], "email": user["email"], "file_name": file_name}
        collection.insert_one(user_file_data)
        return True
    except Exception as e:
        print(f"An error occurred while mapping user and file: {e}")
        return False

# Function to check if file exists in S3 bucket
def check_file_exists(s3, bucket_name, file_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

# Function to upload file to S3
def upload_to_s3(file, s3, bucket_name, file_name):
    try:
        file_contents = file.read()

        # Check if the file is empty
        if not file_contents:
            return False, "Empty file provided"

        # Upload the file contents to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_contents)
        # s3.upload_fileobj(file, bucket_name, file_name)
        return True, "File uploaded successfully"
    except FileNotFoundError:
        return False, "File not found"
    except NoCredentialsError:
        return False, "Credentials not found"
    
def pdf_txt_extraction(file_name):
    '''
    Function to:
        - Extract content from resume PDF files stored in AWS S3
        - Store the extracted text files back to S3
    '''
    try:
        s3_client, bucket_name = aws_connection()

        # Folder containing PDF files
        resume_files_folder = config['s3-bucket']['resumes_folder_name']

        # Folder to store extracted text files
        txt_file_folder = config['s3-bucket']['text_folder_name']
        
        input_file_key=resume_files_folder+file_name
        s3_client.download_file(bucket_name, input_file_key, file_name)

        # Extract text from the downloaded PDF file
        text = ''
        with open(file_name, 'rb') as file:
            reader = PdfReader(file)
            for page in reader.pages:
                extracted_text = page.extract_text() or ''
                # Remove extra spaces and clean up text
                cleaned_text = re.sub(r'\s+', ' ', extracted_text).strip()
                text += cleaned_text + ' '

        # Join non-empty sections
        text = ' '.join(filter(None, text.split(' ')))

        # Delete the temporary downloaded file (pdf)
        os.remove(file_name)

        # Writing file to S3 bucket
        file_name_wo_extension = os.path.splitext(os.path.basename(file_name))[0]
        output_file_path = file_name_wo_extension + '.txt'
        with open(output_file_path, 'w', encoding="utf-8") as file:
            file.write(text)

        # Upload the file to S3
        output_file_key = txt_file_folder + output_file_path
        response = s3_client.upload_file(output_file_path, bucket_name, output_file_key)

        # Delete the temporary downloaded file (txt)
        os.remove(output_file_path)

        return response
    except Exception as e:
        print("Exception in pdf_txt_extraction function: ", e)
        return None

    
def fetch_text_data(file_name):
    try:
        s3_client, bucket_name = aws_connection()

        # Specify the key path of the file you want to retrieve
        key_path = config['s3-bucket']['text_folder_name'] + file_name

        # Retrieve the object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=key_path)
        
        # Read the contents of the object and decode it
        text = response['Body'].read().decode('utf-8')

        return text
    
    except Exception as e:
        print("Error fetching text data:", e)
        return None

def getJobDetails(job_id):
    try:
        conn, table = snowflake_connection()
        fetch_query = f"SELECT job_id, job_title, company, job_location, source, url, date_posted, description FROM {table} WHERE job_id = '{job_id}';"

        cur = conn.cursor()

        cur.execute(fetch_query)

        row = cur.fetchone()
        if row:
            return {
            'job_id': row[0],
            'job_title': row[1],
            'company': row[2],
            'job_location': row[3],
            'source': row[4],
            'url': row[5],
            'date_posted': row[6],
            'description': row[7]
        }
        else:
            return None

    except Exception as e:
        print("Exception in fetch_data_from_snowflake function", e)
        return ''
    

def fetch_from_pinecone(file_name):
    '''
    function to fetch data from pinecone
    '''
    try:
        # Pinecone
        pinecone_api_key, index_name = pinecone_connection()
        pinecone = Pinecone(api_key=pinecone_api_key)

        # fetching data from pinecone
        index = pinecone.Index(name=index_name)
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        if device != 'cuda':
            model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

        query = fetch_text_data(file_name)
        xq = model.encode(query).tolist()
        xc = index.query(vector=xq, top_k=10, include_metadata=True)

        recommendations=[]
        for match in xc['matches']:
            score = match['score']
            job_id = match['id']
            jobDetails = getJobDetails(job_id)
            recommendations.append({"score": score, "jobDetails": jobDetails})
        
        return recommendations

        # fetching data from pinecone namespace
    except Exception as e:
        print("Exception: ", e)
        return None

def fetch_missing_keywords(resumeText, job_desc):
    '''
    Function to fetch missing keywords from OpenAI
    '''
    try:
        openai_api_key = openai_connection()
        openai_client = OpenAI(api_key=openai_api_key)
        prompt = f"You are tasked with analyzing a given resume against a specific job description. Your goal is to identify the top 5 technology stack(like Python, C, C++, etc), required by the job description that are not explicitly mentioned in the resume. While doing so, ensure to consider semantically similar terms that might be implicitly present in the resume. Display your response as a markdown table with columns skill and description.\nCONTEXT:\nRESUME:\n{resumeText}\n\nJOB DESCRIPTION:\n{job_desc}\n"
 
        response = openai_client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            temperature=0.5,
            max_tokens=2048,
            frequency_penalty=0.1,
            presence_penalty=0.1
        )
 
        missing_keywords = response.choices[0].message.content
        return missing_keywords
    
    except Exception as e:
        print("Error fetching missing keywords:", e)
        return None
    
# Route to handle multiple file uploads
@router.post("/upload/")
async def upload_files_to_s3(files: List[UploadFile] = File(...), current_user: dict = Depends(get_current_user)):
    try:
        s3, bucket_name = aws_connection()
        resumes_folder_name = config['s3-bucket']['resumes_folder_name']
        uploaded_files = []
        for file in files:
            file_name = resumes_folder_name+file.filename
            # Check if the file already exists in S3
            if check_file_exists(s3, bucket_name, file_name):
                # If it exists, skip processing this file and move to the next one
                uploaded_files.append({"file_name": file.filename, "status": "File already exists"})
                continue
            # If the file doesn't exist, upload it to S3
            success, message = upload_to_s3(
                file.file, s3, bucket_name, file_name)
            s3_file_url = "s3://" + bucket_name + "/" + file_name
            if success:
                if mapUserAndFile(current_user, file.filename):
                    pdf_txt_extraction(file.filename)
                    uploaded_files.append(
                        {"file_name": file.filename, "file_location": s3_file_url, "status": "File uploaded successfully"})
            else:
                return {"error": message}

        return {"message": "Files processed successfully", "uploaded_files": uploaded_files}
    except Exception as e:
        return {"error": str(e)}
    finally:
        for file in files:
            file.file.close()


@router.get("/files/", response_model=List[str])
async def get_files(current_user: str = Depends(get_current_user)):
    db = mongo_connection()
    collection = db[config['MONGODB']["COLLECTION_USER_FILE"]]
    email = current_user['email']
    user_files = collection.find({"email": email})
    files = [file["file_name"] for file in user_files]
    if not files:
        raise HTTPException(
            status_code=404, detail="No files found for the user")
    return files


@router.get("/getResume/")
async def get_resume(file_name: str):
    s3_client, bucket_name = aws_connection()
    resumes_folder_name = config['s3-bucket']['resumes_folder_name']
    full_path = resumes_folder_name + file_name
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_path)
        return StreamingResponse(response['Body'], media_type='application/pdf')
    except s3_client.exceptions.NoSuchKey:
        print("File not found at path: " + full_path)
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        print("An error occurred: " + str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/getJobRecommendations")
async def get_job_recommendations(file_name: str, current_user: dict = Depends(get_current_user)):
    try:
        # Fetch job recommendations
        file_name=file_name.replace(".pdf",".txt")
        recommendations = fetch_from_pinecone(file_name)
        
        if recommendations:
            return recommendations
        else:
            raise HTTPException(status_code=404, detail="No job recommendations found for the given file.")
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/getMissingKeywords")
async def get_missing_keywords(file_name: str, job_desc: str):
                               
    try:
        file_name = file_name.replace(".pdf", ".txt")
        resumeText = fetch_text_data(file_name)
        # fetchFromOpenAi()
        # prompt
        response = fetch_missing_keywords(resumeText, job_desc)
 
        if response:
            return response
        else:
            raise HTTPException(
                status_code=404, detail="No missing keywords found for the given file.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))