from pydantic import BaseModel, HttpUrl, Field, validator, constr, ValidationError, field_validator, PositiveInt
import re
from typing import Optional, Union
from datetime import datetime, date
import pandas as pd
from validate.connections import aws_connection
# from connections import aws_connection
from io import StringIO, BytesIO
import numpy as np

class ScrapedJobsModel(BaseModel):
    job_id: Union[str,int]
    job_title: str
    company: str
    job_location: Optional[str]
    min_salary: Optional[PositiveInt]
    max_salary: Optional[PositiveInt]
    employment_type: Optional[str]
    source: constr(strip_whitespace=True, pattern='^(LinkedIn|Indeed|SimplyHired)$')
    job_url: HttpUrl
    date_posted: date
    job_desc: str
    
# Function to validate DataFrame rows and apply actions
def validate_and_process_jobs(csv_key):
    
    s3_client, bucket = aws_connection()
        
    # fetching raw csv
    response = s3_client.get_object(Bucket=bucket, Key=f"jobs/{csv_key}")
    csv_obj_content = response['Body'].read()
    
    # file like object creation
    pdfFileObj = BytesIO(csv_obj_content)
    pdf_df = pd.read_csv(pdfFileObj, sep="|")
    pdf_df = pdf_df.replace(np.nan, None)
    
    print("Length before validation: ",len(pdf_df))
    print(pdf_df.head())
    
    # iterating over CSV and validating data
    valid_data = []
    
    for index, row in pdf_df.iterrows():
        try:
            job = ScrapedJobsModel(**row.to_dict())
            valid_data.append(row)  # Keep the row if validation is successful
        except ValidationError as e:
            # print(f"Validation failed for row {index}: {e}\n{e.errors()}")
            flag=False
            for error in e.errors():
                if "job_id" in error['loc'][0]:
                    flag=True
                    break  # If job_id is null, remove the row
                if "job_title" in error['loc'][0]:
                    flag=True
                    break  # If job_id is null, remove the row
                if "company" in error['loc'][0]:
                    flag=True
                    break  # If job_id is null, remove the row
                if "min_salary" in error['loc'][0]:
                    row["min_salary"] = -1 * row['min_salary']    # If min_salary is negative, make the value positive
                if "max_salary" in error['loc'][0]:
                    row["max_salary"] = -1 * row['max_salary']    # If max_salary is negative, make the value positive
                if "source" in error['loc'][0]:
                    if row['job_url'].startswith("https://www.indeed"):
                        row['source'] = "Indeed"
                    elif row['job_url'].startswith("https://www.linkedin"):
                        row['source'] = "LinkedIn"
                    else:
                        row['source'] = None                    
                if "job_url" in error['loc'][0]:
                    flag=True
                    break    # if wrong URL, removing it as condidate will not be able to redirect without url
                if "job_location" in error['loc'][0]:
                    row['job_location'] = None
                if "employment_type" in error['loc'][0]:
                    row['employment_type'] = None
                if "date_posted" in error['loc'][0]:
                    try:
                        row['date_posted'] = datetime.date(row['date_posted'])
                    except:
                        row['date_posted'] = None 
                if "job_desc" in error['loc'][0]:
                    flag=True
                    break    # if job desc is null, the job will not be useful in recommendation
            if flag==False:
                valid_data.append(row)   # add row with updated values           
    
    # creating new dataframe for validated data
    validated_df = pd.DataFrame(valid_data)
    print("\nLength after validation: ",len(validated_df))
    print(validated_df.head())
    
    # Create a StringIO object to hold the CSV data
    csv_buffer = StringIO()
    
    # Write the DataFrame to the StringIO object as a tab-separated CSV file
    validated_df.to_csv(csv_buffer, sep="|", index=False, header=True)
    
    encoded_csv = BytesIO(csv_buffer.getvalue().encode())

    # uploading clean CSV to validated_jobs folder in S3
    s3_clean_key = f"validated_jobs/{csv_key}"
    s3_client.upload_fileobj(encoded_csv, bucket, s3_clean_key)
    
    print("Clean CSV file generated successfully.")
    
        
# validate_and_process_jobs("simplyhired_jobs.csv")