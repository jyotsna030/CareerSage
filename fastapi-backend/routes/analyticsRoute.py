from fastapi import APIRouter
from fastapi.responses import JSONResponse
import re
import pandas as pd
from connections import snowflake_connection

router = APIRouter()


def filter_job_locations():
    conn, jobTable = snowflake_connection()
    query = f"""
    SELECT * FROM {jobTable}
    WHERE JOB_LOCATION NOT ILIKE '%null%'
    AND JOB_LOCATION NOT ILIKE '%remote%'
    AND JOB_LOCATION NOT ILIKE '%United States%'
    """
    curr = conn.cursor()
    curr.execute(query)
    data = curr.fetchall()
    data = pd.DataFrame(data, columns=[desc[0] for desc in curr.description])
    # print(data)

    # Extract state from JOB_LOCATION if in "City, State" format
    data['STATE'] = data['JOB_LOCATION'].str.extract(r',\s*([^,]+)$')

    # If STATE column is empty, fill it with the original location
    # data['STATE'].fillna(data['JOB_LOCATION'], inplace=True)
    data['STATE'] = data['STATE'].fillna(data['JOB_LOCATION'])
    return data


def load_data():
    conn, jobTable = snowflake_connection()
    query = f"""
    SELECT * FROM {jobTable}
    """
    curr = conn.cursor()
    curr.execute(query)
    data = curr.fetchall()
    data = pd.DataFrame(data, columns=[desc[0] for desc in curr.description])

    return data


def get_job_counts(data):
    job_counts = data['STATE'].value_counts().reset_index()
    job_counts.columns = ['STATE', 'COUNT']
    return job_counts

def get_employment_type_counts(data):
    employment_type_counts = data['EMPLOYMENT_TYPE'].value_counts().reset_index()
    employment_type_counts.columns = ['EMPLOYMENT_TYPE', 'COUNT']
    return employment_type_counts



def filter_and_count_jobs(data, job_titles):
    data['Normalized_Job_Title'] = data['JOB_TITLE'].str.lower().str.strip()
    job_titles_regex = '|'.join(
        [f"({re.escape(title.strip().lower())})" for title in job_titles])
    pattern = rf'\b(?:{job_titles_regex})\b'
    title_map = {re.escape(title.strip().lower()): title for title in job_titles}

    def map_titles(normalized_title):
        for regex, original_title in title_map.items():
            if re.match(rf'\b{regex}\b', normalized_title):
                return original_title
        return normalized_title
    filtered_data = data[data['Normalized_Job_Title'].str.contains(
        pattern, regex=True, na=False)]
    filtered_data['Mapped_Job_Title'] = filtered_data['Normalized_Job_Title'].apply(
        map_titles)
    job_title_counts = filtered_data['Mapped_Job_Title'].value_counts(
    ).reset_index()
    job_title_counts.columns = ['JOB_TITLE', 'COUNT']
    return job_title_counts


def preprocess_salaries(data):
    data = data.dropna(subset=['MIN_SALARY'])
    data = data.dropna(subset=['MAX_SALARY'])

    hour_to_yearly_factor = 40 * 4 * 12
    data['MIN_SALARY'] = data['MIN_SALARY'].apply(
        lambda x: x * hour_to_yearly_factor if x < 100 else x)
    data['MAX_SALARY'] = data['MAX_SALARY'].apply(
        lambda x: x * hour_to_yearly_factor if x < 100 else x)
    return data


def filter_employment_types(data):
    return data[data['EMPLOYMENT_TYPE'].isin(['Full-time', 'Contract'])]


@ router.get("/job_counts")
async def get_counts():
    try:
        data = filter_job_locations()
        job_counts = get_job_counts(data)
        return JSONResponse(content=job_counts.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@ router.get("/job_title_counts")
async def get_title_counts():
    try:
        data = load_data()
        job_titles = ['Data Engineer', 'Software Engineer', 'Data Analyst', 'Data Scientist',
                      'Backend Developer', 'UI UX Developer', 'Financial Analyst',
                      'Full stack developer', 'Supply Chain Manager']
        job_title_counts = filter_and_count_jobs(data, job_titles)
        return JSONResponse(content=job_title_counts.to_dict(orient="records"))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@ router.get("/salaries")
async def get_salaries():
    try:
        data = load_data()
        data = preprocess_salaries(data)
        selected_data = data[["MIN_SALARY", "MAX_SALARY"]]
        return selected_data.to_dict(orient="records")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@ router.get("/employment_types")
async def get_employment_types():
    try:
        data = load_data()
        data = filter_employment_types(data)
        employment_type_counts = get_employment_type_counts(data)
        return employment_type_counts.to_dict(orient="records")
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)