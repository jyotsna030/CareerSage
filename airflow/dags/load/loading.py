# from connections import snowflake_connection
from load.connections import snowflake_connection

def load_to_snowflake(csv_key):
    '''
    Function to load the data to snowflake table from stage.
    If the job already is present in table, update it.
    job is inserted only when not present in the table.
    '''
    try:
        conn, jobs_table = snowflake_connection()
        cur = conn.cursor()
        
        # Query to update the record if job with same job_id is present. Insert only when job_id is not present
        merge_query = f'''MERGE INTO {jobs_table} t
                            USING (SELECT $1 JOB_ID, 
                                            $2 JOB_TITLE, 
                                            $3 COMPANY, 
                                            $4 JOB_LOCATION, 
                                            $5 MIN_SALARY, 
                                            $6 MAX_SALARY, 
                                            $7 EMPLOYMENT_TYPE, 
                                            $8 SOURCE, 
                                            $9 URL, 
                                            $10 DATE_POSTED, 
                                            $11 DESCRIPTION from @jobs_stage/{csv_key} (FILE_FORMAT => PIPE_SEPARATED_FF)) s
                            ON t.JOB_ID = s.JOB_ID 
                            WHEN MATCHED THEN
                                UPDATE SET  t.JOB_ID = s.JOB_ID,
                                            t.JOB_TITLE = s.JOB_TITLE,
                                            t.COMPANY = s.COMPANY,
                                            t.JOB_LOCATION = s.JOB_LOCATION,
                                            t.MIN_SALARY = s.MIN_SALARY,
                                            t.MAX_SALARY = s.MAX_SALARY,
                                            t.EMPLOYMENT_TYPE = s.EMPLOYMENT_TYPE,
                                            t.SOURCE = s.SOURCE,
                                            t.URL = s.URL,
                                            t.DATE_POSTED = s.DATE_POSTED,
                                            t.DESCRIPTION = s.DESCRIPTION
                            WHEN NOT MATCHED THEN
                            INSERT (JOB_ID, JOB_TITLE, COMPANY, JOB_LOCATION, MIN_SALARY, MAX_SALARY, EMPLOYMENT_TYPE, SOURCE, URL, DATE_POSTED, DESCRIPTION)
                            VALUES (s.JOB_ID, s.JOB_TITLE, s.COMPANY, s.JOB_LOCATION, s.MIN_SALARY, s.MAX_SALARY, s.EMPLOYMENT_TYPE, s.SOURCE, s.URL, s.DATE_POSTED, s.DESCRIPTION);
                            '''
        
        print("merging data...")
                           
        # executing load
        cur.execute(merge_query)
        
        print("Data merged successfully")
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
    except Exception as e:
        print("Exception in load_to_snowflake function: ", e)
        
# load_to_snowflake("simplyhired_jobs.csv")