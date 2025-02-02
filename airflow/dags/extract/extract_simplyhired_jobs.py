from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import date
from io import BytesIO, StringIO
from selenium import webdriver
import requests
import re
from extract.connections import aws_connection
# from connections import aws_connection

def clean_and_stage(jobs_df):
    '''
    Function to clean the dataframe and stage it to S3
    '''    
    try:
        # removing duplicate data (job_id)
        jobs_df.drop_duplicates(subset=['job_id'], keep="first", inplace=True)
        print("length after dropping duplicates: ", len(jobs_df))

        # Convert DataFrame to CSV format in memory
        csv_buffer = StringIO()
        jobs_df.to_csv(csv_buffer, index=False, sep="|")
        csv_buffer_encode = BytesIO(csv_buffer.getvalue().encode())     
        
        # Loading csv to S3
        client, bucket = aws_connection()
        
        # upload to s3
        response = client.upload_fileobj(csv_buffer_encode, bucket, "jobs/simplyhired_jobs.csv")
        
        print("Staged into S3")
        
        return "Success"
    
    except Exception as e:
        print("Exception in clean_and_stage function: ", e)   
        return "Failed"
    
def scrape_job_details(job_id, internal_driver, url):
    '''
    Function to scrape - "j_title","company","job_location","min salary","max salary","employment_type","job_url","date_posted","job_desc"
    '''
    # Fetch job page
    internal_driver.get(url)
    time.sleep(5)
    
    # Get page source     
    job_details_page_source = internal_driver.page_source
    job_content = BeautifulSoup(job_details_page_source, 'html.parser')
    print("-------------------")
    # Fetching job title
    try:
        j_title = job_content.find("h1", class_="chakra-heading css-yvgnf2").get_text().strip().replace("|"," ")
        print(j_title)
    except Exception as e:
        print("Exception while fetching job title: ",e)
        j_title = None
        
    # Fetching company name
    try:
        company = job_content.find("span", attrs={"data-testid":"viewJobCompanyName"}).get_text().strip().replace("|"," ")
        print(company)
    except Exception as e:
        print("Exception while fetching company: ",e)
        company = None
        
    # Fetching job location (Format: City, State)
    try:
        job_location = job_content.find("span", attrs={"data-testid":"viewJobCompanyLocation"}).get_text().strip().replace("|"," ")
        print(job_location)
    except Exception as e:
        print("Exception while fetching location: ",e)
        job_location=None
        
    # Fetching min_salary, max_salary
    try:
        salary = job_content.find("span", attrs={"data-testid":"viewJobBodyJobCompensation"}).get_text().strip().replace("|"," ").replace(",","").replace("K","000")
        print("salary: ", salary)
        # if min and max sal both available
        if(salary.count("$")==2):
            i = salary.index("$")
            salary = salary[i:]     # removing any words before $
            min_sal = int(float(salary.split("-")[0].strip().replace("$","")))
            max_sal = int(float(salary.split(" ")[2].strip().replace("$","")))
        else:
            #  if only min salary available
            if re.search('From', salary, re.IGNORECASE):
                min_sal = int(float(salary.split(" ")[1].strip().replace("$","")))
                max_sal = None
            #  if only max salary available
            elif re.search('Up to', salary, re.IGNORECASE):
                min_sal = None
                max_sal = int(float(salary.split(" ")[2].strip().replace("$","")))
            else:
                i = salary.index("$")
                salary = salary[i:]     # removing any words before $
                min_sal = int(float(salary.split(" ")[0].strip().replace("$","")))
                max_sal = None
    except Exception as e:
        min_sal, max_sal = None, None
    print("min: ", min_sal, " max: ", max_sal)
    
    # Fetch employment_type
    try:
        employment_type = job_content.find("span", attrs={"data-testid":"viewJobBodyJobDetailsJobType"}).get_text().strip().replace("|",",")
        print(employment_type)
    except Exception as e:
        employment_type = None
    
    # fetch job_desc
    try:
        qualifications = job_content.find("div", attrs={"data-testid":"viewJobQualificationsContainer"}).get_text().strip().replace("|"," ")
        job_desc = qualifications + job_content.find("div", attrs={"data-testid":"viewJobBodyJobFullDescriptionContent"}).get_text().strip().replace("|"," ").replace("\n"," ")
    except Exception as e:
        print("Exception while fetching job description: ", e)
        job_desc = None
    
    return {"job_id": job_id, "job_title": j_title, "company":company, "job_location": job_location, "min_salary": min_sal, 
            "max_salary":max_sal, "employment_type": employment_type, "source":"SimplyHired", "job_url": url, "date_posted": str(date.today()),"job_desc": f"{job_desc}"}
        
def scrape_simplyhired_jobs():
    '''
    function to scrape outer pages of jobs
    '''
    try:
        option = webdriver.ChromeOptions()
        option.add_argument('--incognito')
        option.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=option)
        internal_driver = webdriver.Chrome(options=option)
        
        jobs_df = pd.DataFrame(columns=["job_id","job_title","company","job_location","min_salary","max_salary","employment_type","source","job_url","date_posted","job_desc"])
        job_titles = ['Data Engineer','Software Engineer','Data Analyst','Data Scientist','Backend Developer','UI UX Developer','Financial Analyst','Full stack developer','Supply Chain Manager','Front End Developer','Devops Engineer', 'Product Manager']
        location = "United+States"
        
        for title in job_titles:
            temp_li = []
            start= 0 # starting point for pagination
            count_of_jobs_scraped = 0
            title=title.replace(" ","+")
            
            while count_of_jobs_scraped < 6:
                time.sleep(5)
                main_url = f"https://www.simplyhired.com/search?q={title}&l={location}&t=1"
                start+=10
                
                try:
                    # Send a GET request to the URL and store the response
                    driver.get(main_url)
                    print(count_of_jobs_scraped," -- ",main_url)
                    
                    # Find all list items(jobs postings)
                    parsed_val = driver.page_source
                    parsed_content = BeautifulSoup(parsed_val, "html.parser")
                    time.sleep(5)
                    
                    # Get page source for summary page
                    job_divs = parsed_content.find_all('div', attrs={"class":"css-f8dtpc"})
                    
                    for div in job_divs:
                        
                        if count_of_jobs_scraped < 6:
                            # Fetching job id
                            try:
                                job_id = div['data-jobkey']
                            except Exception as e:
                                print("Exception while fetching job id: ",e)
                                break
                            
                            # Fetching URL for job
                            url_for_job = f"https://www.simplyhired.com/job/{job_id}"
                            
                            # Fetching job details
                            job_details = scrape_job_details(job_id, internal_driver, url_for_job)
                            
                            # Add data to DataFrame
                            temp_li.append(job_details)
                            
                            # incrementing counter 
                            count_of_jobs_scraped+=1
                        else:
                            break
                                                    
                except Exception as e:
                    print("Internal exception: ", e)
            
            # Append list to dataframe
            temp_df = pd.DataFrame(temp_li)
            
            # concating 2 dataframes
            if len(jobs_df)==0:
                jobs_df = temp_df.copy()
            else:
                jobs_df = pd.concat([jobs_df, temp_df], ignore_index = True)
            print("jobs_df length:", len(jobs_df))
                
        # Final staging
        print("----------Final staging------------")
        res = clean_and_stage(jobs_df)
        
    except Exception as e:
        print("Exception in scrape_indeed_jobs function: ",e)
        
        if(len(jobs_df) != 0):
            # staging in S3
            res = clean_and_stage(jobs_df)
    driver.quit()
    internal_driver.quit()
            
# scrape_simplyhired_jobs()