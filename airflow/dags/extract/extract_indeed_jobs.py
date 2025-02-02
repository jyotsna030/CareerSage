from bs4 import BeautifulSoup
from selenium import webdriver
import pandas as pd
import time
from datetime import date
from io import BytesIO, StringIO
import requests
from extract.connections import aws_connection

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
        response = client.upload_fileobj(csv_buffer_encode, bucket, "jobs/indeed_jobs.csv")
        
        print("Staged into S3")
        
        return "Success"
    
    except Exception as e:
        print("Exception in clean_and_stage function: ", e)   
        return "Failed"
    
def scrape_job_details(job_id, url):
    '''
    Function to scrape - "j_title","company","job_location","min salary","max salary","employment_type","job_url","date_posted","job_desc"
    '''
    # Fetch job page
    job_response = requests.get(url)
    time.sleep()
    
    # Get page source for summary page
    print(url)
    job_details_page_source = job_response.text
    job_content = BeautifulSoup(job_details_page_source, 'html.parser')
    print(job_content)
    
    # Fetching job title
    try:
        job_heading = job_content.find("h1", class_="jobsearch-JobInfoHeader-title")
        j_title = job_heading.find("span").get_text().strip().replace("|"," ")
    except Exception as e:
        print("Exception while fetching job title: ",e)
        j_title = None
        
    # Fetching company name
    try:
        company_div = job_content.find("div", attrs={"data-testid":"inlineHeader-companyName"})
        company = company_div.get_text().strip().replace("|"," ")
    except Exception as e:
        print("Exception while fetching company: ",e)
        company=None
        
    # Fetching job location (Format: City, State)
    try:
        location_div = job_content.find("div", attrs={"data-testid":"inlineHeader-companyLocation"})
        if location_div:
            location_list = location_div.get_text().strip().split(",")
            if len(location_list)==3:
                location_list.pop(0)
                
            temp=location_list[0].strip().split(" ")
            # if company name in location, remove it, keep only city name and state name
            if(len(temp)>2):
                job_location = None
            else:
                job_location = location_list[0]+", "+location_list[1].split(" ")[1]
        else:
            # for remote job, div class is different
            job_location = job_content.find("div", attrs={"class":"css-17cdm7w eu4oa1w0"}).get_text().strip()
    except Exception as e:
        print("Exception while fetching location: ",e)
        job_location=None
        
    # Fetching min_salary, max_salary and employment_type
    try:
        sal_jobtype_div = job_content.find("div", attrs={"id":"salaryInfoAndJobType"})
    except Exception as e:
        print("Exception while fetching salary and employment type div: ", e)
        sal_jobtype_div = None
    
    if sal_jobtype_div:
        # fetch salary
        try:
            salary = sal_jobtype_div.find("span", class_="css-19j1a75 eu4oa1w0").get_text().strip()
            # if min and max sal both available
            if(salary.count("$")==2):
                min_sal = int(salary.split("-")[0].strip().replace("$","").replace(",",""))
                max_sal = int(salary.split(" ")[2].replace("-","").replace("$","").strip().replace(",",""))
            #  if only min salary available
            else:
                min_sal = int(salary.split(" ")[0].strip().replace("$","").replace(",",""))
                max_sal = None
        except Exception as e:
            min_sal, max_sal = None, None
            
        # Fetch employment_type
        try:
            employment_type = sal_jobtype_div.find("span", attrs={"class":"css-k5flys eu4oa1w0"}).get_text().replace("- ","").strip()
        except Exception as e:
            employment_type = None
    else:
        min_sal, max_sal, employment_type = None, None, None
    
    # fetch job_desc
    try:
        job_desc = job_content.find("div", attrs={"id":"jobDescriptionText"}).get_text().strip().replace("|"," ").replace("\n"," ")
    except Exception as e:
        print("Exception while fetching job description: ", e)
        job_desc = None
    
    return {"job_id": job_id, "job_title": j_title, "company":company, "job_location": job_location, "min_salary": min_sal, 
            "max_salary":max_sal, "employment_type": employment_type, "source":"Indeed", "job_url": url, "date_posted": str(date.today()),"job_desc": f"{job_desc}"}
        
def scrape_indeed_jobs():
    '''
    function to scrape outer pages of jobs
    '''
    try:
        jobs_df = pd.DataFrame(columns=["job_id","job_title","company","job_location","min_salary","max_salary","employment_type","source","job_url","date_posted","job_desc"])
        job_titles = ['Data Engineer','Software Engineer','Data Analyst','Data Scientist','Backend Developer','UI UX Developer','Financial Analyst','Full stack developer','Supply Chain Manager','Front End Developer','Devops Engineer', 'Product Manager']
        location = "United%20States"
        
        for title in job_titles:
            temp_li = []
            start= 0 # starting point for pagination
            count_of_jobs_scraped = 0
            title=title.replace(" ","%20")
            
            
            while count_of_jobs_scraped < 8:
                time.sleep(2)
                main_url = f"https://www.indeed.com/jobs?q={title}&l={location}&fromage=1&start={start}"
                start+=10
                
                try:
                    # Send a GET request to the URL and store the response
                    response = requests.get(main_url)
                    print("page fetch: ", response.status_code, "--", main_url)
                    
                    if response.status_code == 200:    
                        # Find all list items(jobs postings)
                        list_data = response.text
                        parsed_content = BeautifulSoup(list_data, "html.parser")
                        time.sleep(2)
                        
                        # Get page source for summary page
                        job_divs = parsed_content.find_all('div', attrs={"class":"css-dekpa e37uo190"})
                        
                        for div in job_divs:
                            
                            if count_of_jobs_scraped < 8:
                                # Fetching job id
                                try:
                                    job_id = div.find("a", class_ = "jcs-JobTitle").get("data-jk")
                                except Exception as e:
                                    print("Exception while fetching job id: ",e)
                                    break
                                
                                # Fetching URL for job
                                url_for_job = "https://www.indeed.com"+div.find("a", class_ = "jcs-JobTitle")['href']
                                
                                # Fetching job details
                                job_details = scrape_job_details(job_id, url_for_job)
                                
                                # Add data to DataFrame
                                temp_li.append(job_details)
                                
                                # incrementing counter 
                                count_of_jobs_scraped+=1
                            else:
                                break
                    else:
                        start-=10
                                                    
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