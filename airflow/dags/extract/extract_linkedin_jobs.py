import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from datetime import date
from extract.connections import aws_connection
from io import BytesIO, StringIO

def clean_and_stage(jobs_df):
    '''
    Function to clean the dataframe and stage it to S3
    '''    
    try:
        # removing duplicate data (job_id)
        jobs_df.drop_duplicates(subset=['job_id'], keep="first", inplace=True)
        print("length after dropping duplicates: ", len(jobs_df))
        
        # Convert DataFrame to CSV format in memory
        csv_buffer_linkedin = StringIO()
        jobs_df.to_csv(csv_buffer_linkedin, index=False, sep="|")
        csv_buffer_encode_linkedin = BytesIO(csv_buffer_linkedin.getvalue().encode())     
        
        # Loading csv to S3
        client, bucket = aws_connection()
        
        # upload to s3
        response = client.upload_fileobj(csv_buffer_encode_linkedin, bucket, "jobs/linkedin_jobs.csv")
        
        print("Staged into S3")
        
        return "Success"
    
    except Exception as e:
        print("Exception in clean_and_stage function: ", e)   
        return "Failed"
    
def scrape_linkedin_jobs():
    try:
        jobs_df = pd.DataFrame(columns=["job_id","job_title","company","job_location","min_salary","max_salary","employment_type","source","job_url","date_posted","job_desc"])
        job_titles = ['Data Engineer','Software Engineer','Data Analyst','Data Scientist','Backend Developer','UI UX Developer','Financial Analyst','Full stack developer','Supply Chain Manager','Front End Developer','Devops Engineer', 'Product Manager']
        location = "United States"
        fail_response_count = 0
        
        # construct URL for LinkedIn job search
        for title in job_titles:
            if fail_response_count < 100:
                start= 0 # starting point for pagination
                count_of_jobs_scraped = 0
                temp_li = []
                
                while count_of_jobs_scraped < 8 and fail_response_count < 100:
                    time.sleep(5) 
                    li_url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?f_TPR=r86400&keywords={title}&location={location}&start={start}"
                    start += 10
                    
                    try:    
                        # Send a GET request to the URL and store the response
                        response = requests.get(li_url)
                        print("page fetch: ", response.status_code, "--", li_url)
                                            
                        if response.status_code == 200:    
                            # Find all list items(jobs postings)
                            list_data = response.text
                            list_soup = BeautifulSoup(list_data, "html.parser")
                            page_jobs = list_soup.find_all("li")
                            
                            # if url has no data
                            if page_jobs:
                                
                                #Itetrate through job postings to find job ids
                                for job in page_jobs:
                                    base_card_div = job.find("div", {"class": "base-card"})
                                    job_id = base_card_div.get("data-entity-urn").split(":")[3]
                                    
                                    time.sleep(5)
                                    job_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
                                    
                                    # Send a GET request to the job URL and parse the reponse
                                    job_response = requests.get(job_url)
                                    job_soup = BeautifulSoup(job_response.text, "html.parser")
                                    print("job response: ", job_response.status_code, " -- ", job_url)
                                    print("fail response count: ", fail_response_count)
                                    
                                    # Try to extract and store the job title
                                    if count_of_jobs_scraped < 8 and fail_response_count < 100:                                  
                                        if job_response.status_code == 200:
                                            
                                            # extract job title
                                            try:
                                                j_title = job_soup.find("h2", {"class":"top-card-layout__title font-sans text-lg papabear:text-xl font-bold leading-open text-color-text mb-0 topcard__title"}).text.strip().replace("|"," ")
                                            except:
                                                j_title = None
                                                
                                            # extract company name
                                            try:
                                                company_name = job_soup.find("a", {"class": "topcard__org-name-link topcard__flavor--black-link"}).text.strip().replace("|"," ")
                                            except:
                                                company_name = None
                                                
                                            # extract job location
                                            try:
                                                job_location = job_soup.find("span", {"class":"topcard__flavor topcard__flavor--bullet"}).text.strip().replace("|"," ")
                                            except:
                                                job_location = None
                                                
                                            # fetch salary
                                            try:
                                                salary = job_soup.find("div", class_="salary compensation__salary").get_text().strip()
                                                # if min and max sal both available
                                                if(salary.count("$")==2):
                                                    min_sal = int(float(salary.split("-")[0].strip().replace("$","").replace(",","").split("/")[0].split(".")[0]))
                                                    max_sal = int(float(salary.split(" ")[2].replace("-","").replace("$","").strip().replace(",","").split("/")[0].split(".")[0]))

                                                #  if only min salary available
                                                else:
                                                    min_sal = int(float(salary.split(" ")[0].strip().replace("$","").replace(",","").split("/")[0].split(".")[0]))
                                                    max_sal = None
                                                
                                            except Exception as e:
                                                min_sal, max_sal = None, None
                                                
                                            # Fetch employment_type
                                            try:
                                                employment_type = job_soup.find_all("span", attrs={"class":"description__job-criteria-text"})[1].get_text().strip()
                                            except Exception as e:
                                                employment_type = None
                                                
                                            # extract job description
                                            try:
                                                job_desc_div = job_soup.find("div", {"class":"show-more-less-html__markup"})
                                                job_desc = job_desc_div.get_text().strip().replace("|"," ")
                                            except:
                                                job_desc=None
                                            
                                            job_details = {"job_id": job_id, "job_title": j_title, "company":company_name, "job_location": job_location, "min_salary": min_sal, "max_salary":max_sal, "employment_type": employment_type, "source":"LinkedIn", "job_url": f"https://www.linkedin.com/jobs/view/{job_id}", "date_posted": str(date.today()),"job_desc": f"{job_desc}"}
                                            
                                            # appending row to dataframe
                                            temp_li.append(job_details)
                                            count_of_jobs_scraped+=1
                                        
                                        else:
                                            fail_response_count += 1
                                            
                                    else:
                                        break
                            else:
                                print("No more jobs available")
                                break
                        else:
                            fail_response_count+=1
                            start-=10
                                        
                    except Exception as e:
                        print("Exception in scrape_linkedin_jobs func: ",e)
                
                # Append list to dataframe
                temp_df = pd.DataFrame(temp_li)
                
                # concating temp_df to jobs_df
                if len(jobs_df)==0:
                    jobs_df = temp_df.copy()
                else:
                    jobs_df = pd.concat([jobs_df, temp_df], ignore_index = True)
                print("jobs_df length: ", len(jobs_df))
            else:
                print("Too many failed requests.")
                break
            
                    
        # Final staging
        print("-----------Final staging-----------")
        res = clean_and_stage(jobs_df)
        
    except Exception as e:
        print("Exception: ",e)
        
        if(len(jobs_df) != 0):
            # staging in S3
            res = clean_and_stage(jobs_df)
