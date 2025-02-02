from streamlit_modal import Modal
import streamlit as st
import configparser
import requests

config = configparser.ConfigParser()
config.read('./configuration.properties')

def getResumeList():
    access_token = st.session_state['access_token']

    # Define the headers with the Authorization header containing the access token
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    base_url = config['APIs']['base_url_auth']
    resumeList_url = base_url + "userRoutes/files"
    response = requests.get(resumeList_url, headers=headers)

    if response.status_code == 200:
        result = response.json()
        return result  # Return the list of resume names obtained from the backend
    else:
        return []  
    
# Define function to fetch job recommendations
def fetch_job_recommendations(selected_resume):
    try:
        access_token = st.session_state['access_token']
        headers = {"Authorization": f"Bearer {access_token}"}
        base_url = config['APIs']['base_url_auth']
        url = base_url + f"userRoutes/getJobRecommendations?file_name={selected_resume}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json(), None  
        else:
            error_msg = response.json().get('detail', 'Unknown error occurred.')
            return None, error_msg 
    except Exception as e:
        error_msg = str(e)
        return None, error_msg
    

def display_jobs(recommended_jobs, selected_resume):
    if recommended_jobs:
        if "selected_job_details" not in st.session_state:
            st.session_state['selected_job_details'] = recommended_jobs[0]
        for job in recommended_jobs:
            st.markdown(
                """
                <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0-beta3/css/all.min.css" rel="stylesheet">
                """,
                unsafe_allow_html=True
            )

            # Extracted job details
            score = round(job['score']*100,2)
            job_title = job['jobDetails']['job_title']
            company = job['jobDetails']['company']
            location = job['jobDetails']['job_location']
            date_posted = job['jobDetails']['date_posted']
            url = job['jobDetails']['url']
            source= job['jobDetails']['source']
            # description = job['jobDetails']['description']
            # button_label = f"Get Missing Keywords for {job_title}"
            st.markdown(
                f"""
                <div style="border: 1px solid #e6e6e6; border-radius: 5px; padding: 10px; margin-bottom: 20px;">
                <div style="font-size: 22px; margin-bottom: 11px;">
                <span>{job_title}</span>
                <a href="{url}" style="text-decoration: none;">
                    <i class="fa fa-external-link-alt" style="color: #e6e9ed;"></i>
                </a>
                </div>
                    <p style="font-size: 20px;">Match: {score} %</p>
                    <p>Company: {company}</p>
                    <p>Location: {location}</p>
                    <p>Date Posted: {date_posted}</p>
                    <p>Source: {source}</p>
                """,
                unsafe_allow_html=True
            )
    else:
        st.error("Failed to fetch job recommendations. Please try again.")
        
def show_find_jobs():
    st.title("Find Jobs")

    resume_names = getResumeList()

    selected_resume = st.selectbox("Select a resume", resume_names)

    if selected_resume:
        if "selected_resume" not in st.session_state:
            st.session_state["selected_resume"]=selected_resume
        st.write(f"You selected resume: {selected_resume}")
        
        get_job_matches_button = st.button("Get Job Matches")
        view_resume_button = st.button("View Resume")
            
        modal = Modal(
            key="resume_modal",
            title="Resume",
            padding=50,
            max_width=1000
        )

        if view_resume_button:
            access_token = st.session_state['access_token']
            headers = {"Authorization": f"Bearer {access_token}"}
            base_url = config['APIs']['base_url_auth']
            url = base_url + f"userRoutes/getResume/?file_name=" + selected_resume
            response = requests.get(
                url, headers=headers)
            if response.status_code == 200:
                st.markdown(
                        f'<iframe src="{url}" width="1000" height="1000"></iframe>', unsafe_allow_html=True)
            else:
                st.error("Failed to fetch the resume.")
        #     modal.open()

        # if modal.is_open():
        #     with modal.container():
        #         access_token = st.session_state['access_token']
        #         headers = {"Authorization": f"Bearer {access_token}"}
        #         base_url = config['APIs']['base_url_auth']
        #         url = base_url + f"userRoutes/getResume/?file_name=" + selected_resume
        #         response = requests.get(
        #             url, headers=headers)
        #         if response.status_code == 200:
        #             st.markdown(
        #                 f'<iframe src="{url}" width="1000" height="1000"></iframe>', unsafe_allow_html=True)
        #         else:
        #             st.error("Failed to fetch the resume.")

        if get_job_matches_button:
            recommended_jobs, error_msg = fetch_job_recommendations(selected_resume)
            if error_msg:
                st.error(error_msg)
            else:
                display_jobs(recommended_jobs,selected_resume)
