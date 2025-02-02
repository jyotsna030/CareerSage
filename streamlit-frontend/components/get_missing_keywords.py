import streamlit as st
from streamlit_modal import Modal
from components.get_job_matches import getResumeList
import requests
import configparser
 
config = configparser.ConfigParser()
config.read('./configuration.properties')
 
 
def show_missing_keywords():
 
    st.title("Missing Content in Resume")
    
    selected_resume=st.session_state["selected_resume"]
    job_desc = st.session_state['selected_job_details']['jobDetails']['description']
    
    if "selected_job_details" in st.session_state:
        job = st.session_state['selected_job_details']
        
        score = round(job['score']*100,2)
        job_title = job['jobDetails']['job_title']
        company = job['jobDetails']['company']
        location = job['jobDetails']['job_location']
        date_posted = job['jobDetails']['date_posted']
        url = job['jobDetails']['url']
        source= job['jobDetails']['source']

        st.markdown(
            f"""
            ## Job Details
            - **Job Title:** {job_title}
            - **Match:** {score}%
            - **Company:** {company}
            - **Location:** {location}
            - **Date Posted:** {date_posted}
            - **Source:** {source}
            """,
            unsafe_allow_html=True
        )

    
    if selected_resume:
        st.write(f"You selected resume: {selected_resume}")
        
 
    view_missing_content_button = st.button("Get Missing Content")
 
    modal = Modal(
        key="missing_content_modal",
        title="Missing Content",
        padding=20,
        max_width=1000
    )
 
    if view_missing_content_button:
        access_token = st.session_state['access_token']
        headers = {"Authorization": f"Bearer {access_token}"}
        base_url = config['APIs']['base_url_auth']
        url = base_url + f"userRoutes/getMissingKeywords/?file_name={selected_resume}&job_desc={job_desc}"
        response = requests.get(
            url, headers=headers)
        if response.status_code == 200:
            st.markdown(response.json(), unsafe_allow_html=True)
        else:
            st.error("Failed to fetch.")
 
            