# import pandas as pd
import requests
import streamlit as st
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')

def upload_files_to_s3(files):
    files_data = [("files", (file.name, file)) for file in files]
    access_token = st.session_state['access_token']

    # Define the headers with the Authorization header containing the access token
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    base_url = config['APIs']['base_url_auth']
    upload_resume_url = base_url + "userRoutes/upload"
    # Make the POST request to upload the files
    response = requests.post(upload_resume_url, files=files_data, headers=headers)

    if response.status_code == 200:
        result = response.json()
        if "uploaded_files" in result:
            uploaded_files = result["uploaded_files"]
            for file_info in uploaded_files:
                if file_info['status'] == 'File already exists':
                    st.error(file_info['file_name'] + ": " + file_info['status'])
                else:
                    st.success(file_info['file_name'] + ": " + file_info['status'])
        else:
            error_message = result.get("error", "Unknown error")
            st.error(f"Upload failed: {error_message}")
    else:
        error_message = response.json().get("error", "Unknown error")
        st.error(f"Upload failed: {error_message}")

def show_upload_files():
    st.title("Upload Resumes")

    uploaded_files = st.file_uploader("Choose PDF files", type='pdf', accept_multiple_files=True)

    if uploaded_files:
        if st.button("Confirm Upload"):
            upload_files_to_s3(uploaded_files)
