# main.py

import streamlit as st
from components.login_page import login_page
from components.signup_page import signup_page
import components.upload_page as upload_page
import components.get_job_matches as get_job_matches
import components.analytics as analytics
import components.get_missing_keywords as get_missing_keywords
from PIL import Image

# st.set_page_config(layout="wide")

PAGES = {
    "Upload Files": upload_page,
    "Find Jobs": get_job_matches,
    "Missing Keywords": get_missing_keywords,
    "Analytics Dashboard": analytics
}

def main():
    st.set_page_config(page_title="CareerSage")
    if 'logged_in' not in st.session_state:
        st.session_state['logged_in'] = False
    
    if not st.session_state['logged_in']:      
        st.title("CareerSage")
        st.title("Login/Signup")
        tab1, tab2 = st.tabs(["Login", "Signup"])
        
        with tab1:
            login_page()
            
        with tab2:
            signup_page()
    else:
        st.sidebar.title('Menu')
        selection = st.sidebar.radio("Go to", list(PAGES.keys()))

        if st.sidebar.button('Logout'):
            st.session_state['logged_in'] = False
            del st.session_state['access_token']
            st.rerun()

        if st.session_state['logged_in']:
            page = PAGES[selection]
            page_function = getattr(
                page, 'show_' + selection.lower().replace(' ', '_'))
            page_function()

if __name__ == "__main__":
    main()
