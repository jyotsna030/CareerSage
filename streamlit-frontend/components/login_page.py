import streamlit as st
import requests
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')

def authenticate_user(username, password):
    try:
        base_url = config['APIs']['base_url_auth']
        login_url = base_url + "token"
        response = requests.post(login_url, data={"username": username, "password": password})

        # Check if the request was successful
        if response.status_code == 200:
            access_token = response.json().get("access_token")
            if "access_token" not in st.session_state:
                st.session_state['access_token'] = access_token
            return True
        else:
            return False
    except requests.RequestException as e:
        st.error(f"Error occurred during authentication: {e}")
        return False

def login_page():
    with st.form("login_form", clear_on_submit=True):
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        submitted = st.form_submit_button("Login")
        if submitted:
            response = authenticate_user(username, password)
            if response:
                st.session_state['logged_in'] = True
                st.success("Login successful!")
                st.rerun()
            else:
                st.error("Incorrect username or password.")