import streamlit as st
import re
import requests
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')

def validate_email(email):
    # Email must end with @northeastern.edu
    return re.match(r"^[a-zA-Z0-9._%+-]+@gmail\.com$", email)


def validate_password(password):
    # Password must have at least 8 characters, 1 uppercase, 1 lowercase, 1 number, and 1 special character
    return re.match(r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$", password)

def validate_username(username):
    # Username must start with a letter, have at least 3 characters, and contain only letters, numbers, and underscore
    return re.match(r"^[a-zA-Z][a-zA-Z0-9_]{2,}$", username)


def signup_page():
    with st.form("signup_form", clear_on_submit=True):
        email = st.text_input("Email", key="register_email")
        username = st.text_input("Username", key="register_username")
        password = st.text_input(
            "Password", type="password", key="register_password")
        password_confirm = st.text_input(
            "Confirm Password", type="password", key="register_password_confirm")
        submitted = st.form_submit_button("Signup")
        if submitted:
            if not validate_email(email):
                st.error("Email must be a valid gmail email.")
            elif not validate_username(username):
                st.error("Username must start with a letter and contain only letters, numbers, and underscore.")
            elif not validate_password(password):
                st.error(
                    "Password must have at least 8 characters, including 1 uppercase, 1 lowercase, 1 number, and 1 special character.")
            elif password != password_confirm:
                st.error("Passwords do not match.")
            else:
                try:
                    user_data = {
                    "email": email,
                    "username": username,
                    "password": password
                    }
                    base_url = config['APIs']['base_url_auth']
                    signup_url = base_url + "signup"
                    response = requests.post(signup_url, json=user_data)
                    if response.status_code == 200:
                        st.success("Registration successful!")
                    else:
                        print(response.json())
                        error_message = response.json().get("detail", "Unknown error")
                        st.error(f"Registration failed: {error_message}")
                except Exception as e:
                    print(e)
            
