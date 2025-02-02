import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import configparser

config = configparser.ConfigParser()
config.read('./configuration.properties')


def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.error("Failed to fetch data")


def show_analytics_dashboard():
    # Change this if your backend runs on a different address
    backend_url = config['APIs']['base_url_auth']

    st.title('Job Analytics Dashboard')

    # Fetch data from backend
    job_counts = fetch_data(f"{backend_url}analyticsRoute/job_counts")
    job_title_counts = fetch_data(
        f"{backend_url}analyticsRoute/job_title_counts")
    salaries = fetch_data(f"{backend_url}analyticsRoute/salaries")
    employment_types = fetch_data(
        f"{backend_url}analyticsRoute/employment_types")

    # Display job density map by state
    st.header('Job Density Map by State')
    fig = px.choropleth(job_counts,
                        locations="STATE",
                        locationmode='USA-states',
                        color="COUNT",
                        color_continuous_scale="blugrn",
                        scope="usa",
                        labels={'COUNT': 'Job Count'},
                        title="Job Density Across the US by State")
    st.plotly_chart(fig)

    # Display job count by title
    st.header('Job Count by Title')
    fig_bar = px.bar(job_title_counts.head(6),
                     x='JOB_TITLE',
                     y='COUNT',
                     color='JOB_TITLE',
                     labels={'JOB_TITLE': 'Job Title',
                             'COUNT': 'Number of Jobs'},
                     title='Number of Jobs by Title',
                     color_discrete_sequence=px.colors.qualitative.Vivid)
    st.plotly_chart(fig_bar)

    # Display box plot of salaries
    st.header('Box Plot of Salaries')
    fig_min_salary = px.box(salaries, x="MIN_SALARY", orientation='h',
                            title="Horizontal Box Plot of Minimum Salaries", color_discrete_sequence=['#636EFA'])
    st.plotly_chart(fig_min_salary)

    fig_max_salary = px.box(salaries, x="MAX_SALARY", orientation='h',
                            title="Horizontal Box Plot of Maximum Salaries", color_discrete_sequence=['#EF553B'])
    st.plotly_chart(fig_max_salary)

    # Display employment type distribution
    st.header('Employment Type Distribution')
    fig_employment = px.pie(employment_types,
                            values='COUNT',
                            names='EMPLOYMENT_TYPE',
                            title='Proportion of Jobs by Employment Type',
                            color='EMPLOYMENT_TYPE',
                            color_discrete_sequence=px.colors.qualitative.Pastel)
    fig_employment.update_traces(textposition='inside', textinfo='percent+label',
                                 textfont_size=14, textfont_color='black')
    st.plotly_chart(fig_employment)


if __name__ == "__main__":
    show_analytics_dashboard()
