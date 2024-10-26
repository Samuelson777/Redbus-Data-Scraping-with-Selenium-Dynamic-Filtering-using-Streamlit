import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from datetime import datetime
import os

# Constants
DB_CONFIG = {
    "host": "localhost",
    "user": "postgres",
    "password": "samguna10",
    "database": "Redbusdb"
}

# Data paths
DATA_PATH = "data/"
ROUTE_FILES = {
    "Andhra Pradesh": "df_a1.csv",
    "Telungana": "df_t2.csv",
    "Kerala": "df_k3.csv",
    "South Bengal": "df_s4.csv",
    "West Bengal": "df_w5.csv",
    "Bihar": "df_b6.csv",
    "Haryana": "df_h7.csv",
    "Rajastan": "df_r8.csv",
    "Punjab": "df_p9.csv",
    "Assam": "df_as10.csv"
}

# Error handling decorator
def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

# Database connection
@st.cache_resource
def init_connection():
    return psycopg2.connect(**DB_CONFIG)

# Load route data
@st.cache_data
def load_route_data():
    route_data = {}
    for state, file in ROUTE_FILES.items():
        try:
            df = pd.read_csv(os.path.join(DATA_PATH, file))
            route_data[state] = df["Route_name"].tolist()
        except Exception as e:
            st.warning(f"Could not load data for {state}: {str(e)}")
            route_data[state] = []
    return route_data

# Main function to get filtered bus details
@handle_exceptions
def get_filtered_bus_details(selected_route, bus_type, min_fare, max_fare, rating, start_time):
    conn = init_connection()
    with conn.cursor() as cursor:
        # Define bus type condition
        if bus_type == "sleeper":
            bus_type_condition = "Bus_type LIKE %s"
            bus_type_param = '%Sleeper%'
        elif bus_type == "semi-sleeper":
            bus_type_condition = "Bus_type LIKE %s"
            bus_type_param = '%A/c Semi Sleeper %'
        else:
            bus_type_condition = "Bus_type NOT LIKE %s AND Bus_type NOT LIKE %s"
            bus_type_param = ('%Sleeper%', '%Semi-Sleeper%')

        query = f'''
            SELECT * FROM bus_details 
            WHERE price BETWEEN %s AND %s
            AND route_name = %s
            AND {bus_type_condition}
            AND Ratings >= %s
            AND CAST(start_time AS time) >= %s
            ORDER BY price, start_time DESC
        '''
        
        params = [min_fare, max_fare, selected_route]
        if isinstance(bus_type_param, tuple):
            params.extend(bus_type_param)
        else:
            params.append(bus_type_param)
        params.extend([rating, start_time])
        
        cursor.execute(query, params)
        results = cursor.fetchall()

        df = pd.DataFrame(results, columns=[
            "ID", "Bus_name", "Bus_type", "Start_time", "End_time", 
            "Total_duration", "Price", "Seats_Available", "Ratings", 
            "Route_link", "Route_name"
        ])
        return df

def main():
    # Page configuration
    st.set_page_config(layout="wide", page_title="OnlineBus Booking")

    # Navigation menu
    web = option_menu(
        menu_title="🚌OnlineBus",
        options=["Home", "📍States and Routes"],
        icons=["house", "info-circle"],
        orientation="horizontal"
    )

    if web == "Home":
        # Home page content
        st.image("data\Red.jpg", width=200)
        st.title("Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit")
        st.subheader(":blue[Domain:] Transportation")
        st.subheader(":blue[Objective:] ")
        st.markdown("The 'Redbus Data Scraping and Filtering with Streamlit Application' aims to revolutionize the transportation industry by providing a comprehensive solution for collecting, analyzing, and visualizing bus travel data. By utilizing Selenium for web scraping, this project automates the extraction of detailed information from Redbus, including bus routes, schedules, prices, and seat availability. By streamlining data collection and providing powerful tools for data-driven decision-making, this project can significantly improve operational efficiency and strategic planning in the transportation industry.")
        st.subheader(":blue[Overview:]")
        st.markdown("Selenium: Selenium is a tool used for automating web browsers. It is commonly used for web scraping, which involves extracting data from websites. Selenium allows you to simulate human interactions with a web page, such as clicking buttons, filling out forms, and navigating through pages, to collect the desired data...")
        st.markdown('''Pandas: Use the powerful Pandas library to transform the dataset from CSV format into a structured dataframe.
                        Pandas helps data manipulation, cleaning, and preprocessing, ensuring that data was ready for analysis.''')
        st.markdown('''Postgresql: With help of Postgresql to establish a connection to a Postgresql database, enabling seamless integration of the transformed dataset
                        and the data was efficiently inserted into relevant tables for storage and retrieval.''')
        st.markdown("Streamlit: Developed an interactive web application using Streamlit, a user-friendly framework for data visualization and analysis.")
        st.subheader(":blue[Skill-take:]")
        st.markdown("Selenium, Python, Pandas, Postgresql, psycopg2, Streamlit.")
        st.subheader(":blue[Developed-by:] SAMUELSON G")

        
    elif web == "📍States and Routes":
        # Load route data
        route_data = load_route_data()
        
        # State and route selection
        selected_state = st.selectbox("Select State", list(route_data.keys()))
        selected_route = st.selectbox("Select Route", route_data[selected_state])

        # Filter options
        col1, col2, col3 = st.columns(3)
        with col1:
            select_type = st.selectbox("Bus Type", ["sleeper", "semi-sleeper", "others"])
        with col2:
            min_fare = st.number_input("Minimum Fare", 0, 10000, 0)
            max_fare = st.number_input("Maximum Fare", 0, 10000, 5000)
        with col3:
            select_rating = st.number_input("Minimum Rating", 1.0, 5.0, 3.0, 0.1)

        start_time = st.time_input("Departure Time")

        # Get and display results
        df_result = get_filtered_bus_details(
            selected_route, select_type, min_fare, max_fare, 
            select_rating, start_time
        )

        if df_result is not None and not df_result.empty:
            # Display results and statistics
            st.subheader("Search Results")
            st.dataframe(df_result)

            # Statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Average Price", f"₹{df_result['Price'].mean():.2f}")
            with col2:
                st.metric("Available Buses", len(df_result))
            with col3:
                st.metric("Average Rating", f"{df_result['Ratings'].mean():.1f}⭐")

            # Visualizations
            st.subheader("Analytics")
            
            # Price Distribution
            fig1 = px.histogram(df_result, x="Price", 
                              title="Price Distribution",
                              labels={"Price": "Price (₹)", "count": "Number of Buses"})
            st.plotly_chart(fig1)

            # Bus Type Distribution
            fig2 = px.pie(df_result, names="Bus_type", 
                         title="Bus Type Distribution")
            st.plotly_chart(fig2)

            # Price vs Rating
            fig3 = px.scatter(df_result, x="Price", y="Ratings",
                            hover_data=["Bus_name"],
                            title="Price vs Rating Analysis")
            st.plotly_chart(fig3)
        else:
            st.warning("No buses found matching your criteria.")

        
        # Additional Information
        st.sidebar.header("About the Search")
        st.sidebar.info("""
        This search tool helps you find buses based on:
        - Route selection
        - Bus type preference
        - Price range
        - Rating threshold
        - Departure time
        
        Use the filters above to refine your search.
        """)

        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center'>
            <p>Developed with ❤️ by SAMUELSON G</p>
            <p>Data sourced from RedBus</p>
        </div>
        """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()