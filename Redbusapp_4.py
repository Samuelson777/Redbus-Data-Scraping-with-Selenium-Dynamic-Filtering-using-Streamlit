# 1. Import required libraries
import pandas as pd
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import time
from datetime import datetime
import random

# 2. Define error handling decorator
def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            st.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

# 3. Database Connection
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="samguna10",
        database="Redbusdb"
    )

# 4. Load route data from CSV files
lists_A = pd.read_csv("df_a1.csv")["Route_name"].tolist()
lists_T = pd.read_csv("df_t2.csv")["Route_name"].tolist()
lists_k = pd.read_csv("df_k3.csv")["Route_name"].tolist()
lists_SB = pd.read_csv("df_s4.csv")["Route_name"].tolist()
lists_W = pd.read_csv("df_w5.csv")["Route_name"].tolist()
lists_b = pd.read_csv("df_b6.csv")["Route_name"].tolist()
lists_H = pd.read_csv("df_h7.csv")["Route_name"].tolist()
lists_R = pd.read_csv("df_r8.csv")["Route_name"].tolist()
lists_P = pd.read_csv("df_p9.csv")["Route_name"].tolist()
lists_AS = pd.read_csv("df_as10.csv")["Route_name"].tolist()

# 5. Page Configuration
st.set_page_config(layout="wide")

# 6. Navigation Menu
web = option_menu(
    menu_title="🚌OnlineBus",
    options=["Home", "📍States and Routes"],
    icons=["house", "info-circle"],
    orientation="horizontal"
)

# 7. Home Page
if web == "Home":
    st.image("Red.jpg", width=200)
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

# 8. States and Routes Page
if web == "📍States and Routes":
    # Create mapping of states to routes
    state_to_routes = {
        "Andhra Pradesh": lists_A,
        "Telungana": lists_T,
        "Kerala": lists_k,
        "South Bengal": lists_SB,
        "West Bengal": lists_W,
        "Bihar": lists_b,
        "Haryana": lists_H,
        "Rajastan": lists_R,
        "Punjab": lists_P,
        "Assam": lists_AS
    }

    # State and route selection
    selected_state = st.selectbox("Lists of States", list(state_to_routes.keys()))
    available_routes = state_to_routes[selected_state]
    selected_route = st.selectbox("List of routes", available_routes)

    # Filter options
    col1, col2, col3 = st.columns(3)
    with col1:
        select_type = st.selectbox("Choose bus type", ("sleeper", "semi-sleeper", "others"))
    with col2:
        min_fare = st.number_input("Minimum fare", min_value=0, max_value=10000, step=1)
        max_fare = st.number_input("Maximum fare", min_value=0, max_value=10000, step=1)
    with col3:
        select_rating = st.number_input("Choose rating (1.0 to 5.0)", min_value=1.0, max_value=5.0, step=0.1)

    start_time = st.time_input("Select the start time")

    # Function to get filtered bus details
    @handle_exceptions
    def type_and_fare_selected_route(bus_type, min_fare, max_fare, rating, start_time):
        with psycopg2.connect(host="localhost", user="postgres", password="samguna10",port="5432", database="Redbusdb") as conn:
            my_cursor = conn.cursor()

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

            query = '''
                SELECT * FROM bus_details 
                WHERE price BETWEEN %s AND %s
                AND route_name = %s
                AND ''' + bus_type_condition + '''
                AND Ratings >= %s
                AND CAST(start_time AS time) >= %s
                ORDER BY price, start_time DESC
            '''
            
            # Execute query with parameters
            if isinstance(bus_type_param, tuple):
                my_cursor.execute(query, (min_fare, max_fare, selected_route, bus_type_param[0], bus_type_param[1], rating, start_time))
            else:
                my_cursor.execute(query, (min_fare, max_fare, selected_route, bus_type_param, rating, start_time))
            
            out = my_cursor.fetchall()

        # Convert to DataFrame
        df = pd.DataFrame(out, columns=[
            "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
            "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
        ])
        return df

    # Display results
    df_result = type_and_fare_selected_route(select_type, min_fare, max_fare, select_rating, start_time)
    
    if df_result is not None and not df_result.empty:
        # Add some basic statistics
        st.subheader("Bus Search Results")
        st.dataframe(df_result)
        
        # Statistics Section
        st.subheader("Route Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Average Price", f"₹{df_result['Price'].mean():.2f}")
        with col2:
            st.metric("Available Buses", len(df_result))
        with col3:
            st.metric("Average Rating", f"{df_result['Ratings'].mean():.1f}⭐")

        # Visualizations
        st.subheader("Price Distribution")
        fig = px.histogram(df_result, x="Price", nbins=20, 
                          title="Distribution of Bus Prices",
                          labels={"Price": "Price (₹)", "count": "Number of Buses"})
        st.plotly_chart(fig)

        # Bus Type Distribution
        st.subheader("Bus Types Available")
        bus_type_counts = df_result['Bus_type'].value_counts()
        fig = px.pie(values=bus_type_counts.values, 
                    names=bus_type_counts.index,
                    title="Distribution of Bus Types")
        st.plotly_chart(fig)

        # Price vs Rating Scatter Plot
        st.subheader("Price vs Rating Analysis")
        fig = px.scatter(df_result, x="Price", y="Ratings",
                        hover_data=["Bus_name", "Bus_type"],
                        title="Price vs Rating Distribution")
        st.plotly_chart(fig)

    else:
        st.warning("No buses found matching your criteria. Please try different filters.")

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
