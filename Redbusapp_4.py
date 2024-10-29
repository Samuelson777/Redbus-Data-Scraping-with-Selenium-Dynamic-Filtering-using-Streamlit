# 1. IMPORTS
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import mysql.connector
from mysql.connector import Error

# 2. PAGE CONFIGURATION
st.set_page_config(
    page_title="Red Bus Analysis",
    page_icon=":bus:",
    layout="wide"
)

# 3. UTILITY FUNCTIONS
def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Error as e:
            st.error(f"Database Error: {str(e)}")
            return None
        except Exception as e:
            st.error(f"Error in {func.__name__}: {str(e)}")
            return None
    return wrapper

def init_connection():
    try:
        connection = mysql.connector.connect(
            host="redbusdetails.c5mc0y22khp0.ap-south-1.rds.amazonaws.com",
            database="red_bus_details",
            user="root",
            password="samguna10"
        )
        if connection.is_connected():
            st.success("Successfully connected to the database!")
            return connection
    except Error as e:
        st.error(f"Error connecting to MySQL database: {e}")
        return None

def load_route_data():
    """Load route data from CSV files"""
    try:
        route_files = {
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
        
        return {state: pd.read_csv(file)["Route_name"].tolist() 
                for state, file in route_files.items()}
    except Exception as e:
        st.error(f"Error loading CSV files: {str(e)}")
        return {}

# 4. DATABASE OPERATIONS CLASS
class DatabaseOperations:
    @staticmethod
    @st.cache_data(ttl=3600)
    def get_cached_query_results(query, params=None):
        """Execute query with caching"""
        conn = init_connection()
        if conn is not None:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.fetchall()
            finally:
                conn.close()
        return None

# 5. NAVIGATION MENU
web = option_menu(
    menu_title="🚌OnlineBus",
    options=["Home", "📍States and Routes"],
    icons=["house", "info-circle"],
    orientation="horizontal"
)

# 6. PAGE CONTENT
def show_home_page():
    st.image("Red.jpg", width=200)
    st.title("Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit")
    st.subheader(":blue[Domain:] Transportation")
    st.subheader(":blue[Objective:] ")
    st.markdown("The 'Redbus Data Scraping and Filtering with Streamlit Application' aims to revolutionize the transportation industry by providing a comprehensive solution for collecting, analyzing, and visualizing bus travel data. By utilizing Selenium for web scraping, this project automates the extraction of detailed information from Redbus, including bus routes, schedules, prices, and seat availability. By streamlining data collection and providing powerful tools for data-driven decision-making, this project can significantly improve operational efficiency and strategic planning in the transportation industry.")
    st.subheader(":blue[Overview:]")
    st.markdown("Selenium: Selenium is a tool used for automating web browsers. It is commonly used for web scraping, which involves extracting data from websites. Selenium allows you to simulate human interactions with a web page, such as clicking buttons, filling out forms, and navigating through pages, to collect the desired data...")
    st.markdown('''Pandas: Use the powerful Pandas library to transform the dataset from CSV format into a structured dataframe.
                    Pandas helps data manipulation, cleaning, and preprocessing, ensuring that data was ready for analysis.''')
    st.markdown('''MySQL: With help of SQL to establish a connection to a SQL database, enabling seamless integration of the transformed dataset
                    and the data was efficiently inserted into relevant tables for storage and retrieval.''')
    st.markdown("Streamlit: Developed an interactive web application using Streamlit, a user-friendly framework for data visualization and analysis.")
    st.subheader(":blue[Skill-take:]")
    st.markdown("Selenium, Python, Pandas, MySQL, Streamlit.")
    st.subheader(":blue[Developed-by:] SAMUELSON G")

def show_states_and_routes():
    conn = init_connection()
    if conn is not None:
        try:
            # Load data and create interface
            state_to_routes = load_route_data()
            
            # Selection widgets
            selected_state = st.selectbox("Lists of States", list(state_to_routes.keys()))
            selected_route = st.selectbox("List of routes", state_to_routes[selected_state])

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

            # Query construction
            query = '''
                SELECT * FROM bus_details 
                WHERE price BETWEEN %s AND %s
                AND route_name = %s
                AND Ratings >= %s
                AND CAST(start_time AS time) >= %s
            '''

            # Add bus type condition
            if select_type == "sleeper":
                query += " AND Bus_type LIKE '%Sleeper%'"
            elif select_type == "semi-sleeper":
                query += " AND Bus_type LIKE '%A/c Semi Sleeper%'"
            else:
                query += " AND Bus_type NOT LIKE '%Sleeper%' AND Bus_type NOT LIKE '%Semi-Sleeper%'"

            query += " ORDER BY price, start_time DESC"

            # Execute query
            params = (min_fare, max_fare, selected_route, select_rating, start_time)
            results = DatabaseOperations.get_cached_query_results(query, params)

            if results:
                # Convert to DataFrame
                df_result = pd.DataFrame(results, columns=[
                    "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                    "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
                ])

                # Display results
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

                # Additional Information
                st.subheader("Bus Schedule Information")
                schedule_df = df_result[['Bus_name', 'Start_time', 'End_time', 'Total_duration']]
                st.dataframe(schedule_df)

            else:
                st.warning("No buses found matching your criteria. Please try different filters.")

        except Exception as e:
            st.error(f"Error: {str(e)}")
    else:
        st.error("Database connection failed. Please check your database credentials.")

        
# 7. MAIN APP LOGIC
def main():
    if web == "Home":
        show_home_page()
    elif web == "📍States and Routes":
        show_states_and_routes()

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center'>
        <p>Developed with ❤️ by SAMUELSON G</p>
        <p>Data sourced from RedBus</p>
        <p>© 2024 All rights reserved</p>
    </div>
    """, unsafe_allow_html=True)

    # Error handling decorator (if needed for additional functions)
    def handle_exceptions(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Error as e:
                st.error(f"Database Error: {str(e)}")
                return None
            except Exception as e:
                st.error(f"Error in {func.__name__}: {str(e)}")
                return None
        return wrapper

 # 8. RUN APP
if __name__ == "__main__":
    main()
