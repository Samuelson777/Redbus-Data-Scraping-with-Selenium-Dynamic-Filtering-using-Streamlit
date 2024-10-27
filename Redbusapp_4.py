import sys
import subprocess
import pkg_resources
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime
import logging
import os
from streamlit_option_menu import option_menu

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Package installation function
def install_missing_packages():
    """Install required packages if missing"""
    required = {
        'streamlit': 'streamlit==1.32.0',
        'pandas': 'pandas==1.5.3',
        'psycopg2-binary': 'psycopg2-binary==2.9.9',
        'plotly': 'plotly==5.18.0',
        'streamlit-option-menu': 'streamlit-option-menu==0.3.12'
    }
    
    installed = {pkg.key for pkg in pkg_resources.working_set}
    missing = [required[pkg] for pkg in required if pkg not in installed]
    
    if missing:
        print("Installing missing packages...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing])

# Database connection
@st.cache_resource
def init_connection():
    """Initialize database connection"""
    try:
        return psycopg2.connect(
            host="localhost",
            database="Redbusdb",
            user="postgres",
            password="samguna10"
        )
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

def get_data(query, params=None):
    """Execute database query and return results"""
    conn = init_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                if params:
                    cur.execute(query, params)
                else:
                    cur.execute(query)
                return cur.fetchall()
        except Exception as e:
            st.error(f"Query execution error: {e}")
            return None
        finally:
            conn.close()
    return None

# Data loading functions
@st.cache_data
def load_route_lists():
    """Load route lists from CSV files"""
    try:
        route_lists = {}
        csv_files = {
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
        
        for state, file in csv_files.items():
            try:
                df = pd.read_csv(file)
                route_lists[state] = df["Route_name"].tolist()
            except FileNotFoundError:
                st.warning(f"Warning: {file} not found for {state}")
                route_lists[state] = []
            except Exception as e:
                st.error(f"Error loading {file}: {e}")
                route_lists[state] = []
        
        return route_lists
    except Exception as e:
        st.error(f"Error loading route lists: {e}")
        return {}

# Visualization functions
def create_visualizations(df):
    """Create and display visualizations for bus data"""
    if df is not None and not df.empty:
        st.subheader("Route Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Average Price", f"₹{df['Price'].mean():.2f}")
        with col2:
            st.metric("Available Buses", len(df))
        with col3:
            st.metric("Average Rating", f"{df['Ratings'].mean():.1f}⭐")

        # Price Distribution
        st.subheader("Price Distribution")
        fig_price = px.histogram(
            df, 
            x="Price", 
            nbins=20,
            title="Distribution of Bus Prices",
            labels={"Price": "Price (₹)", "count": "Number of Buses"}
        )
        st.plotly_chart(fig_price)

        # Bus Type Distribution
        st.subheader("Bus Types Available")
        bus_type_counts = df['Bus_type'].value_counts()
        fig_types = px.pie(
            values=bus_type_counts.values,
            names=bus_type_counts.index,
            title="Distribution of Bus Types"
        )
        st.plotly_chart(fig_types)

        # Price vs Rating Analysis
        st.subheader("Price vs Rating Analysis")
        fig_scatter = px.scatter(
            df, 
            x="Price", 
            y="Ratings",
            hover_data=["Bus_name", "Bus_type"],
            title="Price vs Rating Distribution"
        )
        st.plotly_chart(fig_scatter)

def get_filtered_buses(bus_type, min_fare, max_fare, rating, start_time, selected_route):
    """Get filtered bus data from database"""
    try:
        query = """
        SELECT * FROM bus_details 
        WHERE price BETWEEN %s AND %s
        AND route_name = %s
        AND Ratings >= %s
        AND CAST(start_time AS time) >= %s
        """
        
        if bus_type == "sleeper":
            query += " AND Bus_type LIKE '%Sleeper%'"
        elif bus_type == "semi-sleeper":
            query += " AND Bus_type LIKE '%A/c Semi Sleeper%'"
        else:
            query += " AND Bus_type NOT LIKE '%Sleeper%' AND Bus_type NOT LIKE '%Semi-Sleeper%'"
        
        query += " ORDER BY price, start_time DESC"
        
        results = get_data(query, (min_fare, max_fare, selected_route, rating, start_time))
        
        if results:
            return pd.DataFrame(results, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time",
                "Total_duration", "Price", "Seats_Available", "Ratings",
                "Route_link", "Route_name"
            ])
        return None
    except Exception as e:
        st.error(f"Error getting filtered buses: {e}")
        return None

# Main application
def main():
    try:
        # Install required packages
        install_missing_packages()
        
        # Page configuration
        st.set_page_config(layout="wide", page_title="RedBus Analysis")
        
        # Navigation menu
        web = option_menu(
            menu_title="🚌OnlineBus",
            options=["Home", "📍States and Routes"],
            icons=["house", "info-circle"],
            orientation="horizontal"
        )
        
        # Home page
        if web == "Home":
            st.image("Red.jpg", width=200)
            st.title("Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit")
            st.subheader(":blue[Domain:] Transportation")
            st.subheader(":blue[Objective:] ")
            st.markdown("""
            The 'Redbus Data Scraping and Filtering with Streamlit Application' aims to revolutionize 
            the transportation industry by providing a comprehensive solution for collecting, analyzing, 
            and visualizing bus travel data.
            """)
            
            st.subheader(":blue[Overview:]")
            st.markdown("""
            - **Selenium**: Automated web scraping tool for extracting data from websites
            - **Pandas**: Data manipulation and analysis
            - **PostgreSQL**: Database management and storage
             - **Streamlit**: Interactive web application for data visualization and analysis
            """)
            
            st.subheader(":blue[Skill-take:]")
            st.markdown("Selenium, Python, Pandas, Postgresql, psycopg2, Streamlit.")
            st.subheader(":blue[Developed-by:] SAMUELSON G")
        
        # States and Routes page
        elif web == "📍States and Routes":
            # Load route lists
            route_lists = load_route_lists()
            
            # State and route selection
            selected_state = st.selectbox("Lists of States", list(route_lists.keys()))
            available_routes = route_lists[selected_state]
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
            
            # Get filtered bus data
            df_result = get_filtered_buses(select_type, min_fare, max_fare, select_rating, start_time, selected_route)
            
            # Display results
            if df_result is not None and not df_result.empty:
                create_visualizations(df_result)
            else:
                st.warning("No buses found matching your criteria. Please try different filters.")
        
        # Footer
        st.markdown("---")
        st.markdown("""
        <div style='text-align: center'>
            <p>Developed with ❤️ by SAMUELSON G</p>
            <p>Data sourced from RedBus</p>
        </div>
        """, unsafe_allow_html=True)
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
        st.error(f"Error: {e}")

if __name__ == "__main__":
    main()