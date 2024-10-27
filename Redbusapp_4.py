import pandas as pd
import psycopg2
from psycopg2 import pool
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
import os
import logging
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
import hashlib
import re
from typing import Optional, Dict, Any, List
import json
from dataclasses import dataclass
import secrets
import ssl
import time

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data Validation Class
@dataclass
class BusDetails:
    """Data class for validating bus details"""
    bus_name: str
    bus_type: str
    start_time: datetime
    end_time: datetime
    price: float
    seats_available: int
    ratings: float
    route_name: str

    def __post_init__(self):
        if not isinstance(self.price, (int, float)) or self.price < 0:
            raise ValueError("Invalid price value")
        if not 0 <= self.ratings <= 5:
            raise ValueError("Ratings must be between 0 and 5")
        if self.start_time >= self.end_time:
            raise ValueError("Start time must be before end time")

# Security Configuration
class SecurityConfig:
    def __init__(self):
        self.salt = os.getenv('SALT', secrets.token_hex(16))
        self.ssl_context = ssl.create_default_context()
    
    def hash_password(self, password: str) -> str:
        return hashlib.sha256(f"{password}{self.salt}".encode()).hexdigest()
    
    def validate_input(self, input_str: str) -> bool:
        return bool(re.match("^[a-zA-Z0-9_\- ]*$", str(input_str)))

# Database Management
class DatabaseManager:
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialize_pool()
        return cls._instance

    def _initialize_pool(self):
        try:
            db_config = self._get_database_config()
            self._pool = pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                **db_config
            )
            logger.info("Database connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def _get_database_config(self) -> Dict[str, Any]:
        if os.getenv('STREAMLIT_RUNTIME_ENV') == 'cloud':
            return {
                'host': st.secrets['db_host'],
                'database': st.secrets['db_name'],
                'user': st.secrets['db_username'],
                'password': st.secrets['db_password'],
                'port': st.secrets['db_port'],
                'sslmode': 'require'
            }
        return {
            'host': 'localhost',
            'database': 'Redbusdb',
            'user': 'postgres',
            'password': 'samguna10',
            'port': '5432'
        }

    def get_connection(self):
        if self._pool:
            return self._pool.getconn()
        raise Exception("Connection pool not initialized")

    def return_connection(self, conn):
        if self._pool and conn:
            self._pool.putconn(conn)

# Data Access Layer
class BusDataAccess:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.security = SecurityConfig()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_filtered_buses(self, bus_type: str, min_fare: float, max_fare: float, 
                          rating: float, start_time: datetime, selected_route: str) -> Optional[pd.DataFrame]:
        conn = self.db_manager.get_connection()
        try:
            if not all(self.security.validate_input(str(x)) for x in [bus_type, selected_route]):
                raise ValueError("Invalid input detected")

            with conn.cursor() as cur:
                query, params = self._build_query(bus_type, min_fare, max_fare, 
                                                rating, start_time, selected_route)
                cur.execute(query, params)
                results = cur.fetchall()
                
                df = pd.DataFrame(results, columns=[
                    "ID", "Bus_name", "Bus_type", "Start_time", "End_time",
                    "Total_duration", "Price", "Seats_Available", "Ratings",
                    "Route_link", "Route_name"
                ])
                return df
        except Exception as e:
            logger.error(f"Error in get_filtered_buses: {e}")
            raise
        finally:
            self.db_manager.return_connection(conn)

    def _build_query(self, bus_type: str, min_fare: float, max_fare: float,
                    rating: float, start_time: datetime, selected_route: str):
        conditions = []
        params = []

        conditions.append("price BETWEEN %s AND %s")
        params.extend([min_fare, max_fare])

        conditions.append("route_name = %s")
        params.append(selected_route)

        if bus_type == "sleeper":
            conditions.append("Bus_type LIKE %s")
            params.append('%Sleeper%')
        elif bus_type == "semi-sleeper":
            conditions.append("Bus_type LIKE %s")
            params.append('%A/c Semi Sleeper%')
        else:
            conditions.append("(Bus_type NOT LIKE %s AND Bus_type NOT LIKE %s)")
            params.extend(['%Sleeper%', '%Semi-Sleeper%'])

        conditions.append("Ratings >= %s")
        params.append(rating)

        conditions.append("CAST(start_time AS time) >= %s")
        params.append(start_time)

        query = f"""
            SELECT * FROM bus_details 
            WHERE {' AND '.join(conditions)}
            ORDER BY price, start_time DESC
        """
        return query, params

# Cache Management
class CacheManager:
    @staticmethod
    @st.cache_data(ttl=3600)
    def load_route_lists() -> Dict[str, List[str]]:
        try:
            return {
                "Andhra Pradesh": pd.read_csv("df_a1.csv")["Route_name"].tolist(),
                "Telungana": pd.read_csv("df_t2.csv")["Route_name"].tolist(),
                "Kerala": pd.read_csv("df_k3.csv")["Route_name"].tolist(),
                "South Bengal": pd.read_csv("df_s4.csv")["Route_name"].tolist(),
                "West Bengal": pd.read_csv("df_w5.csv")["Route_name"].tolist(),
                "Bihar": pd.read_csv("df_b6.csv")["Route_name"].tolist(),
                "Haryana": pd.read_csv("df_h7.csv")["Route_name"].tolist(),
                "Rajastan": pd.read_csv("df_r8.csv")["Route_name"].tolist(),
                "Punjab": pd.read_csv("df_p9.csv")["Route_name"].tolist(),
                "Assam": pd.read_csv("df_as10.csv")["Route_name"].tolist()
            }
        except Exception as e:
            logger.error(f"Failed to load route lists: {e}")
            return {}

# Visualization Functions
def create_visualizations(df):
    st.subheader("Route Statistics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Average Price", f"₹{df['Price'].mean():.2f}")
    with col2:
        st.metric("Available Buses", len(df))
    with col3:
        st.metric("Average Rating", f"{df['Ratings'].mean():.1f}⭐")

    st.subheader("Price Distribution")
    fig_price = px.histogram(df, x="Price", nbins=20,
                            title="Distribution of Bus Prices",
                            labels={"Price": "Price (₹)", "count": "Number of Buses"})
    st.plotly_chart(fig_price)

    st.subheader("Bus Types Available")
    bus_type_counts = df['Bus_type'].value_counts()
    fig_types = px.pie(values=bus_type_counts.values,
                      names=bus_type_counts.index,
                      title="Distribution of Bus Types")
    st.plotly_chart(fig_types)

    st.subheader("Price vs Rating Analysis")
    fig_scatter = px.scatter(df, x="Price", y="Ratings",
                            hover_data=["Bus_name", "Bus_type"],
                            title="Price vs Rating Distribution")
    st.plotly_chart(fig_scatter)

# Main Application
def main():
    try:
        st.set_page_config(layout="wide")
        
        bus_data_access = BusDataAccess()
        cache_manager = CacheManager()

        web = option_menu(
            menu_title="🚌OnlineBus",
            options=["Home", "📍States and Routes"],
            icons=["house", "info-circle"],
            orientation="horizontal"
        )

        if web == "Home":
            st.image("Red.jpg", width=200)
            st.title("Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit")
            st.subheader(":blue[Domain:] Transportation")
            st.subheader(":blue[Objective:] ")
            st.markdown("The 'Redbus Data Scraping and Filtering with Streamlit Application' aims to revolutionize the transportation industry by providing a comprehensive solution for collecting, analyzing, and visualizing bus travel data.")
            
            st.subheader(":blue[Overview:]")
            st.markdown("""
            - **Selenium**: Automated web scraping tool for extracting data from websites
            - **Pandas**: Data manipulation and analysis
            - **PostgreSQL**: Database management and storage
            - **Streamlit**: Interactive web application framework
            """)
            
            st.subheader(":blue[Skill-take:]")
            st.markdown("Selenium, Python, Pandas, PostgreSQL, psycopg2, Streamlit.")
            st.subheader(":blue[Developed-by:] SAMUELSON G")

        elif web == "📍States and Routes":
            route_lists = cache_manager.load_route_lists()
            
            selected_state = st.selectbox("Lists of States", list(route_lists.keys()))
            selected_route = st.selectbox("List of routes", route_lists[selected_state])

            col1, col2, col3 = st.columns(3)
            with col1:
                select_type = st.selectbox("Choose bus type", ("sleeper", "semi-sleeper", "others"))
            with col2:
                min_fare = st.number_input("Minimum fare", min_value=0, max_value=10000, step=1)
                max_fare = st.number_input("Maximum fare", min_value=0, max_value=10000, step=1)
            with col3:
                select_rating = st.number_input("Choose rating (1.0 to 5.0)", 
                                              min_value=1.0, max_value=5.0, step=0.1)

            start_time = st.time_input("Select the start time")

            try:
                df_result = bus_data_access.get_filtered_buses(
                    select_type, min_fare, max_fare, select_rating, 
                    start_time, selected_route
                )

                if df_result is not None and not df_result.empty:
                    st.subheader("Bus Search Results")
                    st.dataframe(df_result)
                    create_visualizations(df_result)
                else:
                    st.warning("No buses found matching your criteria. Please try different filters.")

            except Exception as e:
                st.error(f"An error occurred while fetching bus details: {e}")
                logger.error(f"Error in display_routes_page: {e}")

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

        st.markdown("---")
        st.markdown("""
        <div style='text-align: center'>
            <p>Developed with ❤️ by SAMUELSON G</p>
            <p>Data sourced from RedBus</p>
        </div>
        """, unsafe_allow_html=True)

    except Exception as e:
        logger.error(f"Application error: {e}")
        st.error("An unexpected error occurred. Please try again later.")

if __name__ == "__main__":
    main()