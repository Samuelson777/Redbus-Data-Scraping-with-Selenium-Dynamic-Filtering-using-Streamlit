# importing libraries
import pandas as pd
import psycopg2
import streamlit as slt
from streamlit_option_menu import option_menu
import plotly.express as px
import time

#Andhra bus
lists_A=[]
df_A=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_a1.csv")
for i,r in df_A.iterrows():
    lists_A.append(r["Route_name"])

#Telungana bus
lists_T=[]
df_T=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_t2.csv")
for i,r in df_T.iterrows():
    lists_T.append(r["Route_name"])

# kerala bus
lists_k=[]
df_k=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_k3.csv")
for i,r in df_k.iterrows():
    lists_k.append(r["Route_name"])

# South bengal bus 
lists_SB=[]
df_SB=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_s4.csv")
for i,r in df_SB.iterrows():
    lists_SB.append(r["Route_name"])

#West bengal bus
lists_W=[]
df_W=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_w5.csv")
for i,r in df_W.iterrows():
    lists_W.append(r["Route_name"])

#Bihar bus
lists_b=[]
df_B=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_b6.csv")
for i,r in df_B.iterrows():
    lists_b.append(r["Route_name"])

# Haryana bus
lists_H=[]
df_H=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_h7.csv")
for i,r in df_H.iterrows():
    lists_H.append(r["Route_name"])

#Rajastan bus
lists_R=[]
df_R=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_r8.csv")
for i,r in df_R.iterrows():
    lists_R.append(r["Route_name"])

#Punjab bus
lists_P=[]
df_P=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_p9.csv")
for i,r in df_P.iterrows():
    lists_P.append(r["Route_name"])

#Assam bus
lists_AS=[]
df_AS=pd.read_csv(r"F:\Education and Job\Guvi\Redbus_M\Scraping_1\df_as10.csv")
for i,r in df_AS.iterrows():
    lists_AS.append(r["Route_name"])


#setting up streamlit page
slt.set_page_config(layout="wide")

web=option_menu(menu_title="🚌OnlineBus",
                options=["Home","📍States and Routes"],
                icons=["house","info-circle"],
                orientation="horizontal"
                )
# Home page setting
if web=="Home":
    slt.image(r"F:\Education and Job\Guvi\Redbus_M\Red.jpg", width=200)
    slt.title("Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit")
    slt.subheader(":blue[Domain:]  Transportation")
    slt.subheader(":blue[Ojective:] ")
    slt.markdown("The 'Redbus Data Scraping and Filtering with Streamlit Application' aims to revolutionize the transportation industry by providing a comprehensive solution for collecting, analyzing, and visualizing bus travel data. By utilizing Selenium for web scraping, this project automates the extraction of detailed information from Redbus, including bus routes, schedules, prices, and seat availability. By streamlining data collection and providing powerful tools for data-driven decision-making, this project can significantly improve operational efficiency and strategic planning in the transportation industry.")
    slt.subheader(":blue[Overview:]") 
    slt.markdown("Selenium: Selenium is a tool used for automating web browsers. It is commonly used for web scraping, which involves extracting data from websites. Selenium allows you to simulate human interactions with a web page, such as clicking buttons, filling out forms, and navigating through pages, to collect the desired data...")
    slt.markdown('''Pandas: Use the powerful Pandas library to transform the dataset from CSV format into a structured dataframe.
                    Pandas helps data manipulation, cleaning, and preprocessing, ensuring that data was ready for analysis.''')
    slt.markdown('''Postgresql: With help of Postgresql to establish a connection to a Postgresql database, enabling seamless integration of the transformed dataset
                    and the data was efficiently inserted into relevant tables for storage and retrieval.''')
    slt.markdown("Streamlit: Developed an interactive web application using Streamlit, a user-friendly framework for data visualization and analysis.")
    slt.subheader(":blue[Skill-take:]")
    slt.markdown("Selenium, Python, Pandas, Postgresql ,psycopg2, Streamlit.")
    slt.subheader(":blue[Developed-by:]  SAMUELSON G")

# States and Routes page setting
if web == "📍States and Routes":
    S = slt.selectbox("Lists of States", ["Adhra Pradesh", "Telugana", "Kerala", "South Bengal", "West Bengal", 
                                          "Bihar", "Haryana", "Rajastan", "Punjab",  "Assam"])
    
    col1,col2=slt.columns(2)
    with col1:
        select_type = slt.radio("Choose bus type", ("sleeper", "semi-sleeper", "others"))
    with col2:
        select_fare = slt.radio("Choose bus fare range", ("50-1000", "1000-2000", "2000 and above"))
    TIME=slt.time_input("select the time")

    # Adhra Pradesh bus fare filtering
    if S == "Adhra Pradesh":
        A = slt.selectbox("List of routes",lists_A)

        def type_and_fare_A(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, A, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_A(select_type, select_fare)
        slt.dataframe(df_result)

    # Telugana bus fare filtering
    if S=="Telugana":
        T=slt.selectbox("list of routes",lists_T)

        def type_and_fare_T(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, T, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_T(select_type, select_fare)
        slt.dataframe(df_result)
          

    # Kerala bus fare filtering
    if S=="Kerala":
        K=slt.selectbox("list of routes",lists_k)

        def type_and_fare_K(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, K, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_K(select_type, select_fare)
        slt.dataframe(df_result)

    # South Bengal bus fare filtering
    if S=="South Bengal":
        SB=slt.selectbox("list of routes",lists_SB)

        def type_and_fare_SB(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, SB, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_SB(select_type, select_fare)
        slt.dataframe(df_result)

    # West Bengal bus fare filtering
    if S=="West Bengal":
        W=slt.selectbox("list of routes",lists_W)

        def type_and_fare_W(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, W, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_W(select_type, select_fare)
        slt.dataframe(df_result)
          

    # Bihar bus fare filtering       
    if S=="Bihar":
        B=slt.selectbox("list of rotes",lists_b)

        def type_and_fare_B(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, B, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_B(select_type, select_fare)
        slt.dataframe(df_result)
    
    # Haryana bus fare filtering
    if S=="Haryana":
        H=slt.selectbox("list of rotes",lists_H)

        def type_and_fare_H(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, H, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_H(select_type, select_fare)
        slt.dataframe(df_result)


    # Rajastan bus fare filtering
    if S=="Rajastan":
        R=slt.selectbox("list of rotes",lists_R)

        def type_and_fare_R(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, R, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_R(select_type, select_fare)
        slt.dataframe(df_result)


    # Punjab bus fare filtering
    if S=="Punjab":
        P=slt.selectbox("list of rotes",lists_P)

        def type_and_fare_P(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, P, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_P(select_type, select_fare)
        slt.dataframe(df_result)


    # Assam bus fare filtering
    if S=="Assam":
        AS=slt.selectbox("list of rotes",lists_AS)

        def type_and_fare_AS(bus_type, fare_range):
            with psycopg2.connect(host="localhost", user="postgres", password="samguna10", database="Redbusdb") as conn:
                my_cursor = conn.cursor()

                # Define fare range based on selection
                if fare_range == "50-1000":
                    fare_min, fare_max = 50, 1000
                elif fare_range == "1000-2000":
                    fare_min, fare_max = 1000, 2000
                else:
                    fare_min, fare_max = 2000, 100000  # assuming a high max value for "2000 and above"

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
                    AND CAST(start_time AS time) >= %s
                    ORDER BY price, start_time DESC
                '''
                my_cursor.execute(query, (fare_min, fare_max, AS, bus_type_param, TIME))
                out = my_cursor.fetchall()

            df = pd.DataFrame(out, columns=[
                "ID", "Bus_name", "Bus_type", "Start_time", "End_time", "Total_duration",
                "Price", "Seats_Available", "Ratings", "Route_link", "Route_name"
            ])
            return df
        
        df_result = type_and_fare_AS(select_type, select_fare)
        slt.dataframe(df_result)