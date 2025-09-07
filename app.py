import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# db connection
def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT", "5432"),
        )
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

# get metrics of data
def get_metrics():
    conn = connect_to_database()
    if conn is None:
        return None, None, None, None
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT SUM(Transaction_count) FROM agg_transaction")
        total_transactions = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(Registered_Users) FROM agg_user")
        total_users = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(Insurance_count) FROM agg_insurance")
        total_insurance = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT SUM(Transaction_amount) FROM agg_transaction")
        total_amount = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return total_transactions, total_users, total_insurance, total_amount
        
    except Exception as e:
        st.error(f"Error fetching metrics: {e}")
        conn.close()
        return None, None, None, None

st.set_page_config(
    page_title="PhonePe Dashboard",
    page_icon="📱",
    layout="wide"
)

st.title("PhonePe Transaction Insights Dashboard")

st.sidebar.title("Navigation")

page_option = st.sidebar.selectbox(
    "Choose Page:",
    ["🏠 Home", "📊 Analysis"]
)


if page_option == "🏠 Home":
    st.session_state.page = "home"
elif page_option == "📊 Analysis":
    st.session_state.page = "analysis"


if "page" not in st.session_state:
    st.session_state.page = "home"


if st.session_state.page == "home":    
    st.write("""
    This dashboard helps you understand PhonePe transaction data across India.
    """)
    
    st.subheader("📈 Metrics")
    
    total_transactions, total_users, total_insurance, total_amount = get_metrics()
    
    if total_transactions is not None:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", f"{total_transactions:,}")
        
        with col2:
            st.metric("Total Users", f"{total_users:,}")
        
        with col3:
            st.metric("Insurance Records", f"{total_insurance:,}")
        
        with col4:
            st.metric("Total Amount", f"₹{total_amount:,.0f}")
        

    else:
        st.error("Unable to fetch metrics from database. Please check your database connection.")
        # Fallback
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Transactions", "Database Error")
        
        with col2:
            st.metric("Total Users", "Database Error")
        
        with col3:
            st.metric("Insurance Records", "Database Error")
        
        with col4:
            st.metric("Total Amount", "Database Error")
    
    # India Map Visualization
    st.divider()
    st.markdown("<h2 style='text-align: center; color: #1f77b4;'>🗺️ India Map - State-wise Data Visualization</h2>", unsafe_allow_html=True)
    st.divider()
    
    # Dropdowns for data selection - positioned at top
    st.markdown("<h4 style='text-align: center; margin-bottom: 20px;'>Select Data Parameters</h4>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_type = st.selectbox(
            "Select Data Type:",
            ["Transactions", "Users"],
            key="home_data_type"
        )
    
    with col2:
        # Get available years from database
        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT CAST(Year AS TEXT) FROM agg_transaction 
                    UNION 
                    SELECT DISTINCT CAST(Year AS TEXT) FROM agg_user 
                    ORDER BY CAST(Year AS TEXT)
                """)
                available_years = [row[0] for row in cursor.fetchall()]
                conn.close()
                
                year = st.selectbox(
                    "Select Year:",
                    available_years,
                    key="home_year",
                    on_change=lambda: st.rerun()
                )
            except Exception as e:
                st.error(f"Error fetching years: {e}")
                year = "2024"
        else:
            year = "2024"
    
    with col3:
        # Get available quarters for selected year
        if conn:
            try:
                conn = connect_to_database()
                cursor = conn.cursor()
                if data_type == "Transactions":
                    cursor.execute("""
                        SELECT DISTINCT Quarter FROM agg_transaction 
                        WHERE CAST(Year AS TEXT) = %s 
                        ORDER BY Quarter
                    """, (year,))
                else:  # Users
                    cursor.execute("""
                        SELECT DISTINCT Quarter FROM agg_user 
                        WHERE CAST(Year AS TEXT) = %s 
                        ORDER BY Quarter
                    """, (year,))
                
                available_quarters = [f"Q{row[0]}" for row in cursor.fetchall()]
                conn.close()
                
                quarter = st.selectbox(
                    "Select Quarter:",
                    available_quarters,
                    key="home_quarter"
                )
            except Exception as e:
                st.error(f"Error fetching quarters: {e}")
                quarter = "Q1"
        else:
            quarter = "Q1"
    
    # Fetch data for the map
    conn = connect_to_database()
    if conn:
        try:
            cursor = conn.cursor()
            
            
            quarter_num = int(quarter[1])
            
            if data_type == "Transactions":
                cursor.execute("""
                    SELECT State, SUM(Transaction_count) as total_count, SUM(Transaction_amount) as total_amount
                    FROM agg_transaction 
                    WHERE CAST(Year AS TEXT) = %s AND Quarter = %s
                    GROUP BY State
                """, (year, quarter_num))
            elif data_type == "Users":
                cursor.execute("""
                    SELECT State, SUM(Registered_Users) as total_count, SUM(App_Opens) as total_amount
                    FROM agg_user 
                    WHERE CAST(Year AS TEXT) = %s AND Quarter = %s
                    GROUP BY State
                """, (year, quarter_num))
            
            map_data = cursor.fetchall()
            conn.close()
            
            if map_data:
                # Create DataFrame for map
                df_map = pd.DataFrame(map_data, columns=['State', 'Count', 'Amount'])
                
                df_map['Count'] = pd.to_numeric(df_map['Count'], errors='coerce')
                df_map['Amount'] = pd.to_numeric(df_map['Amount'], errors='coerce')
                
                # Remove any rows with null or invalid data
                df_map = df_map.dropna()
                df_map = df_map[df_map['Count'] > 0]
                
                if len(df_map) == 0:
                    st.warning("No valid data found after cleaning. Please check your database.")
                else:
                    
                    # state mapping
                    state_mapping = {
                        'andaman-&-nicobar-islands': 'Andaman & Nicobar',
                        'andhra-pradesh': 'Andhra Pradesh',
                        'arunachal-pradesh': 'Arunachal Pradesh',
                        'assam': 'Assam',
                        'bihar': 'Bihar',
                        'chandigarh': 'Chandigarh',
                        'chhattisgarh': 'Chhattisgarh',
                        'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
                        'delhi': 'Delhi',
                        'goa': 'Goa',
                        'gujarat': 'Gujarat',
                        'haryana': 'Haryana',
                        'himachal-pradesh': 'Himachal Pradesh',
                        'jammu-&-kashmir': 'Jammu & Kashmir',
                        'jharkhand': 'Jharkhand',
                        'karnataka': 'Karnataka',
                        'kerala': 'Kerala',
                        'ladakh': 'Ladakh',
                        'lakshadweep': 'Lakshadweep',
                        'madhya-pradesh': 'Madhya Pradesh',
                        'maharashtra': 'Maharashtra',
                        'manipur': 'Manipur',
                        'meghalaya': 'Meghalaya',
                        'mizoram': 'Mizoram',
                        'nagaland': 'Nagaland',
                        'odisha': 'Odisha',
                        'puducherry': 'Puducherry',
                        'punjab': 'Punjab',
                        'rajasthan': 'Rajasthan',
                        'sikkim': 'Sikkim',
                        'tamil-nadu': 'Tamil Nadu',
                        'telangana': 'Telangana',
                        'tripura': 'Tripura',
                        'uttar-pradesh': 'Uttar Pradesh',
                        'uttarakhand': 'Uttarakhand',
                        'west-bengal': 'West Bengal'
                    }
                
                # State name mapping
                df_map['State_Clean'] = df_map['State'].map(state_mapping).fillna(df_map['State'])
                color_column = 'Count'
                title_suffix = "Count"
                
                try:
                    fig = go.Figure(data=go.Choropleth(
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locationmode='geojson-id',
                        locations=df_map['State_Clean'],
                        z=df_map[color_column],
                        autocolorscale=False,
                        colorscale='Viridis',
                        marker_line_color='peachpuff',
                        colorbar=dict(
                            title={'text': title_suffix},
                            thickness=15,
                            len=0.35,
                            bgcolor='rgba(255,255,255,0.6)',
                            xanchor='left',
                            x=0.01,
                            yanchor='bottom',
                            y=0.05
                        )
                    ))

                    # Map projection for India
                    fig.update_geos(
                        visible=False,
                        projection=dict(
                            type='conic conformal',
                            parallels=[12.472944444, 35.172805555556],
                            rotation={'lat': 24, 'lon': 80}
                        ),
                        lonaxis={'range': [68, 98]},
                        lataxis={'range': [6, 38]}
                    )

                    # Map layout
                    fig.update_layout(
                        title=dict(
                            text=f"{data_type} {title_suffix} by State - {quarter} {year}",
                            xanchor='center',
                            x=0.5,
                            yref='paper',
                            yanchor='bottom',
                            y=1,
                            pad={'b': 10}
                        ),
                        margin={'r': 0, 't': 30, 'l': 0, 'b': 0},
                        height=800,
                        width=None
                    )
                    
                except Exception as map_creation_error:
                    st.error(f"Error creating choropleth map: {map_creation_error}")
                    fig = None
                
                map_col, stats_col = st.columns([3, 1])
                
                # Left column: Display the choropleth map
                with map_col:
                    if fig is not None:
                        st.subheader("📊 Interactive Map")
                        st.plotly_chart(fig, use_container_width=True, height=700)
                    else:
                        st.warning("Choropleth map creation failed")
                
                # Right column: Display top 5 states
                with stats_col:                    
                    st.markdown("<h3 style='text-align: center; margin-bottom: 20px; color: #3477eb;'>🏆 Top 5 States</h3>", unsafe_allow_html=True)
                    top_states = df_map.nlargest(5, color_column)
                    
                    for i, (_, row) in enumerate(top_states.iterrows(), 1):
                        st.markdown(f"""
                        <div style="
                            background-color: white;
                            border: 1px solid #dee2e6;
                            border-radius: 8px;
                            padding: 12px;
                            margin-top: 24px;
                            margin-bottom: 10px;
                            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                        ">
                            <div style="font-weight: bold; font-size: 24px; color: #414ad1; margin-bottom: 8px;">
                                {i}. {row['State']}
                            </div>
                            <div style="font-size: 16px; color: #000108; margin-bottom: 4px;">
                                Total Count  - {row['Count']:,}
                            </div>
                            <div style="font-size: 16px; color: #000108;">
                                Total Amount  - ₹{row['Amount']:,.0f}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                st.subheader(f"📊 {data_type} Summary Statistics")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Total States", len(df_map))
                
                with col2:
                    st.metric(f"Total {title_suffix}", f"{df_map[color_column].sum():,}")
                
                with col3:
                    st.metric(f"Average {title_suffix}", f"{df_map[color_column].mean():,.0f}")
                
                with col4:
                    st.metric(f"Max {title_suffix}", f"{df_map[color_column].max():,}")
                
            else:
                st.warning("No data available for the selected criteria.")
                
        except Exception as e:
            st.error(f"Error fetching map data: {e}")
            if conn:
                conn.close()
    else:
        st.error("Unable to connect to database for map visualization.")
    

########################## Analysis Page ##########################

elif st.session_state.page == "analysis":
    st.header("📊 Analysis Dashboard")
    
    # Analysis type selection
    analysis_type = st.selectbox(
        "Choose Analysis Type:",
        [
            "Select Analysis",
            "Decoding Transaction Dynamics",
            "Device Dominance & User Engagement", 
            "Insurance Penetration & Growth Potential",
            "User Engagement & Growth Strategy",
            "Transaction Analysis Across States & Districts"
        ]
    )
    
    if analysis_type == "Select Analysis":
        st.write("Please select an analysis type from the dropdown above.")
        
    elif analysis_type == "Decoding Transaction Dynamics":
        st.subheader("📊 Transaction Pattern Analysis")
        st.write("Understanding how transactions happen over time and across different categories.")
        
        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chart 1: Transaction Volume Over Time (Stacked Area Plot)
                st.subheader("📈 Chart 1: Transaction Volume Over Time")
                cursor.execute("""
                    SELECT Year, Quarter, Transaction_type, SUM(Transaction_count) as total_transactions
                    FROM agg_transaction 
                    GROUP BY Year, Quarter, Transaction_type 
                    ORDER BY Year, Quarter
                """)
                transaction_data = cursor.fetchall()
                
                if transaction_data:
                    df = pd.DataFrame(transaction_data, columns=['Year', 'Quarter', 'Transaction_Type', 'Transactions'])
                    df['Period'] = df['Year'].astype(str) + ' Q' + df['Quarter'].astype(str)
                    
                    fig = px.area(df, x='Period', y='Transactions', color='Transaction_Type',
                                title="Transaction Volume Over Time (Stacked by Type)")
                    fig.update_layout(xaxis_title="Year-Quarter", yaxis_title="Total Transactions")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No transaction data found for Chart 1.")
                
                # Chart 2: Transaction Type Distribution (Donut Chart)
                st.subheader("🍩 Chart 2: Transaction Type Distribution")
                cursor.execute("""
                    SELECT Transaction_type, SUM(Transaction_count) as total_count
                    FROM agg_transaction 
                    GROUP BY Transaction_type 
                    ORDER BY total_count DESC
                """)
                type_data = cursor.fetchall()
                
                if type_data:
                    df_type = pd.DataFrame(type_data, columns=['Transaction_Type', 'Count'])
                    fig2 = px.pie(df_type, values='Count', names='Transaction_Type', 
                                 title="Transaction Type Distribution",
                                 hole=0.4)  # This creates a donut chart
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("No transaction type data found for Chart 2.")
                
                # Chart 3: Transaction Amount vs Count Scatter Plot
                st.subheader("🔍 Chart 3: Transaction Amount vs Count Relationship")
                cursor.execute("""
                    SELECT Transaction_count, Transaction_amount
                    FROM agg_transaction 
                    WHERE Transaction_amount > 0
                    LIMIT 1000
                """)
                scatter_data = cursor.fetchall()
                
                if scatter_data:
                    df_scatter = pd.DataFrame(scatter_data, columns=['Count', 'Amount'])
                    fig3 = px.scatter(df_scatter, x='Count', y='Amount', 
                                    title="Transaction Count vs Amount Relationship",
                                    opacity=0.6)
                    fig3.update_layout(xaxis_title="Transaction Count", yaxis_title="Transaction Amount (₹)")
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No scatter plot data found for Chart 3.")
                
                # Chart 4: Transaction Amount Distribution (Box Plot)
                st.subheader("📊 Chart 4: Transaction Amount Distribution")
                if scatter_data:
                    fig4 = px.box(df_scatter, y='Amount',
                                 title="Distribution of Transaction Amounts",
                                 points="outliers")
                    fig4.update_layout(yaxis_title="Transaction Amount (₹)")
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("No box plot data found for Chart 4.")
                
                # Chart 5: Top Districts by Transaction Volume (Bar Chart)
                st.subheader("🏆 Chart 5: Top Districts by Transaction Volume")
                cursor.execute("""
                    SELECT District, SUM(Count) as total_transactions
                    FROM map_transaction_hover 
                    GROUP BY District 
                    ORDER BY total_transactions DESC 
                    LIMIT 15
                """)
                district_data = cursor.fetchall()
                
                if district_data:
                    df_district = pd.DataFrame(district_data, columns=['District', 'Transactions'])
                    fig5 = px.bar(df_district, x='District', y='Transactions',
                                 title="Top Districts by Transaction Volume")
                    fig5.update_layout(xaxis_title="District", yaxis_title="Total Transactions")
                    fig5.update_xaxes(tickangle=45)
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("No district data found for Chart 5.")
                
                conn.close()
                    
            except Exception as e:
                st.error(f"Error fetching transaction data: {e}")
                if conn:
                    conn.close()
        else:
            st.error("Unable to connect to database.")
        
    elif analysis_type == "Device Dominance & User Engagement":
        st.subheader("📱 Device and User Analysis")
        st.write("Understanding which devices users prefer and how they engage with the app.")
        
        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chart 1: Device Brand Popularity (Bar Chart)
                st.subheader("📊 Chart 1: Device Brand Distribution")
                cursor.execute("""
                    SELECT Brand, SUM(User_Count) as total_users
                    FROM agg_user_device 
                    GROUP BY Brand 
                    ORDER BY total_users DESC 
                    LIMIT 10
                """)
                device_data = cursor.fetchall()
                
                if device_data:
                    df = pd.DataFrame(device_data, columns=['Brand', 'Users'])
                    fig = px.bar(df, x='Brand', y='Users',
                                title="Device Brand Distribution")
                    fig.update_layout(xaxis_title="Brand", yaxis_title="Total Users")
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No device data found for Chart 1.")
                
                # Chart 2: User Engagement by State (Box Plot)
                st.subheader("📊 Chart 2: User Engagement Distribution by State")
                cursor.execute("""
                    SELECT State, 
                           SUM(Registered_Users) as total_users,
                           SUM(App_Opens) as total_app_opens
                    FROM agg_user 
                    GROUP BY State 
                    HAVING SUM(Registered_Users) > 0
                    ORDER BY total_users DESC 
                    LIMIT 15
                """)
                user_data = cursor.fetchall()
                
                if user_data:
                    df_user = pd.DataFrame(user_data, columns=['State', 'Users', 'App_Opens'])
                    # Create box plot for both Users and App Opens
                    fig2 = px.box(df_user, y=['Users', 'App_Opens'],
                                 title="Distribution of Users and App Opens Across States",
                                 points="outliers")
                    fig2.update_layout(yaxis_title="Count")
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("No user data found for Chart 2.")
                
                # Chart 3: App Opens vs Registered Users Scatter Plot
                st.subheader("🔍 Chart 3: App Opens vs Registered Users Relationship")
                if user_data:
                    df_user = pd.DataFrame(user_data, columns=['State', 'Users', 'App_Opens'])
                    fig3 = px.scatter(df_user, x='Users', y='App_Opens', 
                                    text='State',
                                    title="App Opens vs Registered Users by State")
                    fig3.update_layout(xaxis_title="Registered Users", yaxis_title="App Opens")
                    fig3.update_traces(textposition="top center")
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No scatter plot data found for Chart 3.")
                
                # Chart 4: Top Districts by User Count (Bar Chart)
                st.subheader("🏆 Chart 4: Top Districts by User Count")
                cursor.execute("""
                    SELECT District, SUM(Registered_Users) as total_users
                    FROM map_user_hover 
                    GROUP BY District 
                    ORDER BY total_users DESC 
                    LIMIT 15
                """)
                district_user_data = cursor.fetchall()
                
                if district_user_data:
                    df_district_user = pd.DataFrame(district_user_data, columns=['District', 'Users'])
                    fig4 = px.bar(df_district_user, x='District', y='Users',
                                 title="Top Districts by Registered Users")
                    fig4.update_layout(xaxis_title="District", yaxis_title="Total Users")
                    fig4.update_xaxes(tickangle=45)
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("No district user data found for Chart 4.")
                
                # Chart 5: User Growth Over Time (Stacked Area Plot)
                st.subheader("📈 Chart 5: User Growth Over Time")
                cursor.execute("""
                    SELECT Year, Quarter, State, SUM(Registered_Users) as total_users
                    FROM agg_user 
                    GROUP BY Year, Quarter, State 
                    ORDER BY Year, Quarter
                """)
                growth_data = cursor.fetchall()
                
                if growth_data:
                    df_growth = pd.DataFrame(growth_data, columns=['Year', 'Quarter', 'State', 'Users'])
                    df_growth['Period'] = df_growth['Year'].astype(str) + ' Q' + df_growth['Quarter'].astype(str)
                    
                    fig5 = px.area(df_growth, x='Period', y='Users', color='State',
                                  title="User Growth Over Time (Stacked by State)")
                    fig5.update_layout(xaxis_title="Year-Quarter", yaxis_title="Total Registered Users")
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("No growth data found for Chart 5.")
                
                conn.close()
                    
            except Exception as e:
                st.error(f"Error fetching device and user data: {e}")
                if conn:
                    conn.close()
        else:
            st.error("Unable to connect to database.")
        
    elif analysis_type == "Insurance Penetration & Growth Potential":
        st.subheader("🛡️ Insurance Market Analysis")
        st.write("Understanding insurance adoption rates and finding growth opportunities.")
        
        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chart 1: Insurance Adoption by State (Bar Chart)
                st.subheader("📊 Chart 1: Insurance Adoption by State")
                cursor.execute("""
                    SELECT State, SUM(Insurance_count) as total_insurance
                    FROM agg_insurance 
                    GROUP BY State 
                    ORDER BY total_insurance DESC 
                    LIMIT 15
                """)
                insurance_data = cursor.fetchall()
                
                if insurance_data:
                    df = pd.DataFrame(insurance_data, columns=['State', 'Insurance_Count'])
                    fig = px.bar(df, x='State', y='Insurance_Count', 
                                title="Insurance Adoption by State")
                    fig.update_layout(xaxis_title="State", yaxis_title="Total Insurance Count")
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No insurance data found for Chart 1.")
                
                # Chart 2: Insurance Type Distribution (Donut Chart)
                st.subheader("🍩 Chart 2: Insurance Type Distribution")
                cursor.execute("""
                    SELECT Insurance_type, SUM(Insurance_count) as total_count
                    FROM agg_insurance 
                    GROUP BY Insurance_type 
                    ORDER BY total_count DESC
                """)
                type_data = cursor.fetchall()
                
                if type_data:
                    df_type = pd.DataFrame(type_data, columns=['Insurance_Type', 'Count'])
                    fig2 = px.pie(df_type, values='Count', names='Insurance_Type', 
                                 title="Insurance Type Distribution",
                                 hole=0.4)  # This creates a donut chart
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("No insurance type data found for Chart 2.")
                
                # Chart 3: Insurance Amount vs Count Scatter Plot
                st.subheader("🔍 Chart 3: Insurance Amount vs Count Relationship")
                cursor.execute("""
                    SELECT Insurance_count, Insurance_amount
                    FROM agg_insurance 
                    WHERE Insurance_amount > 0
                    LIMIT 1000
                """)
                scatter_data = cursor.fetchall()
                
                if scatter_data:
                    df_scatter = pd.DataFrame(scatter_data, columns=['Count', 'Amount'])
                    fig3 = px.scatter(df_scatter, x='Count', y='Amount', 
                                    title="Insurance Count vs Amount Relationship",
                                    opacity=0.6)
                    fig3.update_layout(xaxis_title="Insurance Count", yaxis_title="Insurance Amount (₹)")
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No scatter plot data found for Chart 3.")
                

                
                # Chart 4: Top Districts by Insurance Count (Bar Chart)
                st.subheader("🏆 Chart 4: Top Districts by Insurance Count")
                cursor.execute("""
                    SELECT District, SUM(Count) as total_insurance
                    FROM map_insurance_hover 
                    GROUP BY District 
                    ORDER BY total_insurance DESC 
                    LIMIT 15
                """)
                district_data = cursor.fetchall()
                
                if district_data:
                    df_district = pd.DataFrame(district_data, columns=['District', 'Insurance_Count'])
                    fig4 = px.bar(df_district, x='District', y='Insurance_Count',
                                 title="Top Districts by Insurance Count")
                    fig4.update_layout(xaxis_title="District", yaxis_title="Total Insurance Count")
                    fig4.update_xaxes(tickangle=45)
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("No district insurance data found for Chart 4.")
                
                # Chart 5: Insurance Growth Over Time (Stacked Area Plot)
                st.subheader("📈 Chart 5: Insurance Growth Over Time")
                cursor.execute("""
                    SELECT Year, Quarter, State, SUM(Insurance_count) as total_insurance
                    FROM agg_insurance 
                    GROUP BY Year, Quarter, State 
                    ORDER BY Year, Quarter
                """)
                growth_data = cursor.fetchall()
                
                if growth_data:
                    df_growth = pd.DataFrame(growth_data, columns=['Year', 'Quarter', 'State', 'Insurance_Count'])
                    df_growth['Period'] = df_growth['Year'].astype(str) + ' Q' + df_growth['Quarter'].astype(str)
                    
                    fig5 = px.area(df_growth, x='Period', y='Insurance_Count', color='State',
                                  title="Insurance Growth Over Time (Stacked by State)")
                    fig5.update_layout(xaxis_title="Year-Quarter", yaxis_title="Total Insurance Count")
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("No growth data found for Chart 5.")
                
                conn.close()
                    
            except Exception as e:
                st.error(f"Error fetching insurance data: {e}")
                if conn:
                    conn.close()
        else:
            st.error("Unable to connect to database.")
        
    elif analysis_type == "User Engagement & Growth Strategy":
        st.subheader("👥 User Behavior Analysis")
        st.write("Understanding how users interact with the app and planning growth strategies.")
        
        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chart 1: User Engagement Ratio by State (Histogram)
                st.subheader("📊 Chart 1: User Engagement Ratio Distribution")
                cursor.execute("""
                    SELECT State, 
                           SUM(Registered_Users) as total_users,
                           SUM(App_Opens) as total_app_opens,
                           ROUND(AVG(App_Opens::numeric / NULLIF(Registered_Users, 0)), 2) as engagement_ratio
                    FROM agg_user 
                    GROUP BY State 
                    HAVING SUM(Registered_Users) > 0
                    ORDER BY engagement_ratio DESC 
                    LIMIT 15
                """)
                user_data = cursor.fetchall()
                
                if user_data:
                    df = pd.DataFrame(user_data, columns=['State', 'Registered_Users', 'App_Opens', 'Engagement_Ratio'])
                    fig = px.histogram(df, x='Engagement_Ratio', nbins=15,
                                      title="Distribution of User Engagement Ratios Across States",
                                      opacity=0.7)
                    fig.update_layout(xaxis_title="Engagement Ratio", yaxis_title="Number of States")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No user engagement data found for Chart 1.")
                
                # Chart 2: App Opens Distribution (Box Plot)
                st.subheader("📊 Chart 2: App Opens Distribution")
                if user_data:
                    df = pd.DataFrame(user_data, columns=['State', 'Registered_Users', 'App_Opens', 'Engagement_Ratio'])
                    fig2 = px.box(df, y='App_Opens',
                                 title="Distribution of App Opens Across States",
                                 points="outliers")
                    fig2.update_layout(yaxis_title="App Opens")
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("No box plot data found for Chart 2.")
                
                # Chart 3: Top User Districts (Bar Chart)
                st.subheader("🏆 Chart 3: Top Districts by User Count")
                cursor.execute("""
                    SELECT District, SUM(Registered_Users) as total_users
                    FROM top_user_district 
                    GROUP BY District 
                    ORDER BY total_users DESC 
                    LIMIT 15
                """)
                district_data = cursor.fetchall()
                
                if district_data:
                    df_district = pd.DataFrame(district_data, columns=['District', 'Users'])
                    fig3 = px.bar(df_district, x='District', y='Users',
                                 title="Top Districts by Registered Users")
                    fig3.update_layout(xaxis_title="District", yaxis_title="Total Users")
                    fig3.update_xaxes(tickangle=45)
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No district user data found for Chart 3.")
                
                # Chart 4: User Growth vs App Engagement Colored Scatter Plot
                st.subheader("🔍 Chart 4: User Growth vs App Engagement")
                if user_data:
                    df = pd.DataFrame(user_data, columns=['State', 'Registered_Users', 'App_Opens', 'Engagement_Ratio'])
                    df['Engagement_Ratio'] = pd.to_numeric(df['Engagement_Ratio'], errors='coerce')
                    fig4 = px.scatter(df, x='Registered_Users', y='App_Opens', 
                                    text='State',
                                    title="Registered Users vs App Opens by State",
                                    size='Engagement_Ratio',
                                    color='Engagement_Ratio')
                    fig4.update_layout(xaxis_title="Registered Users", yaxis_title="App Opens")
                    fig4.update_traces(textposition="top center")
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("No scatter plot data found for Chart 4.")
                
                # Chart 5: Top Pincodes by User Count (Bar Chart)
                st.subheader("📍 Chart 5: Top Pincodes by User Count")
                cursor.execute("""
                    SELECT Pincode, SUM(Registered_Users) as total_users
                    FROM top_user_pincode 
                    GROUP BY Pincode 
                    ORDER BY total_users DESC 
                    LIMIT 15
                """)
                pincode_data = cursor.fetchall()
                
                if pincode_data:
                    df_pincode = pd.DataFrame(pincode_data, columns=['Pincode', 'Users'])
                    fig5 = px.bar(df_pincode, x='Pincode', y='Users',
                                 title="Top Pincodes by Registered Users")
                    fig5.update_layout(xaxis_title="Pincode", yaxis_title="Total Users")
                    fig5.update_xaxes(tickangle=45)
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("No pincode user data found for Chart 5.")
                

                
                conn.close()
                    
            except Exception as e:
                st.error(f"Error fetching user data: {e}")
                if conn:
                    conn.close()
        else:
            st.error("Unable to connect to database.")
        
    elif analysis_type == "Transaction Analysis Across States & Districts":
        st.subheader("🗺️ Geographic Transaction Analysis")
        st.write("Understanding transaction patterns across different regions of India.")

        conn = connect_to_database()
        if conn:
            try:
                cursor = conn.cursor()
                
                # Chart 1: Transaction Volume by State (Bar Chart)
                st.subheader("📊 Chart 1: Transaction Volume by State")
                cursor.execute("""
                    SELECT State, 
                           SUM(Transaction_count) as total_transactions,
                           SUM(Transaction_amount) as total_amount
                    FROM agg_transaction 
                    GROUP BY State 
                    ORDER BY total_transactions DESC 
                    LIMIT 15
                """)
                transaction_data = cursor.fetchall()
                
                if transaction_data:
                    df = pd.DataFrame(transaction_data, columns=['State', 'Transaction_Count', 'Transaction_Amount'])
                    fig = px.bar(df, x='State', y='Transaction_Count', 
                                title="Transaction Volume by State")
                    fig.update_layout(xaxis_title="State", yaxis_title="Total Transaction Count")
                    fig.update_xaxes(tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No transaction data found for Chart 1.")
                     
                # Chart 2: Transaction Amount by State (Box Plot)
                st.subheader("💰 Chart 2: Transaction Amount Distribution")
                if transaction_data:
                    fig2 = px.box(df, y='Transaction_Amount',
                                 title="Distribution of Transaction Amounts Across States",
                                 points="outliers")
                    fig2.update_layout(yaxis_title="Transaction Amount (₹)")
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("No transaction amount data found for Chart 2.")
                
                # Chart 3: Top Transaction Districts (Bar Chart)
                st.subheader("🏆 Chart 3: Top Districts by Transaction Count")
                cursor.execute("""
                    SELECT District, SUM(Count) as total_transactions
                    FROM map_transaction_hover 
                    GROUP BY District 
                    ORDER BY total_transactions DESC 
                    LIMIT 15
                """)
                district_data = cursor.fetchall()
                
                if district_data:
                    df_district = pd.DataFrame(district_data, columns=['District', 'Transactions'])
                    fig3 = px.bar(df_district, x='District', y='Transactions',
                                 title="Top Districts by Transaction Count")
                    fig3.update_layout(xaxis_title="District", yaxis_title="Total Transactions")
                    fig3.update_xaxes(tickangle=45)
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("No district transaction data found for Chart 3.")
                
                # Chart 4: Transaction Amount Distribution (Histogram)
                st.subheader("📊 Chart 4: Transaction Amount Distribution")
                if transaction_data:
                    df = pd.DataFrame(transaction_data, columns=['State', 'Transaction_Count', 'Transaction_Amount'])
                    fig4 = px.histogram(df, x='Transaction_Amount', nbins=20,
                                       title="Distribution of Transaction Amounts Across States",
                                       opacity=0.7)
                    fig4.update_layout(xaxis_title="Transaction Amount (₹)", yaxis_title="Number of States")
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("No histogram data found for Chart 4.")
                
                # Chart 5: Top Transaction Pincodes (Pie Chart)
                st.subheader("🥧 Chart 5: Top Pincodes by Transaction Count")
                cursor.execute("""
                    SELECT Pincode, SUM(Pincode_Count) as total_transactions
                    FROM top_transaction_pincode 
                    GROUP BY Pincode 
                    ORDER BY total_transactions DESC 
                    LIMIT 10
                """)
                pincode_data = cursor.fetchall()
                
                if pincode_data:
                    df_pincode = pd.DataFrame(pincode_data, columns=['Pincode', 'Transactions'])
                    fig5 = px.pie(df_pincode, values='Transactions', names='Pincode',
                                 title="Top Pincodes by Transaction Count")
                    st.plotly_chart(fig5, use_container_width=True)
                else:
                    st.warning("No pincode transaction data found for Chart 5.")
                
                conn.close()
                    
            except Exception as e:
                st.error(f"Error fetching transaction data: {e}")
                if conn:
                    conn.close()
        else:
            st.error("Unable to connect to database.")

# Footer
st.markdown("""
<style>
.footer {
    position: fixed;
    bottom: 0;
    left: 0;
    width: 100%;
    background-color: #f0f2f6;
    border-top: 1px solid #e0e0e0;
    padding: 10px 0;
    text-align: center;
    z-index: 1000;
    box-shadow: 0 -2px 10px rgba(0,0,0,0.1);
}

.footer-content {
    max-width: 1200px;
    margin: 0 auto;
    padding: 0 20px;
}

.footer-text {
    color: #666;
    font-size: 14px;
    margin: 0;
}
</style>
""", unsafe_allow_html=True)

st.markdown("<div style='margin-bottom: 80px;'></div>", unsafe_allow_html=True)

# Footer
st.markdown("""
<div class="footer">
    <div class="footer-content">
        <p class="footer-text">📱 PhonePe Dashboard - Made with Streamlit</p>
    </div>
</div>  
""", unsafe_allow_html=True)
