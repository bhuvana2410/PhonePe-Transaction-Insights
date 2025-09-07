# PhonePe Transaction Insights Dashboard

A comprehensive data visualization and analytics dashboard for PhonePe transaction data built with Streamlit and PostgreSQL.

## Features

### 🏠 Home Page
- **Key Metrics Overview**: Real-time dashboard showing total transactions, users, insurance records, and transaction amounts
- **Interactive India Map**: State-wise choropleth with filters for Data Type (Transactions | Users), Year, and Quarter; Top 5 states panel and summary stats
- **Dashboard Features**: Overview of all available analysis categories
- **Getting Started Guide**: Instructions for using the dashboard

### 📊 Analysis Categories
The dashboard provides 5 main analysis categories:

1. **Decoding Transaction Dynamics**
   - Transaction pattern analysis
   - Trend identification over time
   - Payment method preferences

2. **Device Dominance & User Engagement**
   - Device brand analysis across states
   - User engagement patterns
   - App usage statistics

3. **Insurance Penetration & Growth Potential**
   - Insurance adoption rates by region
   - Growth opportunities identification
   - Market penetration analysis

4. **User Engagement & Growth Strategy**
   - User behavior analysis
   - Strategic insights for growth
   - Engagement metrics

5. **Transaction Analysis Across States & Districts**
   - Regional performance comparison
   - District-level insights
   - Geographic trend analysis

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd PhonePe-Transaction-Insights
   ```

2. **Create and activate virtual environment**
   ```bash
   python -m venv venv
   # On Windows
   venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   Create a `.env` file in the root directory with your PostgreSQL credentials:
   ```
   DB_HOST=your_host
   DB_NAME=your_database_name
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_PORT=5432
   ```

### Prerequisites
- PostgreSQL 13+ running and accessible
- A database matching `DB_NAME` exists
- The `data` directory with JSON files is present.

## Usage

### Running the Streamlit App
```bash
streamlit run app.py
```

The app will open in your default web browser at `http://localhost:8501`

### Home Map Controls
- **Data Type**: `Transactions` or `Users`
- **Year**: Pulled from `agg_transaction` and `agg_user`
- **Quarter**: Contextual to selected year and data type
- Uses an external India states GeoJSON for boundaries

### Data Extraction (Optional)
If you need to extract data from JSON files and populate the database:
```bash
python data_extractor.py
```

What this script does:
- Reads JSON files under `data/aggregated`, `data/map/*/hover`, and `data/top`
- Creates and populates tables: `agg_*`, `map_*_hover`, and `top_*` as listed above
- Can be re-run safely; creates tables if missing and inserts aggregated rows

## Navigation

- **Sidebar**: Contains the main navigation with Home and Analysis sections
- **Home**: Overview dashboard with key metrics and feature descriptions
- **Analysis**: Dropdown menu with 5 analysis categories for detailed insights

## Data Sources

The dashboard connects to PostgreSQL database containing:
- Aggregated transaction data
- User engagement metrics
- Insurance penetration data
- Geographic distribution data
- Top performing districts and pincodes

## Technologies Used

- **Frontend**: Streamlit
- **Visualization**: Plotly
- **Data Processing**: Pandas
- **Database**: PostgreSQL
- **Data Extraction**: Custom Python scripts
- **ORM/DB Driver**: psycopg2
- **Env Management**: python-dotenv

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License.
