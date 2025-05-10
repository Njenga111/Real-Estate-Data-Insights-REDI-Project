# **Real Estate Data Insights (REDI)**

### **Project Overview**
Real Estate Data Insights (REDI) is a Python-based ETL pipeline that scrapes real estate data from two major Kenyan property websites: [Property254](http://property254.co.ke) and [BuyRentKenya](http://buyrentkenya.com). It cleans, transforms, and loads the data into a PostgreSQL database for further analysis and visualization through Power BI dashboards.

---

### **Key Features**
- **Data Extraction**: Scrapes property data (e.g., location, price, size) from Property254 and BuyRentKenya.
- **Data Transformation**: Cleans data by handling missing values, formatting, and ensuring consistency.
- **Data Loading**: Loads the cleaned data into a PostgreSQL database for future analysis.

---
### **Project Structure**
/Real Estate Data Insights (REDI)/
├── .env # Store sensitive configuration like DB credentials
├── .gitignore # Excludes files such as virtual environments and logs
├── main.py # Script to run the entire ETL pipeline
├── run_pipeline.py # Entry point to trigger the ETL process
├── etl/ # Contains all ETL-related code (Extract, Transform, Load)
│ ├── extract/ # Web scraping logic
│ ├── transform/ # Data cleaning and transformation logic
│ └── load/ # Logic for loading data into PostgreSQL
├── venv/ # Python virtual environment directory
├── requirements.txt # Lists all the project dependencies
└── logs/ # Stores ETL process logs for error tracking

---
### Contributing
If you’d like to contribute, please follow these steps:

1.Fork the repository
2.Create a feature branch
3.Implement your changes
4.Submit a pull request

---
### License
This project is licensed under the MIT License.

---
### Contact
For any questions or inquiries, please contact: dominicnjenga@example.com

---