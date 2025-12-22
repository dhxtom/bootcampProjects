# CSV Profiler

CSV Profiler is a Python project that analyzes (profiles) CSV files and extracts useful insights, generating reports in two formats:
- **JSON** 
- **Markdown** 

The project provides two ways to use the tool:
1. **Command Line Interface (CLI)**
2. **Web Interface using Streamlit**


##  Features

- Read any CSV file
- Count rows and columns
- Detect missing values
- Infer column types:
  - Numeric (number)
  - Text (text)
- Compute basic statistics for numeric columns:
  - min
  - max
  - mean
- Generate reports:
  - `report.json` 
  - `report.md` 
- Simple web interface to upload files and download reports

## Run the Project – Web App (Streamlit):

 PYTHONPATH=src uv run streamlit run app.py



