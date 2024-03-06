
# Data Wrangling - Profiling, Pipeline & Visualisation

## Description
This repository contains the pipeline for our data wrangling project. The project aims to demonstrate data engineering techniques, including data profiling, data cleaning, transformation, and data visualisations using Python.

## Installation

### Prerequisites
- Python 3.11 or later
- pip

### Steps
1. Clone the repository:
```bash
git clone git@github.com:ykipfer/data_wrangling_testat.git
cd data_wrangling_testat
```

2. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage
To run the data pipeline, navigate to the project's root directory and execute. If necessary you can make adjustments to the config file `config.json`:
```bash
python wrangling.py
```

## Output
The pipeline will generate the following files. Note, the `output` and `log` directories, as well as any subdirectories, will be automatically generated upon execution:
- `output/processed_data/clean_loan_data.csv`: The cleaned and transformed data
- `output/profile/cleaned_profiling_report.html`: The data profile report
- `logs/`: Log files for the pipeline 

# Authors
* Stefan Mettler
* Yanik Kipfer
