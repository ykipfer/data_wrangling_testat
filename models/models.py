import os
import logging
import pandas as pd
import json
from utils.utils import merge_to_date_time_col, remove_columns_with_missing, remove_rows_with_duplicate_col_conditionally


class Pipeline:
    def __init__(self, config_path="config.json"):
        self.path_to_config = config_path
        self.config = None
        self.input_file_list = None
        self.df = None

    def read_config(self):
        """
        Reads and loads the configuration from the specified JSON file.

        Parameters:
            config_json (str): The path to the JSON configuration file.

        Returns:
            None
        """
        logging.info(f"Reading config from {self.path_to_config}")
        with open(self.path_to_config) as f:
            self.config = json.load(f)

    def get_file_list(self):
        """
        Returns a list of all files in the specified directory.
        """
        self.input_file_list = os.listdir(self.config['inputdir'])
        logging.info(f"Getting {len(self.input_file_list)} files from {self.config['inputdir']}")


    def load_data(self, file):
        """
        Load CSV, or other data file, from the specified path and store it in the dataframe.
        """
        logging.info(f"Loading data from {file}")
        self.df = pd.read_csv(f"{self.config['inputdir']}{file}", low_memory=False)

    def handling_accuracy(self):
        # handle data type and formats
        self.df['issue_month'] = self.df['issue_month'].replace('Octxyz', 'Oct')
        merge_to_date_time_col(df=self.df, year_col='issue_year', month_col='issue_month', datetime_col='issue_date')
        pass

    def handling_completeness(self):
        # Remove all columns that have only null values or have null values above a certain threshold
        cols_before = self.df.shape[1]
        self.df, cols_to_remove = remove_columns_with_missing(self.df, self.config['threshold_missing'])
        logging.info(
            f"Removed {cols_before - self.df.shape[1]} columns with > {int(self.config['threshold_missing'] * 100)}% missing observations: {cols_to_remove}")

    def handling_integrity(self):
        # handle duplicates
        # drop first column, which contains a row index
        # (prevents detection of duplicate rows)
        self.df = self.df.drop(columns='Unnamed: 0')
        rows_before = self.df.shape[0]
        #drop rows where all values are duplicates
        self.df = self.df.drop_duplicates()
        logging.info(f"Removed {rows_before - self.df.shape[0]} rows with duplicates")
        condition = self.df['installment'] > 10000
        rows_before = self.df.shape[0]
        self.df = remove_rows_with_duplicate_col_conditionally(df=self.df, col='member_id', condition=condition)
        logging.info(f"Removed {rows_before - self.df.shape[0]} rows which had the same " + 
                     "member_id as another entry and the monthly installment was bigger than 10'000")


        # handle inconsistent data
        regex = r'^.{0,1}$|^.{3,}$'
        self.df['sub_grade'] = self.df['sub_grade'].replace(regex, None, regex=True)
        #self.df['sub_grade'] = self.df['sub_grade'].replace('ALPHA_CENTAURI', None)
        logging.info("Removed nonsensical values from the subgrade column")
        pass

    def handling_text(self):
        # Code for handling text
        # harmonisation,business rules, deduplication
        pass

    def data_protection(self):
        # Code for data protection
        # anonymisation,data retention
        pass

    def save_data(self, file):
        self.df.to_csv(f"{self.config['outputdir']}preprocessed_{file}", index=False)

    def run_pipeline(self):
        logging.info("Starting wrangling process")
        self.read_config()
        self.get_file_list()
        for file in self.input_file_list:
            self.load_data(file)
            # self.handling_text()
            self.handling_accuracy()
            self.handling_integrity()
            self.handling_completeness()
            # self.data_protection()
            self.save_data(file)
