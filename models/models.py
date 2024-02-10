import logging
import pandas as pd
import json
from utils.utils import remove_columns_with_missing



class Pipeline:
    def __init__(self, config_path="config.json"):
        self.path_to_config = config_path
        self.config = None
        self.df = None

    def read_config(self):
        """
        Reads and loads the configuration from the specified JSON file.

        Parameters:
            config_json (str): The path to the JSON configuration file.

        Returns:
            None
        """
        with open(self.path_to_config) as f:
            self.config = json.load(f)

    def load_data(self):
        """
        Load CSV from the specified path and store it in the dataframe.
        """
        logging.info(f"Loading data from {self.config['csv_path']}")
        self.df = pd.read_csv(self.config['csv_path'], low_memory=False)

    def handling_accuracy(self):
        # handle data type and formats
        pass

    def handling_completeness(self):
        # Remove all columns that have only null values or have null values above a certain threshold
        cols_before = self.df.shape[1]
        self.df, cols_to_remove = remove_columns_with_missing(self.df, self.config['threshold_missing'])
        logging.info(
            f"Removed {cols_before - self.df.shape[1]} columns with > {int(self.config['threshold_missing'] * 100)}% missing observations: {cols_to_remove}")

    def handling_integrity(self):
        # handle duplicates
        # handle inconsistent data
        pass

    def handling_text(self):
        # Code for handling text
        # harmonisation,business rules, deduplication
        pass

    def data_protection(self):
        # Code for data protection
        # anonymisation,data retention
        pass

    def wrangling(self):
        self.read_config()
        self.load_data()
        # self.handling_text()
        # self.handling_accuracy()
        # self.handling_integrity()
        self.handling_completeness()
        # self.data_protection()
