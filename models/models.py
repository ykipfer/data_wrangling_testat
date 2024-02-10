from utils.utils import remove_columns_with_missing
import logging
import pandas as pd

logging.basicConfig(level=logging.DEBUG,
                    filename=f"logs/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log",
                    filemode="w",
                    format="%(asctime)s: %(levelname)s - %(message)s")


class Pipeline:
    def __init__(self, path_to_csv: str):
        self.path_to_csv = path_to_csv
        self.df = None

    def load_data(self):
        """
        Load CSV from the specified path and store it in the dataframe.
        """
        logging.info(f"Loading data from {self.path_to_csv}")
        self.df = pd.read_csv(self.path_to_csv, low_memory=False)

    def handling_accuracy(self):
        # handle data type and formats
        pass

    def handling_completeness(self, threshold=0.9):
        # Remove all columns that have only null values or have null values above a certain threshold
        cols_before = self.df.shape[1]
        self.df, cols_to_remove = remove_columns_with_missing(self.df, threshold)
        logging.info(f"Removed {cols_before - self.df.shape[1]} columns with > {int(threshold * 100)}% missing observations: {cols_to_remove}")


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
        self.load_data()
        #self.handling_text()
        #self.handling_accuracy()
        #self.handling_integrity()
        self.handling_completeness()
        #self.data_protection()
