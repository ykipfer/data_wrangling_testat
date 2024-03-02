import logging
from utils.utils import (read_config,
                         setup_logging,
                         get_file_list,
                         create_output_directories,
                         encrypt_col,
                         harmonise_with_threshold,
                         merge_to_date_time_col,
                         remove_columns_with_missing,
                         remove_rows_with_duplicate_col_conditionally,
                         remove_rows_with_missing,
                         read_csv_to_df,
                         create_profiling_report)
from cryptography.fernet import Fernet


class Pipeline:
    def __init__(self, config_path="config.json"):
        self.path_to_config = config_path
        self.config = None
        self.input_file_list = None
        self.df = None

    def setup(self):
        # set up logging
        setup_logging()

        # create output directories
        create_output_directories()

        # read config
        self.config = read_config(self.path_to_config)

        # get input file list
        self.input_file_list = get_file_list(self.config['inputdir'])

    def load_data(self, file):
        """
        Load CSV, or other data file, from the specified path and store it in the dataframe.
        """
        logging.info(f"Loading data from {file}")
        if file.endswith(".csv"):
            self.df = read_csv_to_df(f'{self.config["inputdir"]}{file}')

    def handling_accuracy(self):
        # handle data type and formats
        self.df['issue_month'] = self.df['issue_month'].replace('Octxyz', 'Oct')
        # merge year and month columns to datetime column
        merge_to_date_time_col(df=self.df, year_col='issue_year', month_col='issue_month', datetime_col='issue_date')

    def handling_completeness(self):
        # Remove all columns that have only null values or have null values above a certain threshold
        cols_before = self.df.shape[1]
        self.df, cols_to_remove = remove_columns_with_missing(self.df, self.config['threshold_missing'])
        logging.info(
            f"Removed {cols_before - self.df.shape[1]} columns with > {int(self.config['threshold_missing'] * 100)}% missing observations: {cols_to_remove}")

        rows_before = self.df.shape[0]
        self.df, rows_to_remove = remove_rows_with_missing(self.df, self.config['threshold_missing'])
        logging.info(
            f"Removed {rows_before - self.df.shape[0]} rows with > {int(self.config['threshold_missing'] * 100)}% missing observations: {rows_to_remove}")

        # impute missing values
        self.df['mths_since_last_delinq'] = self.df['mths_since_last_delinq'].fillna(0)
        self.df['tot_cur_bal'] = self.df['tot_cur_bal'].fillna(0)
        self.df['tot_coll_amt'] = self.df['tot_coll_amt'].fillna(0)

    def handling_integrity(self):

        # handle duplicates
        # drop first column, which contains a row index (prevents detection of duplicate rows)
        self.df = self.df.drop(columns='Unnamed: 0')
        rows_before = self.df.shape[0]

        # drop rows where all values are duplicates
        self.df = self.df.drop_duplicates()
        logging.info(f"Removed {rows_before - self.df.shape[0]} rows with duplicates")

        # remove rows with montly installment bigger than 10'000
        condition = self.df['installment'] > 10000
        rows_before = self.df.shape[0]
        self.df = remove_rows_with_duplicate_col_conditionally(df=self.df, col='member_id', condition=condition)
        logging.info(f"Removed {rows_before - self.df.shape[0]} rows which had the same " +
                     "member_id as another entry and the monthly installment was bigger than 10'000")

        # handle inconsistent data
        # check that subgrade is a subset of grade
        self.df['sub_grade'] = self.df.apply(
            lambda row: row['sub_grade'] if row['sub_grade'] is not None and row['sub_grade'][0] in row[
                'grade'] else None, axis=1)
        logging.info("Removed subgrade entries that are not a subset of grade entries")

        # Check if subgrade matches the required format (e.g. A1, B2, C3, etc.)
        self.df['sub_grade'] = self.df['sub_grade'].apply(
            lambda x: x if x is not None and x[0] in ['A', 'B', 'C', 'D', 'E', 'F', 'G'] and x[1].isdigit() else None)
        logging.info("Removed subgrade entries that do not match the required format (e.g. A1, B2, C3, etc.)")

        # replace negative interest rate values with NaN
        self.df['int_rate'] = self.df['int_rate'].apply(lambda x: x if x > 0 else None)
        logging.info("Replaced negative interest rate values with NaN")

    def handling_text(self):
        # Code for handling text
        # harmonisation,business rules, deduplication
        # convert to lower case and remove leading and trailing white spaces
        self.df['emp_title'] = self.df['emp_title'].str.lower().str.strip()

        # replace rn with registered nurse
        self.df['emp_title'] = self.df['emp_title'].replace('rn', 'registered nurse')

        # harmonise job titles
        for cluster_string in self.config['cluster_list']:
            harmonise_with_threshold(self.df, 'emp_title', cluster_string, self.config['similarity_score_threshold'])

    def data_protection(self):
        # Code for data protection

        # get private key
        private_key = self.config["encryption_key"]

        # create fernet object
        fernet = Fernet(private_key)

        # encrypt url column
        encrypt_col(df=self.df, col='url', fernet=fernet)

    def save_data(self, file):
        self.df.to_csv(f"{self.config['outputdir']}clean_loan_data.csv", index=False)

    def profile_data(self):
        create_profiling_report(self.df, self.config['outputdir'])

    def run_pipeline(self):
        self.setup()
        for file in self.input_file_list:
            self.load_data(file)
            self.handling_text()
            self.handling_accuracy()
            self.handling_integrity()
            self.handling_completeness()
            self.data_protection()
            self.profile_data()
            self.save_data(file)
