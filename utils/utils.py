import numpy as np
import pandas as pd
import json
import datetime
from thefuzz import process
import os
import logging
from ydata_profiling import ProfileReport


def setup_logging():
    """
    Set up logging to file

    Returns:
        None
    """
    logging.basicConfig(level=logging.INFO,
                        filename=f"logs/{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.log",
                        filemode="w",
                        format="%(asctime)s: %(levelname)s - %(message)s")


def get_file_list(directory):
    """
    Returns a list of all files in the specified directory.
    """

    file_list = [file for file in os.listdir(directory) if file.endswith(".DS_Store") is False]
    logging.info(f"Getting {len(file_list)} files from {directory}")
    return file_list


def read_config(config_path):
    """
    Reads and loads the configuration from the specified JSON file.

    Parameters:
        config_path (str): The path to the JSON configuration file.

    Returns:
        dict: The loaded configuration.
    """
    logging.info(f"Reading config from {config_path}")
    with open(config_path) as f:
        config = json.load(f)
    return config


def read_csv_to_df(file_path):
    """
    Reads a CSV file and returns a pandas DataFrame.

    Parameters:
    -----------
    file_path : str
        The path to the CSV file.

    Returns:
    --------
    pandas.DataFrame
        The DataFrame containing the CSV data.
    """
    df = pd.read_csv(file_path, low_memory=False)
    return df


def find_missing_cols(df, threshold=1):
    """ Find Columns with all (or threshold) missing
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for missing values.
        Returns:
        --------
        List of Columns name
    """
    return [col for col in df.columns if df[col].isnull().sum() / df.shape[0] > threshold]


def remove_columns_with_missing(df, threshold=1):
    """ Remove Columns with all (or threshold) missing
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for missing values.
        Returns:
        --------
        pandas.DataFrame
    """
    cols_to_remove = find_missing_cols(df, threshold)
    return df.drop(columns=cols_to_remove), cols_to_remove


# remove rows with more than 95% missing values
def find_rows_with_missing(df, threshold=1):
    """ Find Rows with all (or threshold) missing
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for missing values.
        Returns:
        --------
        List of Rows index
    """
    return df.index[df.isnull().mean(axis=1) > threshold].tolist()


def remove_rows_with_missing(df, threshold=1):
    """ Remove Rows with all (or threshold) missing
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for missing values.
        Returns:
        --------
        pandas.DataFrame
    """
    rows_to_remove = find_rows_with_missing(df, threshold)
    return df.drop(index=rows_to_remove), rows_to_remove


def remove_rows_with_duplicate_col_conditionally(df, col, condition):
    """ Remove Rows where the column col id duplicate and the condition applies
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for duplicate values.
        col: string 
        the col to check for duplicate values
        condition: _bool
        the condition the duplicate rows have to fulfill to be removed
        Returns:
        --------
        pandas.DataFrame
    """
    # filter for all entries containing duplicated col order by col
    duplicated_cols = df[df.duplicated(subset=col, keep=False)][col].unique().tolist()
    return df[~df[col].isin(duplicated_cols) | df[col].isin(duplicated_cols) & ~condition]


# currently does not work as intendend, still working on it
def replace_non_dependend_cols(df, col, subcol, predicate):
    """ Replaces entries where the values of two cols that should be dependend on one another, are not dependend
        Parameters:
        -----------
        df : pandas.DataFrame
        The input DataFrame to analyze for duplicate values.
        col: string 
        the first of the two cols to compare
        subcol: string 
        the second of the two cols to compare
        predicat: function
        the predicate to compare the two cols with 
        Returns:
        --------
        pandas.DataFrame
    """
    df_without_independend_entries = df.copy()
    _pred = df_without_independend_entries[subcol].str.match(df_without_independend_entries[col] + '\d{1}')
    df_without_independend_entries[subcol] = np.where(_pred, None, df_without_independend_entries[subcol])
    return df_without_independend_entries


def has_string_type(s: pd.Series) -> bool:
    """ Returns true if column is of type string
        Parameters:
        -----------
        s: pd.Series
        The series to be investigated
    """
    if pd.StringDtype.is_dtype(s.dtype):
        # StringDtype extension type
        return True

    if s.dtype != "object":
        # No object column - definitely no string
        return False

    try:
        s.str
    except AttributeError:
        return False

    # The str accessor exists, this must be a String column
    return True


def merge_to_date_time_col(df, year_col, month_col, datetime_col='merged_datetime'):
    """ Takes two colums, one containing a year and one containing a month and 
        create a new column combining the values from the year and month column to a datetime and dropping the old columns
        Parameters:
        -----------
        df: pandas.DataFrame
        the frame on which we operate
        year_col: string
        name of the col that contains the year value
        month_col: string
        name of the col that contains the month value
        Returns:
        --------
        pandas.DataFrame
    """
    # We drop all rows where the year or month column is empty
    df = df.dropna(subset=[month_col])
    df = df.dropna(subset=[year_col])
    # convert year to integer
    df[year_col] = df[year_col].astype(int)
    # create new variable datetime_col with data type datetime
    df[datetime_col] = pd.to_datetime(df[year_col].astype(str) + '-' + df[month_col], format='%Y-%b')
    # drop year and month columns
    df = df.drop(columns=[year_col, month_col])
    return df


def encrypt_col(df, col, fernet=None):
    """
    Encrypts the specified column in the DataFrame.
    """
    # Ensure that 'url' column is of type string if not convert it
    if isinstance(df[col], str) is False:
        df[col] = df[col].astype(str)
    return df[col].apply(lambda x: fernet.encrypt(x.encode()))


def decrypt_col(df, col, fernet):
    """
    Decrypts the specified column in the DataFrame.
    """
    # decrypt url
    return df[col].apply(lambda x: fernet.decrypt(x).decode())


def harmonise_with_threshold(df, col, cluster_string, threshold):
    """
    Harmonises the specified column in the DataFrame with the given cluster string and threshold.
    """
    for val, similarity in process.extract(cluster_string, df[col].unique(), limit=len(df[col].unique())):
        if similarity >= threshold:
            df.loc[df[col] == val, col] = cluster_string


def create_profiling_report(df, output_dir):
    """
    Create a profiling report for the given DataFrame and save it to the specified output directory.
    """
    logging.info(f"Creating profiling report for the DataFrame")
    profile = ProfileReport(df, title='Profiling Report: Wrangled Loan Data', minimal=True)
    profile.to_file(output_file=f"{output_dir}wrangled_profiling_report.html")


def create_output_directories():
    """
    Creates the specified directories if they do not exist.
    """
    directories = ["output/", "output/processed_data", "output/profile", "logs/"]
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)