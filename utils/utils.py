import numpy as np
import pandas as pd
from cryptography.fernet import Fernet

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
    
#currently does not work as intendend, still working on it
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
    _pred = df_without_independend_entries[subcol].str.match(df_without_independend_entries[col]+'\d{1}')
    df_without_independend_entries[subcol] = np.where(_pred, None,  df_without_independend_entries[subcol])
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
    # sanity check, that we are working with string cols
    # TODO: make day column optional
    if(not has_string_type(df[year_col]) or not has_string_type(month_col)):
        return
    # create new variable datetime_col with data type datetime
    df_with_new_col = df.copy()
    df_with_new_col[datetime_col] = pd.to_datetime(df[year_col].astype(str) + '-' + df[month_col], format='%Y-%b')
    # drop no longer needed cols
    return df_with_new_col.drop(columns=[year_col, month_col])

def encrypt_col(df, col, fernet=None):
    """
    """
    encrypt = hash
    if fernet is not None: 
        encrypt = fernet.encrypt

    encrypted_df = df.copy()
    # Ensure that 'url' column is of type string if not convert it
    if isinstance(encrypted_df[col], str) is False:
        encrypted_df[col] = encrypted_df[col].astype(str)
    return encrypted_df[col].apply(lambda x: encrypt(x.encode()))

def decrypt_col(df, col, fernet):
    """
    """

    # encrypt url
    df[col] = df[col].apply(lambda x: fernet.decrypt(x).decode())
    return 