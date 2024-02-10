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
