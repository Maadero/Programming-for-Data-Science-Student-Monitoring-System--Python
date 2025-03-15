"""
CWPreprocessing Module

Overview:
The CWPreprocessing module is designed for data preprocessing and database management, 
particularly focusing on educational datasets. It provides functionalities to clean, process, 
and standardize data, especially test results, and facilitates the transfer of this processed data 
to a database. The module is built to work with pandas DataFrames, allowing for efficient and 
flexible data manipulation.

Key Functionalities:
- Cleaning and standardizing column names in DataFrames.
- Replacing NaN values with zeros in DataFrames to handle missing data effectively.
- Removing duplicate rows based on unique identifiers like 'ResearchId'.
- Dropping unnecessary columns from DataFrames based on specific requirements.
- Converting specified columns to numeric types to ensure data consistency.
- Processing entire DataFrames to make them ready for analysis or database storage.
- Standardizing grade values to a uniform scale.
- Loading, processing, and renaming data from CSV files for database integration.
- Transferring processed data to a SQL database, handling table creation and data insertion.
- Retrieving data from database tables and returning it as pandas DataFrames.
- Closing database connections to maintain database integrity and performance.

Usage:
This module is intended for use in scenarios where educational data needs to be preprocessed 
before analysis or storage. It is particularly useful in environments where data cleanliness 
and consistency are crucial, such as in academic research, student performance analysis, and 
educational data reporting.

Dependencies:
- pandas: Used for data manipulation and analysis.
- numpy: Provides support for numerical operations.
- sqlite3: Enables interaction with SQLite databases.
- DAFunction: A custom module for specific database-related operations.

Testing:
The module includes a comprehensive test suite, `Test_CWPreprocessing_Functions`, 
which provides unit tests for each functionality. These tests ensure the reliability 
and stability of the module's functionalities.

Note:
The module assumes that the structure of input data is consistent with the expected format, 
particularly in terms of column names and data types. It also assumes that the database connection 
provided is active and valid.

Author:
[Your Name]
[Your Contact Information]
[Other Relevant Information]

"""






import pandas as pd
from DAFunction import DAFunction
import numpy as np
import unittest
import numpy as np
from unittest.mock import MagicMock, patch



class CWPreprocessing:
    
    def __init__(self, db_path):
        """
        Initialize the CWPreprocessing class for data preprocessing and database operations.

        Parameters:
        - db_path (str): The path to the database file.

        Raises:
        - Exception: If the connection to the database fails.
        """
        # Initialise the DAFunction class for database operations
        self.da_function = DAFunction(db_path)
        self.conn = self.da_function.conn 

        if not self.conn:
            raise Exception("Failed to connect to the database.")
    
    
    
    def clean_column_name(self, column):
        """
        Clean a single column name by standardizing its format.

        Parameters:
        column (str): The name of the column to be cleaned.

        Returns:
        str: A cleaned and standardized version of the input column name.

        Description:
        Retains 'ResearchId' as-is. For other names, removes spaces and text after '/'.
        """
        if column == 'ResearchId':
            return column  # Preserve the 'ResearchId' column name as is
        column = column.split('/')[0]
        return column.title().replace(' ', '')


    def clean_column_names(self, df):
        """
        Apply the 'clean_column_name' function to all column names in a DataFrame.

        Parameters:
        df (pandas.DataFrame): The DataFrame whose column names are to be cleaned.

        Returns:
        pandas.DataFrame: DataFrame with cleaned column names.
        """
        df.columns = [self.clean_column_name(col) for col in df.columns]
        return df


    def replace_nan_with_zero(self, df):
        """
        Replace NaN (Not a Number) values in a DataFrame with 0.

        Parameters:
        df (pandas.DataFrame): The DataFrame in which NaN values need to be replaced.

        Returns:
        pandas.DataFrame: The DataFrame after replacing NaN values with 0.

        This function modifies the given DataFrame by replacing all NaN values with 0.
        It is useful for handling missing data in datasets.
        """
        df.fillna(0, inplace=True)
        return df


    def drop_duplicate_research_ids(self, df):
        """
        Remove duplicate rows in a DataFrame based on 'ResearchId', keeping the first occurrence.

        Parameters:
        df (pandas.DataFrame): DataFrame containing potential duplicate rows.

        Returns:
        pandas.DataFrame: DataFrame with duplicates removed.
        """
        df.sort_values(by='Grade', ascending=False, inplace=True)
        df.drop_duplicates(subset=['ResearchId'], keep='first', inplace=True)
        return df


    def drop_unnecessary_columns(self, df, columns_to_drop):
        """
        Remove specified columns from a DataFrame if they exist.

        Parameters:
        df (pandas.DataFrame): The DataFrame to modify.
        columns_to_drop (list): A list of strings representing column names to be dropped.

        Returns:
        pandas.DataFrame: Modified DataFrame with specified columns removed.
        """
        for col in columns_to_drop:
            if col in df.columns:
                df.drop(col, axis=1, inplace=True)
        return df


    def convert_columns_to_numeric(self, df, columns):
        """
        Convert specified columns of a DataFrame to numeric type.

        Parameters:
        df (pandas.DataFrame): DataFrame containing the columns to be converted.
        columns (list): List of column names to be converted to numeric type.

        Returns:
        pandas.DataFrame: DataFrame with specified columns converted to numeric type.
        """
        for col in columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df


    def process_dataframe(self, df):
        """
        Perform data cleaning operations on a DataFrame, focusing on 'Grade' and 'Q' columns.

        This function processes a provided DataFrame by standardizing column names, converting any '-' symbols 
        to NaN in 'Grade' and 'Q' columns, converting these specific columns to numeric types, replacing NaN values 
        with zero (0.0), removing duplicate rows based on 'ResearchId', and dropping unnecessary columns.

        Parameters:
        df (pandas.DataFrame): The DataFrame to be processed. Expected to contain columns like 'ResearchId', 
                               'Grade', 'Q1', 'Q2', etc., and may include others like 'State' and 'TimeTaken'.

        Returns:
        pandas.DataFrame: A cleaned and processed DataFrame. The returned DataFrame has standardized column names, 
                          no null values in 'Grade' and 'Q' columns, unique rows based on 'ResearchId', 
                          and unnecessary columns removed. 'Grade' and 'Q' columns are converted to numeric types.

        Steps Involved:
        1. Standardize Column Names: Cleans up the column names by removing spaces and splitting at '/'.
        2. Replace '-' with NaN in 'Grade' and 'Q' Columns: Converts any '-' symbols in these columns to NaN.
        3. Convert Specified Columns to Numeric: Changes 'Grade' and columns starting with 'Q' to numeric types,
           with non-numeric values converted to NaN.
        4. Replace NaN with Zero in Specified Columns: Replaces all NaN or null values in 'Grade' and 'Q' columns
           with 0.0.
        5. Remove Duplicate Rows: Drops duplicate rows based on 'ResearchId', keeping only the first occurrence.
        6. Drop Unnecessary Columns: Removes columns like 'State' and 'TimeTaken' if present.

        """
        df = df.copy()
        df = self.clean_column_names(df)

        grade_q_columns = [col for col in df.columns if 'Grade' in col or col.startswith('Q')]

        for col in grade_q_columns:
            df[col] = df[col].replace('-', np.nan)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        df = self.drop_duplicate_research_ids(df)
        df = self.drop_unnecessary_columns(df, ['State', 'TimeTaken'])

        return df


    def standardise_grade(self, df):
        """
        Standardize the 'Grade' column of a DataFrame to a uniform scale.

        Parameters:
        - df (pandas.DataFrame): The DataFrame with the 'Grade' column.

        Returns:
        - pandas.DataFrame: The DataFrame with standardized 'Grade' column.
        """
        df_copy = df.copy()
        df_copy['Grade'] = pd.to_numeric(df_copy['Grade'], errors='coerce')
        df_copy = df_copy[df_copy['Grade'].notna()]
        max_grade = df_copy['Grade'].max()
        df_copy['Grade'] = (df_copy['Grade'] / max_grade) * 100
        return df_copy


    def standardise_and_rename(self, dataframes):
        """
        Standardize and rename a collection of DataFrames.

        Parameters:
        - dataframes (dict): A dictionary of DataFrames to process.

        Returns:
        - dict: A dictionary of standardized and renamed DataFrames.
        """
        cleaned_dataframes = {}
        for key, df in dataframes.items():
            df_copy = df.copy()
            df_copy = self.standardise_grade(df_copy)
            cleaned_key = f'df_Clean{key}'
            cleaned_dataframes[cleaned_key] = df_copy
        return cleaned_dataframes



    # Global variable for table name mapping
    table_name_mapping = {
        'Formative_Test_1.csv': 'Test1', 
        'Formative_Test_2.csv': 'Test2',
        'Formative_Test_3.csv': 'Test3', 
        'Formative_Test_4.csv': 'Test4',
        'Formative_Mock_Test.csv': 'Mocktest', 
        'SumTest.csv': 'Sumtest'
    }

    def load_process_and_rename_data(self, folder_path):
        """
        Load, process, and rename data from CSV files in a specified folder.

        Parameters:
        - folder_path (str): Path to the folder containing CSV files.

        Returns:
        - tuple: A tuple containing two dictionaries, one with original and another with processed DataFrames.
        """
        # Load original dataframes
        original_dataframes = self.da_function.load_csv_files_from_folder(folder_path, exclude_files=['StudentRate.csv'])

        # Process and rename dataframes
        processed_dataframes = {}
        for file_name, df in original_dataframes.items():
            new_table_name = CWPreprocessing.table_name_mapping.get(file_name + '.csv', file_name)
            processed_df = self.process_dataframe(df)
            processed_dataframes[new_table_name] = processed_df

        return original_dataframes, processed_dataframes
            
            
    

    def transfer_data_to_database(self, processed_dataframes):
        """
        Transfer processed DataFrames to the specified database. 

        This function iterates over a dictionary of processed pandas DataFrames and transfers each of them to a 
        database table. The method determines the appropriate SQL table column data types and utilizes the 
        DAFunction class's static method to create and transfer each DataFrame to the corresponding SQL table.

        Parameters:
        - processed_dataframes (dict): A dictionary where the keys are table names (str) and the values are 
                                       the processed pandas DataFrames. Each DataFrame is assumed to represent 
                                       the data to be stored in the table named by its key.

        Processing Steps:
        1. For each DataFrame in the dictionary:
            a. Determine the appropriate SQL column data types for each column in the DataFrame. 'ResearchId', 
               'StartedOn', and 'Completed' have predefined types, while columns starting with 'Q' are assumed 
               to be real numbers.
            b. Use the DAFunction class's 'create_and_transfer_to_sqltable' method to create the SQL table 
               (if not exists) and transfer the DataFrame to it.
        2. Handle exceptions that may occur during the database transfer process, logging errors for each table.

        Notes:
        - The function assumes that the 'DAFunction' class has a static method 'create_and_transfer_to_sqltable' 
          capable of handling the DataFrame to SQL table conversion and data transfer.
        - It is expected that the database connection is already established and available through the 'self.conn' attribute.
        - This function handles basic data type conversion for specific known columns and may need modifications 
          to handle different or additional columns.
        - The function prints a message to the console for each successfully transferred table and logs errors 
          encountered during the transfer process.
        """
        for table_name, df in processed_dataframes.items():
            try:
                # Define the data types for the SQL table columns
                column_data_types = {
                    'ResearchId': 'INTEGER',
                    'StartedOn': 'TIMESTAMP',
                    'Completed': 'TIMESTAMP',
                    'Grade': 'REAL'
                }
                column_data_types.update({col: 'REAL' for col in df.columns if col.startswith('Q')})

                # Call the static method from DAFunction class with column data types
                DAFunction.create_and_transfer_to_sqltable(df, table_name, self.conn, column_data_types)
                print(f"Transferred data to table {table_name}")
            except Exception as e:
                print(f"Error transferring data to table {table_name}: {e}")
                
                
        
    def get_table_data(self, table_name):
        """
        Retrieve data from a specific table in the database.

        Parameters:
        - table_name (str): Name of the table to retrieve data from.

        Returns:
        - pandas.DataFrame: DataFrame containing data from the specified table.
        """
        if self.conn is None:
            return None
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn)
            return df
        except Exception as e:
            print(f"Error retrieving data from {table_name}: {e}")
            return None

        
    def get_dataframe(self, table_type, table_name):
        """
        Retrieve a DataFrame based on table type and name.

        Parameters:
        - table_type (str): Type of the table ('processed' or 'original').
        - table_name (str): Name of the table.

        Returns:
        - pandas.DataFrame: The requested DataFrame.
        """
        if table_type.lower() == 'processed':
            if self.conn:
                df = self.get_table_data(table_name)
                return df
        elif table_type.lower() == 'original':
            # Assuming original_dataframes dictionary is accessible here
            return original_dataframes.get(table_name, "Table not found")
        return None
    

    
    def close_connection(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()

       
    
    
class Test_CWPreprocessing_Functions(unittest.TestCase):
    
    def setUp(self):
        """
        Set up the test environment for the CWPreprocessing class.

        This method is executed before each test. It initializes a mock database connection and patches
        the DAFunction class to use this mock connection. This setup ensures that tests do not affect
        a real database.
        """
        # Mock the DAFunction class to avoid actual database connection
        self.mock_da_function = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_da_function.conn = self.mock_conn

        # Patch the DAFunction to use the mock object
        with patch('DAFunction.DAFunction', return_value=self.mock_da_function):
            self.preprocessing = CWPreprocessing(':memory:')
        
    def test_clean_column_name(self):
        """
        Test the clean_column_name method for its ability to standardize column names.

        This test checks whether the method correctly cleans and formats various types of column names, 
        including handling spaces, special characters, and retaining specific column names like 'ResearchId'.
        """
        self.assertEqual(self.preprocessing.clean_column_name("test column"), "TestColumn")
        self.assertEqual(self.preprocessing.clean_column_name("singleWord"), "Singleword")
        self.assertEqual(self.preprocessing.clean_column_name("multi/word column"), "Multi")

        
    def test_clean_column_names(self):
        """
        Test the clean_column_names method to ensure it correctly cleans all column names in a DataFrame.

        This test verifies that the method applies the clean_column_name logic to each column in a given DataFrame,
        resulting in a DataFrame with all column names standardized.
        """
        df = pd.DataFrame(columns=["test column", "singleWord", "multi/word column"])
        df = self.preprocessing.clean_column_names(df)
        expected_columns = ["TestColumn", "Singleword", "Multi"]
        self.assertEqual(list(df.columns), expected_columns)

        
    def test_replace_nan_with_zero(self):
        """
        Test the replace_nan_with_zero method for replacing NaN values in a DataFrame.

        This test checks if the method accurately replaces all NaN (Not a Number) values in the DataFrame with zero,
        ensuring there are no missing values left in the data.
        """
        df = pd.DataFrame({"A": [1, np.nan, 3]})
        df = self.preprocessing.replace_nan_with_zero(df)
        self.assertTrue(df.isnull().sum().sum() == 0)

        
    def test_drop_duplicate_research_ids(self):
        """
        Test the drop_duplicate_research_ids method for removing duplicate rows.

        This test ensures that the method correctly identifies and removes duplicate rows in a DataFrame based on the
        'ResearchId' column, keeping only the first occurrence of each unique ID.
        """
        df = pd.DataFrame({"ResearchId": [1, 1, 2], "Grade": [90, 80, 85]})
        df = self.preprocessing.drop_duplicate_research_ids(df)
        self.assertEqual(len(df), 2)

        
    def test_drop_unnecessary_columns(self):
        """
        Test the drop_unnecessary_columns method for its ability to remove specified columns.

        This test verifies that the method can successfully remove a list of given columns from a DataFrame, leaving
        only the relevant columns in the data.
        """
        df = pd.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6], "C": [7, 8, 9]})
        df = self.preprocessing.drop_unnecessary_columns(df, ["B", "C"])
        self.assertEqual(list(df.columns), ["A"])

        
    def test_process_dataframe(self):
        """
        Test the process_dataframe method for performing a series of data cleaning operations.

        This test checks the method's ability to standardize column names, convert certain symbols to NaN, handle 
        numeric conversions, and remove duplicates and unnecessary columns, resulting in a clean and processed DataFrame.
        """
        df = pd.DataFrame({
            "ResearchId": [1, 1, 2],
            "Grade": [80, np.nan, 60],
            "State": ['NY', 'NY', 'CA'],
            "TimeTaken": [30, 45, 40]
        })
        df = self.preprocessing.process_dataframe(df)
        self.assertTrue("State" not in df.columns and "TimeTaken" not in df.columns)
        self.assertTrue(df.isnull().sum().sum() == 0)

        
    def test_standardise_grade(self):
        """
        Test the standardise_grade method for normalizing grade values in a DataFrame.

        This test ensures that the method correctly standardizes the 'Grade' column in a DataFrame to a uniform scale,
        handling non-numeric and missing values appropriately.
        """
        df = pd.DataFrame({"Grade": [50, 100, 75]})
        df = self.preprocessing.standardise_grade(df)
        self.assertEqual(df["Grade"].max(), 100.0)

        
    def test_standardise_and_rename(self):
        """
        Test the standardise_and_rename method for standardizing and renaming a collection of DataFrames.

        This test verifies that the method can process multiple DataFrames, standardizing their 'Grade' columns and
        renaming them for consistency and clarity.
        """
        dfs = {"Test": pd.DataFrame({"Grade": [50, 100, 75]})}
        cleaned_dfs = self.preprocessing.standardise_and_rename(dfs)
        self.assertIn('df_CleanTest', cleaned_dfs)
        self.assertEqual(cleaned_dfs["df_CleanTest"]["Grade"].max(), 100.0)


    def test_convert_columns_to_numeric(self):
        """
        Test the convert_columns_to_numeric method for converting specified columns to numeric type.

        This test checks if the method accurately converts given columns in a DataFrame to numeric types, handling
        non-numeric values by converting them to NaN.
        """
        df = pd.DataFrame({"A": ["1", "2", "three"], "B": ["4.1", "five", "6.2"], "C": ["7", "8", "9"]})
        columns_to_convert = ["A", "B"]
        df = self.preprocessing.convert_columns_to_numeric(df, columns_to_convert)
        self.assertTrue(pd.api.types.is_numeric_dtype(df["A"]))
        self.assertTrue(pd.api.types.is_numeric_dtype(df["B"]))
        self.assertTrue(df["B"].isna().sum() > 0)  # 'five' should be converted to NaN

    def test_load_process_and_rename_data(self):
        """
        Test the load_process_and_rename_data method for loading, processing, and renaming data from CSV files.

        This test ensures that the method can correctly read data from a folder, process each DataFrame, and rename them
        based on predefined mappings. The test uses a mock to simulate reading CSV files.
        """
        # Mock reading CSV files from a folder
        test_csv_data = "ResearchId,Grade\n1,80\n2,90"
        with patch('builtins.open', return_value=StringIO(test_csv_data)):
            original_dataframes, processed_dataframes = self.preprocessing.load_process_and_rename_data('/fake/path')
        self.assertIn('Test1', processed_dataframes)
        self.assertIsInstance(processed_dataframes['Test1'], pd.DataFrame)

    def test_transfer_data_to_database(self):
        """
        Test the transfer_data_to_database method for its ability to transfer DataFrames to a database.

        This test verifies that the method can successfully define SQL column data types and use the DAFunction class's 
        method to transfer DataFrames to the corresponding SQL tables. The test checks if the database transfer method is called.
        """
        processed_dataframes = {'Test1': pd.DataFrame({"ResearchId": [1, 2], "Grade": [80, 90]})}
        self.preprocessing.transfer_data_to_database(processed_dataframes)
        # Verify if the database method was called
        self.assertTrue(self.mock_da_function.create_and_transfer_to_sqltable.called)


    def test_get_table_data(self):
        """
        Test the get_table_data method for retrieving data from a specific table in the database.

        This test ensures that the method can correctly execute a SQL query to retrieve data from a specified table,
        returning the results as a DataFrame. The test uses a mock to simulate the database query.
        """
        mock_df = pd.DataFrame({"ResearchId": [1, 2], "Grade": [80, 90]})
        with patch('pandas.read_sql_query', return_value=mock_df):
            df = self.preprocessing.get_table_data('Test1')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)


    def test_get_dataframe(self):
        """
        Test the get_dataframe method for retrieving a DataFrame based on table type and name.

        This test checks the method's ability to return the correct DataFrame, whether it's an 'original' or 'processed'
        type, based on the provided table name. The test mocks the necessary database interactions.
        """
        # Mock return value of get_table_data for processed data
        self.preprocessing.get_table_data = MagicMock(return_value=pd.DataFrame({"ResearchId": [1], "Grade": [90]}))
        df = self.preprocessing.get_dataframe('processed', 'Test1')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    def test_close_connection(self):
        """
        Test the close_connection method to ensure it properly closes the database connection.

        This test verifies that the method correctly calls the close method on the database connection object.
        """
        self.preprocessing.close_connection()
        self.mock_conn.close.assert_called_once()

        
    
def run_tests():
    """
    Execute the test suite for the CWPreprocessing module.

    This function compiles a test suite from the TestCWPreprocessingFunctions class, 
    which contains unit tests for various data processing functions in the module. 
    The suite is then run using a test runner, which executes each test and provides the results.

    Parameters:
    None

    Returns:
    None
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_CWPreprocessing_Functions))
    runner = unittest.TextTestRunner()
    runner.run(suite)
    
    

    

def main(table_type, table_name):
    # Create an instance of CWPreprocessing with the database path
    preprocessing = CWPreprocessing('CWDatabase.db')

    original_dataframes, processed_dataframes = preprocessing.load_process_and_rename_data('TestResult Folder')

    preprocessing.transfer_data_to_database(processed_dataframes)

    df = preprocessing.get_dataframe(table_type, table_name)
    display(df)

    preprocessing.close_connection()

    
    
if __name__ == '__main__':
    choice = input("Enter 'P' to run the program or 'T' to run tests: ").lower()
    if choice == 'p':
        main()
    elif choice == 't':
        run_tests()



