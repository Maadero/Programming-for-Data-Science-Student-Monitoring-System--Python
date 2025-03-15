"""
underperformingStudent Module

This module contains the implementation of the `underperformingStudent` class, which is designed for
analyzing and processing student performance data from a database. The class offers various methods
for data extraction, transformation, and visualization to identify underperforming students based on
criteria such as low grades and the number of low grades.

Classes:
    - underperformingStudent: A class for analyzing and processing student performance data.

Methods:
    - create_dataframe(): Generates a consolidated DataFrame containing grades from various tests
      in the database for each ResearchId.
    - replace_nan_with_zero(df): Replaces NaN values in the provided DataFrame with zeros.
    - convert_grades_to_numeric(df): Converts grade columns in the provided DataFrame to numeric type.
    - standardise_grades(df): Standardizes grade columns in the provided DataFrame to a percentage scale.
    - drop_rows_above_threshold(df): Drops rows from the provided DataFrame where at least three
      grade columns have values greater than 49.
    - sort_dataframe(df): Sorts the provided DataFrame by a specified column, e.g., 'Grade_Sumtest'.
    - apply_conditional_formatting(df): Applies conditional formatting to the provided DataFrame
      for highlighting grades based on specific criteria.

Usage Example:
    - To perform the analysis, create an instance of the `underperformingStudent` class with a
      path to the database.
    - Use the class methods to perform various data operations such as creating a DataFrame, replacing
      NaN values, converting grades to numeric type, standardizing grades, dropping rows above a
      certain threshold, sorting the DataFrame, and applying conditional formatting.
    - The results of the analysis, including the number of underperforming students, can be displayed
      and customized using the provided methods.

Note:
    - Ensure that the necessary database tables with appropriate schema and data exist before using
      this module.
    - This module is intended for educational and analytical purposes, allowing users to gain insights
      into student performance data.
  
  
Reference: 23COP504_CW      
Author: F333494
Version: 1
Date: 22/01/24          
"""





from DAFunction import DAFunction
import sqlite3 
import pandas as pd
import unittest
import traceback


class underperformingStudent:
    """
    Represents a class for analyzing underperforming students' grades from a database.

    Args:
        db_path (str): Path to the database.

    Attributes:
        da_function (DAFunction): An instance of DAFunction for database operations.
        db_conn (sqlite3.Connection): The database connection.

    """
    def __init__(self, db_path):
        """
    Initialize the testResults class with a specific database path.

    This constructor establishes a connection to the SQLite database specified by 'db_path'
    and creates an instance of the DAFunction class for further data access and manipulation.
    It is the entry point for database operations related to test results in the context of this class.

    Parameters:
    - db_path (str): A string specifying the path to the SQLite database file. This path is used
                     to establish a database connection and is also passed to the DAFunction class
                     for its internal use.

    Raises exception: If the connection to the SQLite database fails, an exception is raised with
                 a message indicating the failure. 

    The database connection (`self.conn`) and the DAFunction instance (`self.da_function`) are
    essential components for the class. They are used in various methods of the class to interact
    with the database.
    """
        self.da_function = DAFunction(db_path)  # Using DAFunction for database operations
        self.db_conn = self.da_function.conn  # Accessing the connection from DAFunction
        
    
    
    def create_dataframe(self):
        """
        Generate a consolidated DataFrame containing grades from various tests in the database.

        Returns:
        pandas.DataFrame: A DataFrame containing grades from different tests for each ResearchId.
        """
        research_ids_query = '''
            SELECT DISTINCT ResearchId FROM (
                SELECT ResearchId FROM Test1
                UNION
                SELECT ResearchId FROM Test2
                UNION
                SELECT ResearchId FROM Test3
                UNION
                SELECT ResearchId FROM Test4
                UNION
                SELECT ResearchId FROM Mocktest
                UNION
                SELECT ResearchId FROM Sumtest
            )
        '''
        query = f'''
            SELECT
                ResearchIds.ResearchId,
                Test1.Grade AS Grade_Test1,
                Test2.Grade AS Grade_Test2,
                Test3.Grade AS Grade_Test3,
                Test4.Grade AS Grade_Test4,
                Mocktest.Grade AS Grade_Mocktest,
                Sumtest.Grade AS Grade_Sumtest
            FROM
                ({research_ids_query}) AS ResearchIds
                LEFT JOIN Test1 ON ResearchIds.ResearchId = Test1.ResearchId
                LEFT JOIN Test2 ON ResearchIds.ResearchId = Test2.ResearchId
                LEFT JOIN Test3 ON ResearchIds.ResearchId = Test3.ResearchId
                LEFT JOIN Test4 ON ResearchIds.ResearchId = Test4.ResearchId
                LEFT JOIN Mocktest ON ResearchIds.ResearchId = Mocktest.ResearchId
                LEFT JOIN Sumtest ON ResearchIds.ResearchId = Sumtest.ResearchId
        '''
        df = pd.read_sql_query(query, self.db_conn)
        df.set_index('ResearchId', inplace=True)
        return df


    
    def replace_nan_with_zero(self, df):
        """
        Replace NaN values in the DataFrame with zeros.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with NaN values replaced by zeros.
        """
        df.fillna(0, inplace=True)
        return df

    
    
    def convert_grades_to_numeric(self, df):
        """
        Convert grade columns in the DataFrame to numeric type.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with grade columns converted to numeric type.
        """
        grade_columns = df.filter(like='Grade').columns
        for column in grade_columns:
            df[column] = pd.to_numeric(df[column], errors='coerce') 
        return df
    
    
    
    def standardise_grades(self, df):
        """
        Standardize grades in the DataFrame to a percentage scale.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with standardized grades.
        """
        grade_columns = df.filter(like='Grade').columns
        for column in grade_columns:
            df[column] = ((df[column] / df[column].max()) * 100).round(1)
        return df



    def drop_rows_above_threshold(self, df):
        """
        Drop rows where more than 3 grade columns have values above 49.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with rows dropped based on the threshold.
        """
        grade_columns = df.filter(like='Grade').columns
        mask = (df[grade_columns] >= 1) & (df[grade_columns] <= 49)
        mask = mask.sum(axis=1) >= 3  # Count how many columns meet the condition and check if it's at least 3
        df = df[mask]
        return df



    def sort_dataframe(self, df):
        """
        Sort the DataFrame by the 'Grade_Sumtest' column.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame sorted by 'Grade_Sumtest'.
        """
        df = df.sort_values(by='Grade_Sumtest')
        return df
    
    
    
    def apply_conditional_formatting(self, df):
        """
        Apply conditional formatting to highlight test grades in the DataFrame.

        Args:
            df (pandas.DataFrame): The input DataFrame.

        Returns:
            pandas.io.formats.style.Styler: The styled DataFrame.
        """
        def highlight_test_grades(row):
            test_grade_columns = ['Grade_Test1', 'Grade_Test2', 'Grade_Test3', 'Grade_Test4']
            styles = []
            for col, val in row.iteritems():
                if col in test_grade_columns:
                    if 0 <= val < 50: color = 'yellow'
                    elif 50 <= val < 70: color = 'grey'
                    elif val >= 70: color = 'green'
                    else: color = ''
                    styles.append(f'background-color: {color}')
                else:
                    styles.append('')
            return styles

        return df.style.apply(highlight_test_grades, axis=1)
   


class Test_UnderperformingStudent_Functions(unittest.TestCase):
    """
    A test suite for the underperformingStudent class.

    This test suite covers various aspects of the underperformingStudent class
    to ensure that its methods work as expected.

    Attributes:
        connection (sqlite3.Connection): The in-memory database connection for testing.
    Methods: as outlined below.    
    """
    @classmethod
    def setUpClass(cls):
        """
        Set up a test database with dummy data for testing purposes.

        This method creates an in-memory database and populates it with test data
        for use in the test cases.
        """
        cls.connection = sqlite3.connect(':memory:')
        cls.setup_dummy_data(cls.connection)

        
        
    @staticmethod
    def setup_dummy_data(conn):
        """
        Create and populate tables with test data.

        This method creates tables with names 'Test1', 'Test2', 'Test3', 'Test4', 'Mocktest', and 'Sumtest'
        in the specified database connection and inserts test data into these tables.
        """
        tables = ['Test1', 'Test2', 'Test3', 'Test4', 'Mocktest', 'Sumtest']
        for table in tables:
            conn.execute(f"CREATE TABLE {table} (ResearchId INTEGER, Grade REAL);")
            conn.executemany(f"INSERT INTO {table} (ResearchId, Grade) VALUES (?, ?)", [(1, 80), (2, 70), (3, 60)])
        conn.commit()

        
        
    def test_create_dataframe(self):
        """
        Test if DataFrame is correctly created from the database.

        This test case checks whether the 'create_dataframe' method of the underperformingStudent class
        correctly generates a DataFrame containing grades from various tests in the database.
        It verifies the presence of the 'Grade_Test1' column in the resulting DataFrame.
        """
        student_analysis = underperformingStudent(':memory:')
        student_analysis.db_conn = self.connection
        df = student_analysis.create_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue('Grade_Test1' in df.columns)

        
        
    def test_replace_nan_with_zero(self):
        """
        Test if NaN values are correctly replaced with zero.

        This test case checks whether the 'replace_nan_with_zero' method of the underperformingStudent class
        correctly replaces NaN values with zeros in a given DataFrame.
        It verifies that the resulting DataFrame has no NaN values.
        """
        df = pd.DataFrame({'Grade_Test1': [80, None, 60]})
        student_analysis = underperformingStudent(':memory:')
        result_df = student_analysis.replace_nan_with_zero(df)
        self.assertEqual(result_df.isnull().sum().sum(), 0)

        
        
    def test_convert_grades_to_numeric(self):
        """
        Test conversion of grades to numeric type.

        This test case checks whether the 'convert_grades_to_numeric' method of the underperformingStudent class
        correctly converts grade columns in a given DataFrame to numeric type.
        It ensures that the specified column 'Grade_Test1' becomes of numeric type.
        """
        df = pd.DataFrame({'Grade_Test1': ['80', '90', 'invalid']})
        student_analysis = underperformingStudent(':memory:')
        result_df = student_analysis.convert_grades_to_numeric(df)
        self.assertTrue(pd.api.types.is_numeric_dtype(result_df['Grade_Test1']))

        
        
    def test_standardise_grades(self):
        """
        Test if grades are correctly standardized.

        This test case checks whether the 'standardise_grades' method of the underperformingStudent class
        correctly standardizes grade columns in a given DataFrame to a percentage scale.
        It ensures that the maximum value of the specified column 'Grade_Test1' is 100.
        """
        df = pd.DataFrame({'Grade_Test1': [80, 90, 100]})
        student_analysis = underperformingStudent(':memory:')
        result_df = student_analysis.standardise_grades(df)
        self.assertEqual(result_df['Grade_Test1'].max(), 100)

        
        
    def test_drop_rows_above_threshold(self):
        """
        Test if rows above a certain threshold are dropped.

        This test case checks whether the 'drop_rows_above_threshold' method of the underperformingStudent class
        correctly drops rows in a given DataFrame where more than three grade columns have values above 49.
        It ensures that the resulting DataFrame is empty as all rows should be dropped in the test DataFrame.
        """
        df = pd.DataFrame({
            'Grade_Test1': [50, 60, 70],
            'Grade_Test2': [55, 65, 75],
            'Grade_Test3': [60, 70, 80]
        })
        result_df = underperformingStudent.drop_rows_above_threshold(df)
        self.assertTrue(result_df.empty)


        
        
    def test_sort_dataframe(self):
        """
        Test if DataFrame is correctly sorted.

        This test case checks whether the 'sort_dataframe' method of the underperformingStudent class
        correctly sorts a given DataFrame by the 'Grade_Sumtest' column.
        It ensures that the first row in the sorted DataFrame has the expected value for 'Grade_Sumtest'.
        """
        df = pd.DataFrame({'Grade_Sumtest': [70, 90, 85], 'Grade_Test1': [50, 60, 55]})
        student_analysis = underperformingStudent(':memory:')
        result_df = student_analysis.sort_dataframe(df)
        self.assertEqual(result_df.iloc[0]['Grade_Sumtest'], 70)

        
        
    def test_apply_conditional_formatting(self):
        """
        Test if conditional formatting is applied correctly.

        This test case checks whether the 'apply_conditional_formatting' method of the underperformingStudent class
        correctly applies conditional formatting to a given DataFrame.
        It verifies that the formatted DataFrame contains a specific background color style.
        """
        df = pd.DataFrame({'Grade_Test1': [30, 40, 50]})
        student_analysis = underperformingStudent(':memory:')
        styled_df = student_analysis.apply_conditional_formatting(df)
        self.assertTrue('background-color: yellow' in styled_df.render())

        
        
    @classmethod
    def tearDownClass(cls):
        """
        Close the database connection after tests.

        This method is responsible for closing the in-memory database connection
        used for testing after all test cases have been executed.
        """
        cls.connection.close()
        
        
        

def run_tests():
    """
    Run the test suite for the 'TestUnderperformingStudent' class.
    This function initializes a test suite, adds all tests from the specified class,
    and runs them using a test runner.
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_UnderperformingStudent_Functions))
    runner = unittest.TextTestRunner()
    runner.run(suite)



def main():
    """
    Main function to execute tasks related to the analysis of underperforming students.
    """
    try:
        
        student_analysis = underperformingStudent('Resultdatabase.db')

        if not student_analysis.db_conn:
            raise Exception("Failed to connect to the database.")

        df = student_analysis.create_dataframe()
        df = student_analysis.replace_nan_with_zero(df)
        df = student_analysis.convert_grades_to_numeric(df)
        df = student_analysis.standardise_grades(df)
        df = student_analysis.drop_rows_above_threshold(df)
        df = student_analysis.sort_dataframe(df)

        # Apply styling for highlighting grades
        styled_df = student_analysis.apply_conditional_formatting(df)

        number_of_underperforming_students = len(df)
        print(f"There are {number_of_underperforming_students} underperforming students.")
        print("These students have at least 3 grades between 1 and 49")
        print("Yellow denotes grades between 0 and 49, Grey denotes grades between 50 and 69,"
              f"and Green denotes grades above 70.")
        display(styled_df)

    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if student_analysis.db_conn:
            student_analysis.db_conn.close()

            

if __name__ == "__main__":
    user_input = input("Enter 'P' to run the program or 'T' to run tests: ").lower()
    if user_input == 't':
        run_tests()
    elif user_input == 'p':
        main()
    else:
        print("Invalid choice. Enter 'P' to run the program or 'T' to run tests.")
