"""
Module: hardworkingStudents

This module contains the hardworkingStudents class designed to analyze student performance data. 
It identifies students who, despite having beginner-level knowledge, show exceptional performance 
in summative assessments. The module integrates with a database through the DAFunction module for 
data retrieval and processing.

Classes:
    hardworkingStudents: Handles the operations for identifying and analyzing hardworking students.

Key Functionalities:
    - Establish a connection to a SQLite database to access student data.
    - Read and process student data from CSV files.
    - Join student data with their respective performance data from the database.
    - Identify hardworking students based on specific criteria (beginner level with high grades).
    - Display a summary of identified hardworking students.

Usage:
    The module can be used to instantiate a hardworkingStudents object with a database path. 
    Once created, it can read student data, process it, and display the results.

Example:
    ```
    hw_students = hardworkingStudents('path_to_database.db')
    hw_students.run()
    ```

Dependencies:
    - DAFunction module for database interaction.
    - pandas for data manipulation.
    - unittest for running unit tests.

Tests:
    The module includes a test suite `Test_hardworkingStudents_Functions` to verify the correctness 
    of each method in the hardworkingStudents class. The tests use mock objects and patches to isolate 
    the tests from actual file system and database dependencies.


Reference: 23COP504_CW
Author: F333494
Version: 1
Date: 22/01/24
"""



from DAFunction import DAFunction
import pandas as pd
import sqlite3
import unittest
from unittest.mock import patch, MagicMock, create_autospec



class hardworkingStudents:
    def __init__(self, db_path):
        """
        Initialize the hardworkingStudents class with a database path.

        This class is responsible for identifying hardworking students based on
        their performance and self-assessed programming knowledge level.

        Parameters:
        - db_path (str): Path to the SQLite database file.
        """
        self.db_conn = DAFunction.connect_to_database(db_path)
      

        
    def read_student_data(self, file_path):
        """
        Read student data from a CSV file into a DataFrame.

        This method utilizes the DAFunction module to read student data from a CSV file.

        Parameters:
        - file_path (str): Path to the CSV file containing student data.

        Returns:
        - pandas.DataFrame: A DataFrame containing the student data.
        """
        return DAFunction.read_csv_to_df(file_path)
    
    

    def join_student_data_with_sumtest(self, student_df, sumtest_table='Sumtest'):
        """
        Join student data DataFrame with the Sumtest table from the database.

        This method combines student information with their respective grades from
        the Sumtest table in the database.

        Parameters:
        - student_df (pandas.DataFrame): DataFrame containing student data.
        - sumtest_table (str, optional): Name of the Sumtest table in the database.

        Returns:
        - pandas.DataFrame: A DataFrame resulting from the join operation.
        """
        student_df_copy = student_df[['research id', 'What level programming knowledge do you have?']].copy()
        student_df_copy.rename(columns={
            'research id': 'ResearchId', 
            'What level programming knowledge do you have?': 'Ratings'
        }, inplace=True)

        sumtest_df = pd.read_sql_query(f"SELECT * FROM {sumtest_table}", self.db_conn)
        sumtest_df.rename(columns={'Grade': 'Grade_SumTest'}, inplace=True)
        joined_df = pd.merge(student_df_copy, sumtest_df, on='ResearchId', how='inner')
        joined_df.set_index('ResearchId', inplace=True)

        return joined_df

    
    
    def generate_hardworking_students_list(self, df):
        """
        Generate a list of hardworking students based on certain criteria.

        This method filters and sorts students who are rated as 'Below Beginner' or 'Beginner'
        and have grades above 60 in the Sumtest.

        Parameters:
        - df (pandas.DataFrame): DataFrame containing joined student data and grades.

        Returns:
        - pandas.DataFrame: A DataFrame containing data of hardworking students.
        """
        filtered_df = df[
            (df['Ratings'].isin(['Below Beginner', 'Beginner'])) & 
            (df['Grade_SumTest'] > 60)
        ].sort_values(by='Grade_SumTest', ascending=False)
        result_df = filtered_df[['Grade_SumTest', 'Ratings']]

        return result_df

    
    
    def display_hardworking_students(self, hardworking_students_df):
        """
        Display information about hardworking students.

        This method prints the number of hardworking students and details about
        their performance and self-assessed programming knowledge level.

        Parameters:
        - hardworking_students_df (pandas.DataFrame): DataFrame containing data of hardworking students.
        """
        number_of_hardworking_students = len(hardworking_students_df)
        print(f"There are {number_of_hardworking_students} hardworking students" 
              f" according to this programme and they are all beginners. Their"
              f" summative online test scores exceeds 60.")
        print(hardworking_students_df)

        
        
    def run(self):
        """
        Main method to execute the functionalities of the hardworkingStudents class.

        This method acts as the primary workflow, orchestrating the reading,
        processing, and displaying of hardworking student data.
        """
        # Method that acts like 'main' for this class
        try:
            student_df = self.read_student_data('TestResult Folder/StudentRate.csv')
            joined_df = self.join_student_data_with_sumtest(student_df)
            hardworking_students_df = self.generate_hardworking_students_list(joined_df)
            self.display_hardworking_students(hardworking_students_df)
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            if self.db_conn:
                self.db_conn.close()


                
class Test_hardworkingStudents_Functions(unittest.TestCase):
    """
    Test suite for testing the functionalities of the hardworkingStudents class.

    This class contains a set of unit tests to ensure that the hardworkingStudents class
    correctly reads, processes, and analyzes student data for identifying hardworking students.
    It utilizes mock objects and patches to isolate the tests from actual database and file system dependencies.
    """

    
    @patch('hardworkingStudents.DAFunction')
    def setUp(self, MockDAFunction):
        """
        Set up mock objects for each test method.

        This method is called before every individual test. It sets up mock objects for
        the database connection and CSV reading functionalities of the DAFunction module.
        """
        self.mock_da_function_instance = MagicMock()
        self.mock_da_function_instance.connect_to_database.return_value = sqlite3.connect(':memory:')

        # Patch the DAFunction class to return the mock instance
        with patch('hardworkingStudents.DAFunction', return_value=self.mock_da_function_instance):
            self.hw_students = hardworkingStudents('Resultdatabase.db')

        
        
    def test_read_student_data(self):
        """
        Test the read_student_data method of hardworkingStudents class.

        This test verifies if the method correctly reads data from a CSV file into a DataFrame.
        It uses a mock object to simulate the reading of a CSV file.
        """
        with patch.object(DAFunction, 'read_csv_to_df', return_value=pd.DataFrame()) as mock_read_csv:
            result_df = self.hw_students.read_student_data('mock_file.csv')
            mock_read_csv.assert_called_with('mock_file.csv')
            self.assertIsInstance(result_df, pd.DataFrame)

            
            
    def test_join_student_data_with_sumtest(self):
        """
        Test the join_student_data_with_sumtest method of hardworkingStudents class.

        This test checks whether the method accurately joins student data with the Sumtest
        table from the database and returns the correct DataFrame structure.
        """
        student_df = pd.DataFrame({'research id': [1, 2], 'What level programming knowledge do you have?': ['Beginner', 'Expert']})
        sumtest_df = pd.DataFrame({'ResearchId': [1, 2], 'Grade': [70, 80]})
        with patch('pandas.read_sql_query', return_value=sumtest_df):
            result_df = self.hw_students.join_student_data_with_sumtest(student_df)
            self.assertIn('Grade_SumTest', result_df.columns)
            self.assertIn('Ratings', result_df.columns)

            
            
    def test_generate_hardworking_students_list(self):
        """
        Test the generate_hardworking_students_list method of hardworkingStudents class.

        This test ensures that the method filters and sorts the DataFrame correctly to identify
        hardworking students based on specific criteria.
        """
        df = pd.DataFrame({
            'Ratings': ['Beginner', 'Expert', 'Beginner'],
            'Grade_SumTest': [65, 50, 80]
        })
        result_df = self.hw_students.generate_hardworking_students_list(df)
        self.assertTrue(result_df['Grade_SumTest'].min() > 60)
        self.assertTrue(all(rating in ['Below Beginner', 'Beginner'] for rating in result_df['Ratings']))

        
        
    def test_display_hardworking_students(self):
        """
        Test the display_hardworking_students method of hardworkingStudents class.

        This test confirms that the method correctly displays information about hardworking
        students, including their count and relevant data.
        """
        df = pd.DataFrame({'Grade_SumTest': [80, 70], 'Ratings': ['Beginner', 'Beginner']})
        with patch('builtins.print') as mock_print:
            self.hw_students.display_hardworking_students(df)
            mock_print.assert_called()
        
    def tearDown(self):  
        """
        Tear down any resources after each test.

        This method is called after each test method execution. It is used to clean up
        or close any resources used during the test.
        """
        self.patcher.stop()
        if self.hw_students.db_conn:
            self.hw_students.db_conn.close()
    
                   

def run_tests():
    """
    Run the test suite for all tests defined in the Test_studentPerformance_Functions class.
    This function initializes a test suite, adds all tests, and then runs them.
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_hardworkingStudents_Functions))
    runner = unittest.TextTestRunner()
    runner.run(suite)

    
    
def main():
    hw_students = hardworkingStudents('Resultdatabase.db')
    hw_students.run()  
    
    
    
if __name__ == '__main__':
    """
    The main entry point of the program when run as a script.
    It prompts the user to choose between running the program or the tests.
    Based on the user's choice, it either runs the main function of the program
    or the test suite. It also handles invalid choices by providing appropriate feedback.
    """
    choice = input("Enter 'P' to run the program or 'T' to run tests: ").lower()
    if choice == 'p':
        main()
    elif choice == 't':
        run_tests()
    else:
        print("Invalid choice. Enter 'P' to run the program or 'T' to run tests.")
