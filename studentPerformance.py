"""
Module: studentPerformance

This module provides functionality for analyzing and visualizing student performance data from a database. It includes the 'studentPerformance' class, which encapsulates various methods for retrieving, processing, and visualizing data related to student grades and performance metrics. The module is designed to work with SQLite databases and uses pandas for data manipulation and matplotlib for visualization.

Classes:
    studentPerformance: A class that provides methods to interact with and analyze student performance data in a database.

Key Features:
    - Retrieval of maximum, specific, and average grades from database tables.
    - Standardization of grades to a 100-point scale.
    - Calculation of relative performance metrics.
    - Visualization of student performance using bar charts.
    - Support for analyzing multiple tests and question columns within a database.

Dependencies:
    - DAFunction: A module providing database access functionality.
    - sqlite3: For SQLite database operations.
    - pandas: For data manipulation and retrieval.
    - matplotlib: For data visualization.
    - unittest: For running unit tests on the module's functions.
    - traceback: For detailed error reporting.

Usage:
    The module can be used to instantiate a 'studentPerformance' object with a given database path. Once created, various methods can be called to analyze and visualize data from the database.

Examples:
    - Creating an instance of 'studentPerformance' and visualizing data for a specific test and student:
        ```
        sp = studentPerformance('path_to_database.db')
        sp.visualise_test(research_id=1, table_name='TestTable')
        ```

    - Running the module's unit tests:
        ```
        if __name__ == "__main__":
            run_tests()
        ```

Reference: 23COP504_CW
Author: F333494
Version: 1
Date: 22/01/24
"""



from DAFunction import DAFunction
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import unittest
from unittest.mock import patch
import traceback


class studentPerformance:
    
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
        self.da_function = DAFunction(db_path)
        self.conn = self.da_function.conn
    
        
        
    @staticmethod
    def get_max_grade(table_name, q_column, connection):
        """
        Get the maximum grade from a specific column in a table.

        Parameters:
        - table_name (str): The name of the database table.
        - q_column (str): The column name to retrieve the maximum grade from.
        - connection (sqlite3.Connection): The database connection object.

        Returns:
        - float: The maximum grade value found in the specified column.
        """
        query = f"SELECT MAX(CAST({q_column} AS REAL)) FROM {table_name};"
        return pd.read_sql_query(query, connection).iloc[0, 0]



    def get_column_grade_and_average(self, research_id, table_name, q_column, connection):
        """
        Retrieve a specific grade and the average grade of a given column from a specified table.

        Parameters:
        - research_id (int): The ResearchId to filter the data.
        - table_name (str): The table name in the database.
        - q_column (str): The column name from which to retrieve the data.
        - connection (sqlite3.Connection): The database connection object.

        Returns:
        - tuple: A tuple containing the specific grade's value and the average grade of the column.
                 Both are returned as floats. Returns (None, None) if no data is found.
        """
        cursor = connection.cursor()
        query = f"SELECT {q_column} FROM {table_name} WHERE ResearchId = ?;"
        cursor.execute(query, (research_id,))
        entry = cursor.fetchone()

        if entry:
            query_avg = f"SELECT AVG(CAST({q_column} AS REAL)) FROM {table_name};"
            cursor.execute(query_avg)
            average = cursor.fetchone()[0]
            return float(entry[0]), average

        return None, None

    
    
    def standardise_grades(self, grade, average, max_grade):
        """
        Standardise given entry and average values to a scale of 100 based on the maximum grade.

        Parameters:
        - grade (float): The specific grade value to be standardized.
        - average (float): The average grade to be standardized.
        - max_grade (float): The maximum grade used for standardization.

        Returns:
        - tuple: A tuple containing the standardized grades of grade and average.
                 Returns (None, None) if grade is None.
        """
        if grade is not None:
            standardised_grade = (grade / max_grade) * 100
            standardised_average = (average / max_grade) * 100
            return standardised_grade, standardised_average
        else:
            return None, None

    
    
    def calculate_relative_performance(self, grade, average):
        """
        Calculate relative performance based on the grade and average.

        Parameters:
        - grade (float): The student's grade.
        - average (float): The average grade.

        Returns:
        - float: Relative performance.
        """
        return grade - average
    
    
    
    def get_question_columns(self, table_name):
        """
        Get the list of question columns (starting with 'Q') in a database table.

        Parameters:
        - table_name (str): The name of the database table.

        Returns:
        - list of str: A list of column names that start with 'Q'.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall() if row[1].startswith('Q')]
        return columns
    
    
    
    def setup_visualisation(self):
        """
        Initialize the visualisation settings for creating a plot using matplotlib.

        This function configures the initial parameters for a matplotlib plot. It sets the figure size 
        and provides a starting point for further customizations and plotting data. The purpose is to 
        establish a consistent and suitable layout for the visual representations that will follow.

        Parameters:
        None

        Returns:
        None

        Effects:
        - Initializes a new matplotlib figure with a specified size.
        - The figure size is set to 10 inches wide by 6 inches tall, which is determined to be 
          visually appealing and suitable for a variety of data visualisations.

        Note:
        - This function should be called prior to plotting specific data to ensure that the 
          visualisation layout is set up correctly.
        - This function does not return any value and does not take any parameters.
        - Subsequent plotting functions or commands should follow this initialization to add data 
          to the figure.
        """
        plt.figure(figsize=(10, 6))

        

    def add_bar_to_visualisation(self, label, grade, color, index):
        """
        Add a bar to the visualization.

        Parameters:
        - label (str): The label for the bar.
        - grade (float): The numerical value represented by the bar.
        - color (str): The color of the bar.
        - index (int): The index position of the bar in the plot.
        """
       
        rounded_grade = round(grade)

        
        plt.bar(label, rounded_grade, color=color)

        # Display the rounded grade as an integer on top of the bar
        plt.text(index, rounded_grade, str(rounded_grade), ha='center', va='bottom')



    def retrieve_and_standardise_q_column_data(self, research_id, table_name, q_column, connection):
        """
        Retrieve and standardise data for a specific column (q_column) for a given research ID from a table.

        Parameters:
        - research_id (int): The unique identifier of the research subject.
        - table_name (str): The name of the database table from which data is drawn.
        - q_column (str): The specific column in the table whose data is to be retrieved and standardised.
        - connection (sqlite3.Connection): The connection object to the SQLite database.

        Returns:
        - tuple: A tuple containing two elements, the standardised value of the specific entry for the research ID,
                 and the standardised average value of the column. Returns (None, None) in case of an error or if
                 the entry is not found.

        This function aims to retrieve the specific entry for the research ID and the average value of all entries
        in the specified column, then standardise these values relative to the maximum value in the column.
        """
        try:
            max_grade = self.get_max_grade(table_name, q_column, self.conn)
            grade, average = self.get_column_grade_and_average(research_id, table_name, q_column, self.conn)
            return self.standardise_grades(grade, average, max_grade) if grade is not None else (None, None)
        except Exception as e:
            print(f"Error in data retrieval: {e}")
            return None, None
        
        
        
    def visualise_performance(self, research_id, table_name, q_column, grade, average):
        """
        Visualize the performance of a specific student in a specific column.

        Parameters:
        - research_id (int): The ResearchId to filter the data.
        - table_name (str): The name of the database table representing the test.
        - q_column (str): The specific question column.
        - grade (float): The student's grade in the question.
        - average (float): The average grade in the question.
        """
        self.setup_visualisation()
        relative_performance = grade - average

        # Absolute Performance
        self.add_bar_to_visualisation(f"{q_column} Grade", grade, 'green', 0)

        # Relative Performance
        self.add_bar_to_visualisation(f"{q_column} Relative", relative_performance, 'orange', 1)

        plt.title(f"Performance in {q_column} of {table_name} for ResearchId {research_id}")
        plt.ylabel("Score")
        plt.show()

        
        
    def visualise_question_column(self, research_id, table_name, q_column):
        """
        Visualize the performance of a specific student in a specific question column.

        Parameters:
        - research_id (int): The ResearchId to filter the data.
        - table_name (str): The name of the database table representing the test.
        - q_column (str): The specific question column.
        """
        grade, average = self.retrieve_and_standardise_q_column_data(research_id, table_name, q_column, self.conn)
        if grade is not None:
            self.visualise_performance(research_id, table_name, q_column, grade, average)

            
            
    def visualise_test(self, research_id, table_name):
        """
        Visualize the performance of a specific test for a given research ID.

        Parameters:
        - research_id (int): The ResearchId to filter the data.
        - table_name (str): The name of the database table representing the test.

        Effects:
        - Displays a bar chart representing the performance in each question of the specified test.
        """
        q_columns = self.get_question_columns(table_name)
        for q_column in q_columns:
            self.visualise_question_column(research_id, table_name, q_column)

            

    def visualise_all_tests(self, research_id):
        """
        Visualize the performance of all tests for a given research ID.

        Parameters:
        - research_id (int): The ResearchId to filter the data.

        Effects:
        - Displays bar charts for the performance in each question of all tests associated with the research ID.
        """
        try:
            tables = self.da_function.get_table_names(self.conn)
            for table in tables:
                self.visualise_test(research_id, table)
        except Exception as e:
            print(f"Error in visualizing test data: {e}")

            

    def close_connection(self):
        """
        Close the database connection.

        Effects:
        - Closes the SQLite database connection if it is open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None    
    

        
        
class Test_studentPerformance_Functions(unittest.TestCase):
    """
    A class to test various functions related to student performance data processing.
    It includes methods to set up a test database, create test data, and test 
    different database operations and data processing functions.
    """
    @classmethod
    def setUpClass(cls):
        """
        Set up the class for testing.

        This method is called once at the beginning of the test suite to set up any necessary resources or configurations.

        Effects:
        - Creates an instance of the `studentPerformance` class for testing.
        - Sets up test data in an in-memory SQLite database.

        Attributes:
        - student_performance (studentPerformance): An instance of the `studentPerformance` class for testing.
        """
        cls.student_performance = studentPerformance(':memory:')
        cls.setup_test_data(cls.student_performance.conn)

        
        
    @classmethod
    def setup_test_data(cls, conn):
        """
        Set up test data in the SQLite database.

        This method is used to populate the test database with sample data for testing purposes.

        Parameters:
        - conn (sqlite3.Connection): The database connection object.

        Effects:
        - Creates a test table in the database.
        - Inserts sample data into the test table.
        """
        conn.execute("CREATE TABLE TestTable1 (ResearchId INT, Q1 REAL, Q2 REAL)")
        test_data = [(1, 80, 70), (2, 90, 85), (3, 75, 65)]
        conn.executemany("INSERT INTO TestTable1 (ResearchId, Q1, Q2) VALUES (?, ?, ?)", test_data)
        conn.commit()
        
        
      
    def test_get_max_grade(self):
        """
        Test the `get_max_grade` method.

        This method tests the functionality of retrieving the maximum grade from a specified table and column.

        Effects:
        - Calls the `get_max_grade` method with specific parameters.
        - Asserts that the returned maximum grade matches the expected value.
        """
        max_grade = self.student_performance.get_max_grade('TestTable1', 'Q1', self.student_performance.conn)
        self.assertEqual(max_grade, 90)
        

        
    def test_get_column_grade_and_average(self):
        """
        Test the `get_column_grade_and_average` method.

        This method tests the retrieval of a specific grade and the average grade from a database table.

        Effects:
        - Calls the `get_column_grade_and_average` method with specific parameters.
        - Asserts that the retrieved grade and average match the expected values.
        """
        grade, average = self.student_performance.get_column_grade_and_average
        (1, 'TestTable1', 'Q1', self.student_performance.conn)
        self.assertEqual(grade, 50)
        self.assertAlmostEqual(average, 75.0)

        
        
    def test_standardise_grades(self):
        """
        Test the `standardise_grades` method.

        This method tests the standardization of grades based on a maximum grade value.

        Effects:
        - Calls the `standardise_grades` method with specific parameters.
        - Asserts that the standardized grades match the expected values.
        """
        standardised_grade, standardised_average = self.student_performance.standardise_grades(50, 75, 100)  # Correct function name
        self.assertEqual(standardised_grade, 50.0)
        self.assertEqual(standardised_average, 75.0)
        
        
        
    def test_calculate_relative_performance(self):
        """
        Test the `calculate_relative_performance` method.

        This method tests the calculation of relative performance based on the grade and average.

        Effects:
        - Calls the `calculate_relative_performance` method with specific parameters.
        - Asserts that the calculated relative performance matches the expected value.
        """
        grade = 80
        average = 70
        expected_relative_performance = grade - average

        relative_performance = self.student_performance.calculate_relative_performance(grade, average)
        self.assertEqual(relative_performance, expected_relative_performance)
        
        
        
    def test_get_question_columns(self):
        """
        Test the `get_question_columns` method.

        This method tests the retrieval of column names starting with 'Q' from a table.

        Effects:
        - Calls the `get_question_columns` method with a specific table name.
        - Asserts that the retrieved column names match the expected values.
        """
        # Assuming 'TestTable' has columns 'Q1' and 'Q2'
        expected_columns = ['Q1', 'Q2']
        question_columns = self.student_performance.get_question_columns('TestTable1')
        self.assertEqual(question_columns, expected_columns)


        
    @patch('matplotlib.pyplot.figure')
    def test_setup_visualisation(self, mock_figure):
        """
        Test the `setup_visualisation` method.

        This method tests the setup of the visualization environment in matplotlib.

        Effects:
        - Calls the `setup_visualisation` method.
        - Asserts that the `figure` function in matplotlib is called with specific parameters.
        """
        self.student_performance.setup_visualisation()
        mock_figure.assert_called_with(figsize=(10, 6))
        

        
    @patch('matplotlib.pyplot.bar')
    @patch('matplotlib.pyplot.text')
    def test_add_bar_to_visualisation(self, mock_text, mock_bar):
        """
        Test the `add_bar_to_visualisation` method.

        This method tests the addition of a bar to a matplotlib visualization.

        Effects:
        - Calls the `add_bar_to_visualisation` method with specific parameters.
        - Asserts that the `bar` and `text` functions in matplotlib are called with specific parameters.
        """
        self.student_performance.add_bar_to_visualisation('Test Label', 50, 'blue', 1)
        mock_bar.assert_called_with('Test Label', 50, color='blue')
        mock_text.assert_called_with(1, 50, '50', ha='center', va='bottom')  # Adjust the expected call
        

        
    def test_retrieve_and_standardise_q_column_data(self):
        """
    Test the retrieval and standardization of data from the 'Q' column.

    This method is designed to validate the functionality of the data retrieval
    and standardization process for the column labeled 'Q' in a dataset. It checks
    whether the data extraction is accurate and if the standardization process
    conforms to the expected format and rules.

    Raises:
        AssertionError: If the test fails, indicating that the retrieval or 
                        standardization process does not work as expected.
    """
        try:
            max_grade = self.get_max_grade(table_name, q_column, connection)
            grade, average = self.get_column_grade_and_average(research_id, table_name, q_column, connection)
            return self.standardise_grades(grade, average, max_grade) if grade is not None else (None, None)
        except Exception as e:
            print(f"Error in data retrieval: {e}")
            return None, None
        
        
        
    @patch('matplotlib.pyplot.show')
    def test_visualise_performance(self, mock_show):
        """
        Test the `visualise_performance` method.

        This method tests the visualization of performance for a specific question.

        Effects:
        - Calls the `visualise_performance` method with specific parameters.
        - Asserts that the `show` function in matplotlib is called once.
        """
        self.student_performance.visualise_performance(1, 'TestTable', 'Q1', 80, 70)
        mock_show.assert_called_once()

        
        
    @patch('matplotlib.pyplot.show')
    def test_visualise_question_column(self, mock_show):
        """
        Test the `visualise_question_column` method.

        This method tests the visualization of performance for a specific question column.

        Effects:
        - Calls the `visualise_question_column` method with specific parameters.
        - Asserts that the `show` function in matplotlib is called once.
        """
        self.student_performance.visualise_question_column(1, 'TestTable', 'Q1')
        mock_show.assert_called_once()  
        
        

    @patch('matplotlib.pyplot.show')
    def test_visualise_test(self, mock_show):
        """
        Test the `visualise_test` method.

        This method tests the visualization of performance for a specific test.

        Effects:
        - Calls the `visualise_test` method with specific parameters.
        - Asserts that the `show` function in matplotlib is called.
        """
        self.student_performance.visualise_test(1, 'TestTable')
        mock_show.assert_called()

        
        
    @patch('matplotlib.pyplot.show')
    def test_visualise_all_tests(self, mock_show):
        """
        Test the `visualise_all_tests` method.

        This method tests the visualization of performance for all tests associated with a research ID.

        Effects:
        - Calls the `visualise_all_tests` method with specific parameters.
        - Asserts that the `show` function in matplotlib is called.
        """
        self.student_performance.visualise_all_tests(1)
        mock_show.assert_called() 
        
        
        
    @classmethod
    def tearDownClass(cls):
        """
        Clean up after all tests are done.
        
        Close the database connection after all tests are completed.
        This method is automatically called after all tests have been run.
        """
        cls.student_performance.close_connection()

        
       
def run_tests():
    """
    Run the test suite for all tests defined in the Test_studentPerformance_Functions class.
    This function initializes a test suite, adds all tests, and then runs them.
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite( Test_studentPerformance_Functions))
    runner = unittest.TextTestRunner()
    runner.run(suite)
    
      

def main(research_id, table_name):
    student_performance = studentPerformance('Resultdatabase.db')
    try:
        student_performance.visualise_test(research_id, table_name)
    except ValueError:
        print("Invalid input. Please enter a valid integer for ResearchId and a valid table name.")
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Unexpected error occurred:\n{error_trace}")
    finally:
        student_performance.close_connection()

        
        

if __name__ == "__main__":
    choice = input("Enter 'P' to run the program or 'T' to run tests: ").lower()
    if choice == 'p':
        research_id = int(input("Enter ResearchId: "))
        table_name = input("Enter table name: ")
        main(research_id, table_name)
    elif choice == 't':
        run_tests()


        
