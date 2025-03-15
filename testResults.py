"""
testResults Module

This module provides functionalities for retrieving, processing, and visualizing test results 
from a database. It's designed to work with educational data, focusing on student grades across 
various tests. The module enables the extraction of grade data for individual students (identified 
by their research ID) and presents this data visually using bar plots.

Classes:
- testResults: The main class responsible for database operations, data retrieval, and visualization.

Key Functionalities:
- Retrieving grades for a specific research ID from a database.
- Visualizing test results using matplotlib for intuitive and informative representation.
- Supporting multiple test tables within a database for comprehensive data handling.
- Providing a user-friendly interface for viewing test results of students.

Usage:
The module can be used in educational data analysis scenarios where insights into student performance 
across various tests are needed. It supports both detailed analysis for individual students and 
aggregate views across different tests.

Example:
To visualize test results for a specific research ID, create an instance of the testResults class 
with a database path, and then call the run_visualisation method with the desired research ID.

Dependencies:
- DAFunction: A separate module for database-related operations.
- pandas: For data manipulation and retrieval.
- sqlite3: To interface with SQLite databases.
- matplotlib: For generating visualizations of the test results.
- numpy: For numerical operations.
- unittest: For writing and running tests of the module functionalities.


Reference: 23COP504_CW
Author: F333494
Version: 1
Date: 22/01/24
"""




from DAFunction import DAFunction
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np
import unittest
from unittest import mock
from unittest.mock import patch
import traceback



class testResults:
    
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
        try:
            self.conn = sqlite3.connect(db_path)
            self.da_function = DAFunction(db_path)  # Create an instance of DAFunction
        except sqlite3.Error as e:
            raise Exception(f"Failed to connect to the database: {e}")

            
            
    def retrieve_grades_for_table(self, research_id, table_name, connection):
        """
        Retrieve and return grades for a specific research ID from a specified table.

        Parameters:
        - research_id (int): The unique identifier for the research.
        - table_name (str): Name of the database table to retrieve grades from.
        - connection (sqlite3.Connection): Active database connection.

        Returns:
        - pandas.DataFrame: A DataFrame containing 'ResearchId' and 'Grade' from the specified table.
        """
        query = f"SELECT ResearchId, Grade FROM {table_name} WHERE ResearchId={research_id};"
        return pd.read_sql_query(query, connection)

    

    def retrieve_grades_by_research_id(self, research_id, connection):
        """
        Retrieve grades from all tables in the database for a specific research ID.

        Parameters:
        - research_id (int): The unique identifier for the research.
        - connection (sqlite3.Connection): Active database connection.

        Returns:
        - dict: A dictionary where each key is a table name and the value is a DataFrame of grades for that table.
        """
        results = {}
        tables = self.da_function.get_table_names(connection)
        for table_name in tables:
            results[table_name] = self.retrieve_grades_for_table(research_id, table_name, connection)
        return results

    

    def setup_visualisation(self):
        """
        Set up the initial parameters for a matplotlib plot.

        This method configures the figure size and prepares for subsequent plotting functions.
        """
        plt.figure(figsize=(10, 6))

        

    def add_bar_to_visualisation(self, table_name, grade, color, index):
        """
        Add a bar to the matplotlib plot.

        Parameters:
        - table_name (str): Name of the test table.
        - grade (float): The grade to be visualized.
        - color (str or tuple): Color of the bar in the plot.
        - index (int): Position index of the bar in the plot.
        """
        plt.bar(table_name, grade, color=color)
        plt.text(index, grade + 1, f"{grade}", ha='center', va='bottom')

        

    def finalise_visualisation(self, research_id):
        """
        Finalize and display the matplotlib visualisation.

        This function adds the finishing touches to the visualisation plot, such as the title, 
        labels for the x-axis and y-axis, and then displays the plot. It is typically called after 
        all the data has been added to the plot.

        Parameters:
        - research_id (int): The unique identifier of the research for which the data is being visualized.
        """
        plt.title(f"All Test Results For ResearchId {research_id}")
        plt.xlabel("Test Name")
        plt.ylabel("Percentage Grade")
        plt.show()

        

    def visualise_grades(self, research_id, data):
        """
        Visualise grades for a given research ID using matplotlib.

        This function creates a bar plot visualisation for the grades associated with a specific research ID.
        It sets up the visualisation, iterates through the provided data to add bars to the plot, and then 
        finalizes the visualisation for display.

        Parameters:
        - research_id (int): The unique identifier of the research for which the grades are being visualized.
        - data (dict): A dictionary containing table names as keys and corresponding DataFrames of grades as values.

        Returns:
        None

        Effects:
        - Initializes the plot with 'setup_visualisation()'.
        - Iterates through the provided data, adding a bar for each table's grade to the plot.
        - Finalizes and displays the plot using 'finalise_visualisation()'.
        """
        self.setup_visualisation()
        table_names = list(data.keys())
        colors = plt.cm.viridis(np.linspace(0, 1, len(table_names)))
        for i, (table_name, df) in enumerate(data.items()):
            grade = round(df['Grade'].iloc[0]) if not df.empty else 0
            self.add_bar_to_visualisation(table_name, grade, colors[i], i)
        self.finalise_visualisation(research_id)
        


    def run_visualisation(self, research_id=None):
        """
        Run the entire visualization process for a specific research ID.

        Parameters:
        - research_id (int, optional): The research ID for which to visualize test results. If not provided, 
                                       the user is prompted to enter it.

        Raises:
        - ValueError: If an invalid research ID is entered.
        - Exception: For any unexpected errors during the visualization process.
        """
        try:
            if research_id is None:
                research_id = int(input("Enter ResearchId: "))
            grades_by_research_id = self.retrieve_grades_by_research_id(research_id, self.conn)
            if any(not df.empty for df in grades_by_research_id.values()):
                self.visualise_grades(research_id, grades_by_research_id)
            else:
                print(f"No test results found for ResearchId {research_id} in the database.")

        except ValueError:
            print("Invalid ResearchId. Please enter a valid integer.")
        except Exception as e:
            print(f"Unexpected error occurred: {e}")

        # No need to close the connection here, it's managed by the __init__ method

        
        
    def close_connection(self):
        """
        Close the database connection.

        Effects:
        - Closes the SQLite database connection if it is open.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            
            

class Test_testResults_Functions(unittest.TestCase):
    """
    A test class for testing various database-related functions in the module.
    """
    def setUp(self):
        """
        Set up a temporary in-memory database for testing.
        This method is called before each test function execution.
        It sets up a database and creates test tables with sample data.
        """
        plt.close('all')  # Close all existing plots
        self.test_results = testResults(':memory:')  # Initialise testResults with in-memory DB
        self.conn = self.test_results.conn
        self.conn.execute('''CREATE TABLE TestTable1 (ResearchId INTEGER, Grade REAL);''')
        self.conn.execute('''CREATE TABLE TestTable2 (ResearchId INTEGER, Grade REAL);''')
        self.conn.execute('''INSERT INTO TestTable1 (ResearchId, Grade) VALUES (1, 85);''')
        self.conn.execute('''INSERT INTO TestTable2 (ResearchId, Grade) VALUES (1, 90);''')
        self.conn.commit()
        

        
    def test_retrieve_grades_by_research_id(self):
        """
        Test retrieving grades for a specific research ID from all tables in the database.
        This method verifies that the correct data is retrieved and returned in a dictionary.
        """
        grades = self.test_results.retrieve_grades_by_research_id(1, self.conn)
        self.assertIn('TestTable1', grades)
        self.assertIn('TestTable2', grades)
        self.assertEqual(grades['TestTable1']['Grade'].iloc[0], 85.0)
        self.assertEqual(grades['TestTable2']['Grade'].iloc[0], 90.0)

    
    
    def test_retrieve_grades_for_table(self):
        """
        Test the functionality of retrieving grades for a specific table.
        This method checks if the correct grades are retrieved for a specified table and research ID.
        """
        df = self.test_results.retrieve_grades_for_table(1, 'TestTable1', self.conn)
        self.assertEqual(df['Grade'].iloc[0], 85)
        

        
    @patch('matplotlib.pyplot.figure')
    def test_setup_visualisation(self, mock_figure):
        """
        Test setting up the visualisation.
        This method mocks the plt.figure function to verify that it is called with the correct size.
        """
        self.test_results.setup_visualisation()
        mock_figure.assert_called_with(figsize=(10, 6))
        
      
    
    @patch('matplotlib.pyplot.bar')
    @patch('matplotlib.pyplot.text')
    def test_add_bar_to_visualisation(self, mock_text, mock_bar):
        """
        Test adding a bar to the visualisation.
        This method mocks matplotlib functions to verify that a bar and its label are added correctly.
        """
        self.test_results.add_bar_to_visualisation("Test1", 85.0, 'blue', 0)
        mock_bar.assert_called_with("Test1", 85.0, color='blue')
        mock_text.assert_called_with(0, 86.0, '85.0', ha='center', va='bottom')
  
      
    
    @patch('matplotlib.pyplot.show')
    @patch('matplotlib.pyplot.ylabel')
    @patch('matplotlib.pyplot.xlabel')
    @patch('matplotlib.pyplot.title')
    def test_finalise_visualisation(self, mock_title, mock_xlabel, mock_ylabel, mock_show):
        """
        Test the functionality of finalising the visualisation.
        This method mocks matplotlib functions to verify that titles and labels are set correctly.
        """
        self.test_results.finalise_visualisation(1)
        mock_title.assert_called_with("All Test Results For ResearchId 1")
        mock_xlabel.assert_called_with("Test Name")
        mock_ylabel.assert_called_with("Percentage Grade")
        mock_show.assert_called_once()

        
        
    @patch('matplotlib.pyplot.show')
    def test_visualise_grades(self, mock_show):
        """
        Test the grade visualization function without displaying the actual plot.
        This method mocks the plt.show() function to test visualization logic without rendering the plot.
        """
        data = {
            'TestTable1': pd.DataFrame({'ResearchId': [1], 'Grade': [85]}),
            'TestTable2': pd.DataFrame({'ResearchId': [1], 'Grade': [90]})
        }
        self.test_results.visualise_grades(1, data)
        mock_show.assert_called_once()
        
        
        
        
    @patch('builtins.input', return_value='1')
    @patch('builtins.print')
    def test_run_visualisation(self, mock_print, mock_input):
        """
        Test the run_visualisation method.
        """
        with patch.object(self.test_results, 'retrieve_grades_by_research_id', return_value=
                          {'TestTable1': pd.DataFrame({'ResearchId': [1], 'Grade': [85]})}), \
             patch.object(self.test_results, 'visualise_grades') as mock_visualise_grades:

            self.test_results.run_visualisation()

            # Extract the actual call arguments
            args, kwargs = mock_visualise_grades.call_args

            self.test_results.retrieve_grades_by_research_id.assert_called_with(1, self.test_results.conn)

            expected_data = {'TestTable1': pd.DataFrame({'ResearchId': [1], 'Grade': [85]})}
            self.assertEqual(args[0], 1)  # Check the research_id argument
            pd.testing.assert_frame_equal(args[1]['TestTable1'], expected_data['TestTable1'])  # Check the DataFrame
            
            mock_print.assert_not_called()


   

    def tearDown(self):
        """
        Tear down method called after each test function execution.
        It closes the database connection and cleans up resources.
        """
        if self.conn:
            self.conn.close()
            self.conn = None


        
        
def run_tests():
    """
    Run the test suite for the 'TesttestResultsFunctions' class.
    This function initializes a test suite, adds all tests from the specified class,
    and runs them using a test runner.
    """
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(Test_testResults_Functions))
    runner = unittest.TextTestRunner()
    runner.run(suite)
        
        
        
def main(research_id=None):
    test_results = testResults('Resultdatabase.db')
    try:
        test_results.run_visualisation(research_id)
    except ValueError:
        print("Invalid ResearchId. Please enter a valid integer.")
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Unexpected error occurred:\n{error_trace}")
    finally:
        test_results.close_connection()

        
        
if __name__ == "__main__":
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

