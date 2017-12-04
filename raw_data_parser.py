import sys
import os
import codecs
import csv
import MySQLdb
import time
from os import listdir
from os.path import isfile, join
from MySQLdb.cursors import DictCursor

from error_parser_M2 import main_error_parser_M2
from error_parser_M1 import main_error_parser_M1
from Welding_data_parser import purify_welding_data

reload(sys)
sys.setdefaultencoding('utf8')

# Create the connection to the database
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

# The cursor to query the "parsed_files" table
cur_raw = conn.cursor()
# The cursor to query the current log file
cur_log = conn.cursor()
# The cursor to write the parsed file to the "parsed_files" table
cur_parsed = conn.cursor()


# ************ The cursors of Module 2 ************
# Create the cursor for the main query
cur = conn.cursor()
# Create the cursor for the subquery
cur_sub = conn.cursor()
# Create the cursor for the connection test
cur_test = conn.cursor()
# Create the cursor for the 10 row subset
cur_sub_10 = conn.cursor()
# The cursor to write the parsed file to the "parsed_files" table
cur_table_parsed = conn.cursor()
# *************************************************

# ************ The cursors of Module 1 ************
# Create the cursor for the main query
cur_m1 = conn.cursor()
# Create the cursor for the subquery
cur_sub_m1 = conn.cursor()
# Create the cursor for the connection test
cur_test_m1 = conn.cursor()
# *************************************************


def test_connection():
    # Tests the connection to the MySQL DB
    # Retrieves the version of the DB as evidence of the connection

    try:
        cur_raw.execute("SELECT VERSION()")
        results = cur_raw.fetchone()
        # Check if anything at all is returned
        if results:
            # print("Database version : %s"), results
            # print "\n"
            return True
        else:
            print "ERROR IN CONNECTION"
            return False
    except MySQLdb.Error:
        return False

        # *************** End of function **********************


def list_files(logs):
    # Extracts the name of the files found in the given path
    onlyfiles = [f for f in listdir(logs) if isfile(join(logs, f))]
    return onlyfiles
    # ************** END of function ***********************


def exception_catcher():
    print"I have found an error"
    # ************** END of function ***********************


def insert_mysql_ohne(meldungen_list_import,  month, machine_module):
    # Execute the cursor function without committing the changes to the DB
    # the commit command is made at the end of the loop before calling looping to the next file

    chosen_module = "m" + str(machine_module) + "_"

    table = chosen_module + month
    sql_command = "INSERT INTO " + table + " VALUES (%s, %s, %s, %s)"

    try:
        cur_log.executemany(sql_command, meldungen_list_import)
        conn.commit()

    except:
        conn.rollback()
        conn.close()

# ************** End of function ***************************


def commit_changes():
    conn.commit()
# ************ End of function ******************************


def parse_data(source, parsed_date, machine_module):
    # Parse the information of the log file into the MySQL DB
    # Only the rows with text are taken into account

    # Start the time measurement
    start_time = time.time()
    counter_leer = 0

    # Initialize the ID value, the ID is set to AI in MySQL, something needs to be passed though
    default_id = 0

    # Define the number of rows to be parsed before the commit operation
    rows_per_loop = 1000

    with codecs.open(source, 'rb', encoding='UTF-8', errors='ignore') as the_source:

        reader = csv.reader(the_source, delimiter='\t', dialect='excel-tab')

        # Initialize counter of parsed rows
        row_counter = 0

        for row in reader:
            if row:
                counter_leer = 0
                # output the current amount of parsed strings to the console
                row_counter += 1
                # print "So far " + str(row_counter) + " rows parsed"

                # check the length of the list
                length_row = len(row)

                if length_row > 1:
                    old_time_stamp = row[0]
                    time_stamp = row[0]

                    # Differentiate the Welding Data from the general meldung
                    if length_row >= 40:
                        meldung_temp = row[1:length_row]
                        meldung = ";".join(str(x) for x in meldung_temp)

                    else:
                        # General case for the short Meldung texts
                        meldung = row[1]

                else:
                    time_stamp = old_time_stamp
                    meldung = row[0]

                # split the date in its time components
                year, month, day = parsed_date.split("-")

                # Create the list for the first parsed row
                if row_counter < 2:
                    meldungen_list = [(default_id, parsed_date, time_stamp, meldung)]
                else:
                    # extend the list for the subsequent parsed rows
                    meldungen_list.append((default_id, parsed_date, time_stamp, meldung))

                # Insert the parsed info into MySQL when "rows_per_loop" lines are collected
                if row_counter % rows_per_loop == 0:
                    insert_mysql_ohne(meldungen_list, month, machine_module)

                    # Reset the row counter
                    row_counter = 0

                else:
                    continue
            else:
                if counter_leer > 4:
                    print " ******* Reached EOF ******** "

                    break
                else:
                    counter_leer += 1
                    continue

        # Check If there are parsed lines below 500 that were not inserted above
        # Here "incomplete" sets will be loaded to the DB
        if 0 < row_counter < rows_per_loop:
            insert_mysql_ohne(meldungen_list, month, machine_module)

        # calculate the required time to finish the parsing
        elapsed_time = time.time() - start_time
        print "Parsing of file " + str(source) + " completed in " + str(elapsed_time) + " seconds"

# ******************** END OF FUNCTION ***********************


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')


# ******************* END OF FUNCTION ************************


def slice_date(logs_path):
    # Retrieve the names of the files in the input folder and extract the date from the file name

    # initialize the variables
    the_dates_list = []

    the_list = list_files(logs_path)
    for entry in the_list:
        year = entry[0:4]
        month = entry[4:6]
        day = entry[6:8]
        joined_date = year + "-" + month + "-" + day

        the_dates_list.extend([joined_date])
    return the_dates_list


def parsed_files(path_of_files):
    # This function registers the files after the parsing of the content in the table "parsed files"
    # in the MySQL table

    default_id = 0
    sql_command = "INSERT INTO parsed_files VALUES (%s, %s)"

    try:
        cur_parsed.execute(sql_command, (default_id, path_of_files))
        conn.commit()

    except:
        conn.rollback()
        conn.close()


def file_existence_check(file_2_parse):
    # Iterate through the list to see if the current file to be parsed has been processed already

    b_exists = False
    sql_command = "SELECT * FROM parsed_files"

    try:
        cur_raw.execute(sql_command)
        row = cur_raw.fetchone()
        while row is not None:
            if file_2_parse == row['File_name']:
                # If a match exist set the variable and exit
                b_exists = True
                print "File " + file_2_parse + " already parsed, file will be skipped"
                return b_exists
            else:
                # If a match does not exist, pull one more row and continue searching
                row = cur_raw.fetchone()
                b_exists = False
        print"The File " + file_2_parse + " has not been parsed before"
        return b_exists

    except:
        conn.rollback()
        conn.close()


# ******************* Main function ***********************

def main_function(logs_path, machine_module, welding_data):

    # define the boolean variable to return at the end of the function
    raw_parsing_complete = False

    # check whether the connection to the database is established
    if test_connection():

        # Create a list with the names files to be parsed and their dates
        logs_list = list_files(logs_path)
        dates_list = slice_date(logs_path)

        # loop through the list containing the file names and parse the data in each one
        for i, j in zip(logs_list, dates_list):
            path_file = logs_path + "\\" + i
            path_date = j

            # skip the file if it has been parsed already
            if not file_existence_check(path_file):
                # parse the raw file
                parse_data(path_file, path_date, machine_module)

                # call the error parsing function after the raw data has been parsed
                # check the corresponding error parser (Module 1 or Module 2)
                if machine_module == 2:
                    # Run the error parser function
                    main_error_parser_M2(path_date)
                    # Run the welding parser function if activated
                    if welding_data:
                        # if a positive result is returned by the function
                        if purify_welding_data(path_date):
                            print "*** Welding data for the file " + path_date + " completed ***"
                        else:
                            print "++++ Welding data could not be fetched ++++ "

                elif machine_module == 1:
                    main_error_parser_M1(path_date)
                else:
                    print "Invalid Module number found"

                # input the name of the file into the "parsed files" table
                parsed_files(path_file)

    # Update the value of the variable to report completion to caller
    raw_parsing_complete = True

    return raw_parsing_complete
# ******************** END OF FUNCTION **********************
