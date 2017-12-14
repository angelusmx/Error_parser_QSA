# **********************************************************************************************
# Find in the DB the row that contains "F4 Grundstellung starten"
# catch the ID in the DB and use it to iterate one row before
# The previous row to the  message "F4 Grundstellung starten" contains in most cases the cause
# of the error (not in all cases true)
# the reason is then isolated and copied to the table "errors"
# the structure of the table is "ID", "Zeit Stamp", "error meldung"
# *********************************************************************************************

import MySQLdb
import MySQLdb.cursors
from MySQLdb.cursors import DictCursor
import datetime as dt
from datetime import datetime

# Definition of the global variable

global last_ID
timestamp_last = "2015-01-01 00:00:00"

# Create the connection to the database
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

# Create the cursor for the main query
cur_m1 = conn.cursor()
# Create the cursor for the subquery
cur_sub_m1 = conn.cursor()
# Create the cursor for the connection test
cur_test_m1 = conn.cursor()
# The cursor to write the parsed file to the "parsed_files_raw" table
cur_raw_parsed = conn.cursor()
# The cursor to read the file  "parsed_files_raw"
cur_parsed_errors = conn.cursor()


def test_connection():
    try:
        cur_test_m1.execute("SELECT VERSION()")
        results = cur_test_m1.fetchone()
        # Check if anything at all is returned
        if results:
            print("Database version : %s"), results
            print "I reached the test connection of Module 1"
            cur_test_m1.close()
            return True
        else:
            print "ERROR IN CONNECTION"
            return False
    except MySQLdb.Error:
        return False
# *************** End of function **********************


def extract_names_raw_parsed():
    # This function retrieves the names of the files already parsed for raw data

    # Initialize the variable
    file_names =[]

    sql_command_retrieve = "SELECT File_name FROM parsed_files_raw WHERE Module = 2"

    try:
        cur_raw_parsed.execute(sql_command_retrieve,)

    except:
        print "Error: Could not retrieve rows from the table"
        conn.rollback()
        conn.close()

    # Load the variable with all the content of the cursor
    raw_files_list = cur_raw_parsed.fetchall()

    # loop through the list and keep the module 2 files
    for element in raw_files_list:
        string_element = str(element['File_name'])
        file_names.append(string_element[-12:-4])

    return file_names

# *************** End of function **********************


def create_time_stamp(datum, zeit):
    # if the time difference from one Meldung to the next ist smaller than 10s then discard it
    # memorize the last time string, calculate the delta, set or reset the flag

    # Parse the time string from the calling function into a datetime object
    the_time = dt.datetime.strptime(zeit, '%H:%M:%S.%f').time()

    # Put the date and time together in a string
    time_stamp_ = dt.datetime.combine(datum, the_time)

    return time_stamp_
# ************** End of function ***************************


def time_distance(timestamp_current):
    # Determine the delta t of the Meldungen

    # define the global variable so that the value does not die with the function
    global timestamp_last
    global timestamp_now

    timestamp_now = timestamp_current

    # the first entry in the table does not have any comparison point
    if timestamp_last == "2015-01-01 00:00:00":

        # overwrite the value with the current timestamp from caller
        timestamp_last = timestamp_now

        # Accept the meldung and return to caller
        return True

    else:

        # calculate the delta_t
        delta_t = timestamp_now - timestamp_last

        # overwrite the value with the current timestamp from caller
        timestamp_last = timestamp_now

        # if the delta t is bigger than 2 second
        if delta_t >= dt.timedelta(0, 2, 0, 0, 0, 0, 0):
            # print delta_t
            return True

        else:
            return False

# ************** End of function ***************************


def parsed_files(path_of_files, machine_module):
    # This function registers the files after the parsing of the content in the table "parsed files"
    # in the MySQL table

    default_id = 0
    sql_command = "INSERT INTO parsed_files_errors VALUES (%s, %s, %s)"

    try:
        cur_parsed_errors.execute(sql_command, (default_id, path_of_files, machine_module))
        conn.commit()

    except:
        conn.rollback()
        conn.close()


# ************** Main function ****************************

def main_error_parser_M1():

    # initialize the completion variable to return at the end of the function
    error_parser_m1_complete = False

    # The current machine module
    machine_module = 1

    # extract all the files that have been already parsed for raw data
    already_raw_parsed = extract_names_raw_parsed()

    # Iterate through the list containing the already raw parsed files
    for data_point in already_raw_parsed:
        # Extract the month from the date of the file
        file_datetime = datetime.strptime(data_point, '%Y%m%d')
        file_month = file_datetime.month

        if file_month > 9:
            # The name of the table to iterate through
            table = "m1_" + str(file_month)

        else:
            # The name of the table to iterate through
            table = "m1_0" + str(file_month)

        print "Parsing errors in file " + data_point

        # The SQL Queries

        command_select_errors = "SELECT * FROM " + table + " WHERE Meldung LIKE %s"
        command_select_string = '%' + 'MESInterface_SetStatusDirect_Bool, MES_STOER_MODUL1_' + '%'
        command_insert = "INSERT INTO errors_m1 VALUES (%s, %s, %s)"

        # Cycle through the tables containing the data of every month
        try:
            # Execute the SQL command to find the errors based on command_select_string
            cur_m1.execute(command_select_errors, (command_select_string,))

            # Fetch the first line from the cursor
            row = cur_m1.fetchone()

            while row is not None:

                # Split the strings based after the comas
                meldung_in_row = row['Meldung']
                meldung_split = meldung_in_row.split(',')

                # If Meldung True then evaluate it, otherwise discard it
                # Note to self: there is a blank space before the word
                if meldung_split[2] == " TRUE":
                    Zeit = row['Zeit']
                    Datum = row['Datum']

                    # Separate the station name from the complete meldung string
                    root_cause_split = meldung_split[1].split('_')
                    # Store the size of the split string, some meldungen have a sub category
                    length_root_cause = len(root_cause_split)

                    # If there is a sub category
                    if length_root_cause > 4:
                        root_cause_text = root_cause_split[3] + " " + root_cause_split[4]

                    # If no subcategory
                    else:
                        # Example: 'MESInterface_SetStatusDirect_Bool', ' MES_STOER_MODUL1_BEFUELLEN', ' TRUE'
                        root_cause_text = root_cause_split[3]

                    # ID increments automatically, as setup in the DB
                    default_ID = 0

                    # Build the timestamp from the fetched values
                    time_stamp = create_time_stamp(Datum, Zeit)

                    # Evaluate the delta t between the meldungen, if bigger as 1 second accept it
                    if time_distance(time_stamp):
                        # insert the result from the subquery with the time stamp in the errors table in the DB
                        cur_sub_m1.execute(command_insert, (default_ID, time_stamp, root_cause_text))
                        conn.commit()
                    else:
                        row = cur_m1.fetchone()

                # if the word "TRUE" is not found after splitting the string, pull another row
                else:
                    row = cur_m1.fetchone()
        except:
            print "Error: unable to fetch data"

        finally:
            # Enter the name of the error parsed file in the table
            parsed_files(data_point, machine_module)

            # Output message of completion for the current table
            print"*** Data extraction for the file " + str(data_point) + " completed ***" + "\n"

    # close the cursors and the connection to DB
    cur_m1.close()
    cur_sub_m1.close()
    cur_test_m1.close()
    cur_raw_parsed.close()
    cur_parsed_errors.close()

    error_parser_m1_complete = True

    # Return completion signal to caller
    return error_parser_m1_complete

    # ************** End of function ***************************
