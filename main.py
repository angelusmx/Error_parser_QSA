import sys
import os
import codecs
import csv
import MySQLdb
import time
from os import listdir
from os.path import isfile, join

reload(sys)
sys.setdefaultencoding('utf8')

# Location of the log files to be processed
logs_location = os.path.normpath('C:/Users/CanizalA/Desktop/logs')

# Create the connection to the database
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa")
cur = conn.cursor()


def test_connection():
    # Tests the connection to the MySQL DB
    # Retrieves the version of the DB as evidence of the connection

    try:
        cur.execute("SELECT VERSION()")
        results = cur.fetchone()
        # Check if anything at all is returned
        if results:
            print("Database version : %s"), results
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


def insert_mysql_ohne(date_stamp, time_stamp, meldung, month):
    # Execute the cursor function without committing the changes to the DB
    # the commit command is made at the end of the loop before calling looping to the next file

    table = "m2_" + month
    sql_command = "INSERT INTO " + table + " VALUES (%s, %s, %s)"

    try:
        cur.execute(sql_command, (date_stamp, time_stamp, meldung))

    except:
        conn.rollback()
        conn.close()

# ************** End of function ***************************


def commit_changes():
    conn.commit()

# ************ End of function ******************************


def parse_data(source, parsed_date):
    # Parse the information of the log file into the MySQL DB
    # Only the rows with text are taken into account

    # Start the time measurement
    start_time = time.time()
    counter_leer = 0

    with codecs.open(source, 'rU', encoding='UTF-8', errors='ignore') as the_source:

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
                    meldung = row[1]

                else:
                    time_stamp = old_time_stamp
                    meldung = row[0]

                # split the date in its time components
                year, month, day = parsed_date.split("-")

                # Insert the parsed info into MySQL
                insert_mysql_ohne(parsed_date, time_stamp, meldung, month)

            else:
                if counter_leer > 4:
                    print " ******* Reached EOF ********"

                    break
                else:
                    counter_leer += 1
                    continue

        # calculate the required time to finish the parsing
        elapsed_time = time.time() - start_time
        print "\n" + "Parsing of file " + str(source) + " completed in " + str(elapsed_time) + " seconds"
# ******************** END OF FUNCTION ***********************


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')

# ******************* END OF FUNCTION ************************


def slice_date():
    # Retrieve the names of the files in the input folder and extract the date from the file name

    # initialize the variables
    the_dates_list = []

    the_list = list_files(logs_location)
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

    table = "parsed files"
    sql_command = "INSERT INTO " + table + " VALUES (%s,)"

    try:
        cur.execute(sql_command, (path_of_files,))

    except:
        conn.rollback()
        conn.close()


def file_existence_check(file_2_parse):
    # Iterate through the list to see if the current file to be parsed has been processed already

    b_exists = 0
    sql_command = "SELECT * FROM parsed_files"

    try:
        cur.execute(sql_command)
        row = cur.fetchone()

        while row is not None:
            if file_2_parse != row:
                # If no match fetch a new row and search again
                b_exists = 0
                continue
            else:
                b_exists = 1
                print "File " + file_2_parse + " already parsed, file will be skipped"
                return b_exists

    except:
        conn.rollback()
        conn.close()


# ******************* Program control ***********************

# check whether the connection to the database is established
if test_connection():

    logs_list = list_files(logs_location)
    dates_list = slice_date()

    # Parse the data in the file
    for i, j in zip(logs_list, dates_list):
        path_file = logs_location + "\\" + i
        path_date = j

        # skip the file if it has been parsed already
        if not file_existence_check(path_file):

            # parse the row complete file
            parse_data(path_file, path_date)

            # commit the info held by the cursor to the DB
            commit_changes()

            # input the name of the file into the "parsed files" table
            parsed_files(path_file)

    print "\n" + "****** Process finished ******* "

