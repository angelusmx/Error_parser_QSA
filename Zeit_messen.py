import MySQLdb
import MySQLdb.cursors
import csv
from MySQLdb.cursors import DictCursor
from datetime import datetime


# Create the connection to the database
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

# Create the cursor for the main query
cur = conn.cursor()
# Create the cursor for the connection test
cur_test = conn.cursor()


def test_connection():
    try:
        cur_test.execute("SELECT VERSION()")
        results = cur_test.fetchone()
        # Check if anything at all is returned
        if results:
            # print("Database version : %s"), results
            cur_test.close()
            return True
        else:
            print "ERROR IN CONNECTION"
            return False
    except MySQLdb.Error:
        return False
# *************** End of function **********************


def calculate_time_delta(date_start, date_end):

    # Extract the month from the date of the file
    file_datetime = datetime.strptime(date_start, '%Y%m%d')
    file_month = file_datetime.month

    # if month of the file > 9:
    if file_month > 9:
        # The name of the table to iterate through
        table = "m2_" + str(file_month)

    else:
        # The name of the table to iterate through
        table = "m2_0" + str(file_month)

    search_string_std = '%' + 'DataRecorder 020 - POLSONO 4FACH' + '%'
    sql_command = "SELECT Datum, Zeit FROM %s WHERE Meldung LIKE '%s' AND Datum BETWEEN '%s' AND '%s'" % \
                  (table, search_string_std, date_start, date_end)

    # initialize the list for loading the delta time values
    delta_time = []

    try:
        # Load the cursors with the values from the DB
        cur.execute(sql_command, )

        # Pull a row from the cursor
        results = cur.fetchall()

        # iterate through the results
        for index, element in enumerate(results):

            # the first element of the list
            if index < 1:
                delta_time[index] = 0

            # all other subsequent cases
            else:

                # The previous entry
                entry_time_previous = results[index-1]['Zeit']
                entry_date_previous = element[index-1]['Datum']

                entry_time_previous = datetime.strptime(entry_time_previous, "%H:%M:%S.%f")
                entry_date_previous = datetime.strptime(entry_date_previous, "%Y-%m-%d")

                previous_time_stamp = datetime.combine(datetime.date(entry_date_previous), datetime.time(entry_time_previous))

                # The current entry in the list
                entry_time = element['Zeit']
                entry_date = element['Datum']

                entry_time = datetime.strptime(entry_time, "%H:%M:%S.%f")
                entry_date = datetime.strptime(entry_date, "%Y-%m-%d")

                current_time_stamp = datetime.combine(datetime.date(entry_date), datetime.time(entry_time))

                # substract the time between the entries

                delta_time[index] = current_time_stamp - previous_time_stamp
    finally:
                # Export the list into a .csv file
                # TODO: Implement the exporting of th results to CSV, before transpose the lists
                # Output message of completion for the current file
                print"*** Data extraction for the file " + " completed ***" + "\n"


start_date = raw_input("give the start date 'YYYYMMDD' ")
end_date = raw_input("give the end date 'YYYYMMDD' ")

# check the connection to DB
if test_connection():

    # call the calculate time delta function
    calculate_time_delta(start_date, end_date)





































