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
from error_generalizer import match_strings

# Definition of the global variable

global last_ID
timestamp_last = "2015-01-01 00:00:00"


# Create the connection to the database
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

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


def test_connection():
    try:
        cur_test.execute("SELECT VERSION()")
        results = cur_test.fetchone()
        # Check if anything at all is returned
        if results:
            print("Database version : %s"), results
            cur_test.close()
            return True
        else:
            print "ERROR IN CONNECTION"
            return False
    except MySQLdb.Error:
        return False
# *************** End of function **********************


def parsed_tables(name_of_table):
    # TODO: discontinue this function, does not work when parsing individual files over time, works fine in one-shot
    # This function registers the files after the parsing of the content in the table "parsed tables"
    # in the MySQL table

    default_id = 0
    sql_command_insertion = "INSERT INTO parsed_tables VALUES (%s, %s)"

    try:
        cur_table_parsed.execute(sql_command_insertion, (default_id, name_of_table))
        conn.commit()

    except:
        conn.rollback()
        conn.close()

# *************** End of function **********************


def table_parsed_check(table_2_parse):
    # Iterate through the list to see if the current table to be parsed has been processed already

    b_exists = False
    sql_command_tables = "SELECT * FROM parsed_tables"

    try:
        cur.execute(sql_command_tables)
        tables = cur.fetchone()
        while tables is not None:
            if table_2_parse == tables['Table_name']:
                # If a match exist set the variable and exit
                b_exists = True
                print "The table " + table_2_parse + " was already parsed, table will be skipped"
                return b_exists
            else:
                # If a match does not exist, pull one more row and continue searching
                tables = cur.fetchone()
                b_exists = False
        print"The table " + table_2_parse + " has not been parsed before"
        return b_exists

    except:
        conn.rollback()
        conn.close()

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
            return True

        else:
            return False

# ************** End of function ***************************


# ************** Main function ****************************

def main_error_parser_M2(file_date):

    # Extract the month from the date of the file
    file_year, file_month, file_day = file_date.split('-')
    file_month = int(file_month)

    # initialize the completion variable to return at the end of the function
    error_parser_m2_complete = False

    # if month of the file > 9:
    if file_month > 9:
        # The name of the table to iterate through
        table = "m2_" + str(file_month)

    else:
        # The name of the table to iterate through
        table = "m2_0" + str(file_month)

    # Start the parsing process
    print "Parsing errors in file " + file_date

    # The SQL Queries
    search_string = '%Anweisung: Strung beseitigen%'
    sql_command = "SELECT ID FROM %s WHERE Meldung LIKE '%s' AND DATE(Datum) = '%s'" % (table, search_string, file_date)

    # Parameter 1 is the ID that resulted from the main query
    sql_command_previous = "SELECT * FROM " + table + " WHERE ID = (SELECT MAX(ID) FROM " + table + " WHERE ID < %s)"
    # TODO: I changed the sort from DESC to ASC, check that the effect on the subset of errors is correct
    sql_command_select_n_previous = "SELECT * FROM " + table + " WHERE ID < %s ORDER BY ID DESC LIMIT 250"
    sql_command_insert = "INSERT INTO errors_m2 VALUES (%s, %s, %s)"

    try:

        # Load the cursors with the values from the DB
        cur.execute(sql_command, )

        # Pull a row from the cursor
        row = cur.fetchone()

        entry_count = 0

        while row is not None:

            # Fetch the first line from the cursor
            # print "stop"
            id_row = row['ID']

            # ************** Uncomment and activate breakpoint to catch Meldung exceptions ******************
            # print ("I am Grooot!")
            # ***********************************************************************************************

            # Fetch the previous 15 rows, the amount is a magic number in the SQL Command
            cur_sub.execute(sql_command_select_n_previous, (id_row,))
            reasons = cur_sub.fetchall()

            for entry in reasons:
                # Loop through the list of general errors
                meldung_test = entry['Meldung']
                real_meldung = match_strings(meldung_test)

                # if the real meldung was found in the table break and store
                if real_meldung != "No match found":
                    break
                else:
                    # otherwise keep looking
                    continue

            if real_meldung == "No match found":
                entry_count = entry_count + 1
                print real_meldung + " " + str(entry_count)
                # print entry_count

            zeit = entry['Zeit']
            datum = entry['Datum']
            meldung = real_meldung
            # Meldung = reason['Meldung']
            default_id = 0

            # Build the timestamp from the fetched values
            time_stamp = create_time_stamp(datum, zeit)

            # Evaluate the delta t between the meldungen, if bigger as 1 second accept it
            # TODO: Replace the distance function with a string comparison: same line for stop and error message
            # TODO: If the file is broken (time lapses) the 20 lines of the subquery can be from way in the past
            if time_distance(time_stamp):

                # insert the result from the subquery with the time stamp in the errors table in the DB
                cur_sub.execute(sql_command_insert, (default_id, time_stamp, meldung))
                conn.commit()

            else:
                row = cur.fetchone()

    except:
            print "**** I have raised an exception ******"

    finally:
        # Output message of completion for the current file
        print"*** Data extraction for the file " + str(file_date) + " completed ***" + "\n"

    # close the connection when finished
    # cur.close()
    # cur_sub.close()
    # cur_test.close()
    # cur_sub_10.close()
    # cur_table_parsed.close()
    # conn.close()

    # Return completion signal to caller
    return error_parser_m2_complete

# ************** End of function ***************************