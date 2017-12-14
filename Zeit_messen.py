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


def calculate_time_delta(date_start, date_end, code):

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

    # The search strings
    search_string_takt_assembly = "CRobi::SendRcv Snd=\x02StartUmsetzen\x04"
    search_string_welding = '%' + 'DataRecorder 020 - POLSONO 4FACH' + '%'

    sql_command_welding = "SELECT Datum, Zeit FROM %s WHERE Meldung LIKE '%s' AND Datum BETWEEN '%s' AND '%s'" % \
                  (table, search_string_welding, date_start, date_end)

    sql_command_assembly = "SELECT Datum, Zeit, Meldung FROM %s WHERE Meldung LIKE '%s' AND Datum BETWEEN '%s' AND '%s'" % \
                  (table, search_string_takt_assembly, date_start, date_end)

    # initialize the list for loading the delta time values
    delta_time = []

    # open the csv File to write the data to
    with open('process_delta_t.csv', 'wb') as csvfile:

        # Create the writer object
        file_writer = csv.writer(csvfile, delimiter=';', dialect='excel-tab', quoting=csv.QUOTE_MINIMAL)
        # Insert the information regarding the search results
        file_writer.writerow(['Delta t between the processes', 'Start date = ', date_start, 'End date =', date_end])

        try:

            # Select the proper command depending on the station to be evaluated
            # 1 is the welding station
            if code == 1:

                # Load the cursors with the values from the DB
                cur.execute(sql_command_welding, )

                # Pull a row from the cursor
                results = cur.fetchall()

                # iterate through the results
                for index, element in enumerate(results):

                    # the first element of the list
                    if index < 1:
                        delta_time.append(0)

                    # all other subsequent cases
                    else:

                        # The previous entry
                        entry_time_previous = results[index-1]['Zeit']
                        entry_date_previous = str(results[index-1]['Datum'])

                        entry_time_previous = datetime.strptime(entry_time_previous, "%H:%M:%S.%f")
                        entry_date_previous = datetime.strptime(entry_date_previous, "%Y-%m-%d")

                        previous_time_stamp = datetime.combine(datetime.date(entry_date_previous), datetime.time(entry_time_previous))

                        # The current entry in the list
                        entry_time = element['Zeit']
                        entry_date = str(element['Datum'])

                        entry_time = datetime.strptime(entry_time, "%H:%M:%S.%f")
                        entry_date = datetime.strptime(entry_date, "%Y-%m-%d")

                        current_time_stamp = datetime.combine(datetime.date(entry_date), datetime.time(entry_time))

                        # substract the time between the entries and append to list
                        delta_value = current_time_stamp - previous_time_stamp
                        delta_time.append(delta_value)

                        # insert the new line into the csv file
                        file_writer.writerow([delta_value])

            # The assembly process
            if code == 2:

                # Load the cursors with the values from the DB
                cur.execute(sql_command_assembly, )

                # Pull a row from the cursor
                results = cur.fetchall()

                # iterate through the results
                for index, element in enumerate(results):

                    # the first element of the list
                    if index < 1:
                        delta_time.append(0)

                    # all other subsequent cases
                    else:

                        # The previous entry
                        entry_time_previous = results[index-1]['Zeit']
                        entry_date_previous = str(results[index-1]['Datum'])

                        entry_time_previous = datetime.strptime(entry_time_previous, "%H:%M:%S.%f")
                        entry_date_previous = datetime.strptime(entry_date_previous, "%Y-%m-%d")

                        previous_time_stamp = datetime.combine(datetime.date(entry_date_previous), datetime.time(entry_time_previous))

                        # The current entry in the list
                        entry_time = element['Zeit']
                        entry_date = str(element['Datum'])

                        entry_time = datetime.strptime(entry_time, "%H:%M:%S.%f")
                        entry_date = datetime.strptime(entry_date, "%Y-%m-%d")

                        current_time_stamp = datetime.combine(datetime.date(entry_date), datetime.time(entry_time))

                        # substract the time between the entries and append to list
                        delta_value = (current_time_stamp - previous_time_stamp).total_seconds()
                        delta_time.append(delta_value)

                        # insert the new line into the csv file
                        file_writer.writerow([entry_time, delta_value])
                        print ""

        finally:
                    # Export the list into a .csv file
                    # TODO: Implement the exporting of th results to CSV, before transpose the lists
                    # Output message of completion for the current file
                    print"*** Data extraction for the file " + " completed ***" + "\n"


def main():

    start_date = '20171211'
    end_date = '20171211'
    # TODO:The user enters here a number that corresponds to one of the possible stations coded
    # The Assembly station
    station_code = 2

    #start_date = raw_input("give the start date 'YYYYMMDD' ")
    #end_date = raw_input("give the end date 'YYYYMMDD' ")

    # check the connection to DB
    if test_connection():

        # call the calculate time delta function
        calculate_time_delta(start_date, end_date, station_code)

































