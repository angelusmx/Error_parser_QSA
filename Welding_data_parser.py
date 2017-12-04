import MySQLdb
import MySQLdb.cursors
from MySQLdb.cursors import DictCursor
from error_parser_M2 import create_time_stamp

# Create the connection to the DB
conn = MySQLdb.connect(host="localhost", user="root", passwd="Midvieditza12!", db="logs_qsa",
                       cursorclass=DictCursor)

# Create the cursor for the main query
cur_main = conn.cursor()
# Create the cursor for inserting the welding values
cur_weld = conn.cursor()


def test_connection():
    # Retrieves the version of the DB as evidence of the connection
    try:
        cur_main.execute("SELECT VERSION()")
        results = cur_main.fetchone()
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


def purify_welding_data(max_month):

    for i in range(1, max_month):

        if max_month > 9:
            # The name of the table to iterate through
            table = "m2_" + str(i)

        else:
            # The name of the table to iterate through
            table = "m2_0" + str(i)

        print "**** Parsing welding data in table " + table + "*******"

        # The SQL Queries
        search_command = "SELECT * FROM " + table + " WHERE Meldung LIKE %s OR Meldung LIKE %s"
        search_string = '%' + 'DataRecorder 020 - 4KW TEST' + '%'
        search_string_std = '%' + 'DataRecorder 020 - POLSONO 4FACH' + '%'

        # Cycle through the tables containing the data of every month
        try:
            # Execute the SQL command
            cur_main.execute(search_command, (search_string, search_string_std))

            counter = 1

            # Fetch the first line from the cursor
            row = cur_main.fetchone()

            while row is not None:
                # print row
                meldung = row['Meldung']
                # replace the comma for the dot as decimal separator
                meldung = meldung.replace(',', '.')
                # this variable contains the list of
                meldung_split = meldung.split(';')

                # Populate a dictionary to hold the variables
                # TODO: This is a desperate method, ideally pass the list (as tuple) to MySQL, so far no luck with that
                welding_vars = {}
                for x in range(0, 45):
                    welding_vars["var{0}".format(x)] = 0

                # for key, value in welding_vars.iteritems():
                #     try:
                #         welding_vars[key] = int(value)
                #     except ValueError:
                #         welding_vars[key] = str(value)

                for y in range(0, 45):
                    var_name = "var" + str(y)
                    try:
                        welding_vars[var_name] = float(meldung_split[y])
                    except ValueError:
                        welding_vars[var_name] = str(meldung_split[y])

                # The date time variables from the row
                zeit = row['Zeit']
                datum = row['Datum']

                # for the insert into MySQL function. The ID is set in the DB to Auto increase
                default_ID = 0

                # Build the timestamp from the fetched values
                time_stamp = create_time_stamp(datum, zeit)

                # SQL Insert command
                insert_command = "INSERT INTO welding_data VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                 " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

                # insert the result from the subquery with the time stamp in the errors table in the DB
                cur_weld.execute(insert_command, (default_ID, time_stamp, welding_vars['var0'], welding_vars['var1'],
                                                  welding_vars['var2'], welding_vars['var3'], welding_vars['var4'],
                                                  welding_vars['var5'], welding_vars['var6'], welding_vars['var7'],
                                                  welding_vars['var8'], welding_vars['var9'], welding_vars['var10'],
                                                  welding_vars['var11'], welding_vars['var12'], welding_vars['var13'],
                                                  welding_vars['var14'], welding_vars['var15'], welding_vars['var16'],
                                                  welding_vars['var17'], welding_vars['var18'], welding_vars['var19'],
                                                  welding_vars['var20'], welding_vars['var21'], welding_vars['var22'],
                                                  welding_vars['var23'], welding_vars['var24'], welding_vars['var25'],
                                                  welding_vars['var26'], welding_vars['var27'], welding_vars['var28'],
                                                  welding_vars['var29'], welding_vars['var30'], welding_vars['var31'],
                                                  welding_vars['var32'], welding_vars['var33'], welding_vars['var34'],
                                                  welding_vars['var35'], welding_vars['var36'], welding_vars['var37'],
                                                  welding_vars['var38'], welding_vars['var39'], welding_vars['var40'],
                                                  welding_vars['var41'], welding_vars['var42'], welding_vars['var43'],
                                                  welding_vars['var44']))
                conn.commit()
                print "process number " + str(counter)
                counter += 1

                # Fetch a new line from the DB
                row = cur_main.fetchone()

        except:
            print "Error: unable to fetch data"

        finally:
            # Output message of completion for the current table
            print"*** Data extraction for the table " + table + " completed ***"
            # Input the recently completed table in the DB

    # Change the value of the variable to reflect completion
    welding_parser_complete = True

    # Return completion signal to caller
    return welding_parser_complete
