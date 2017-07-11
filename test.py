import os
import codecs
import csv

path = os.path.normpath('C:/Users/Canizala/Desktop/test_csv.csv')
print path
counter_leer = 0

with codecs.open(path, 'r', encoding='1252') as the_file:
    while True:
        line = csv.reader(the_file, delimiter='\t')
        # the end of the file has been found
        if counter_leer > 5:
            # leave the loop
            print("EOF Found")
            break

        else:
            if line == "":
                counter_leer += 1
            else:
                counter_leer = 0

            print(line)
