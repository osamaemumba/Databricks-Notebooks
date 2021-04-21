import csv
with open(file_location) as csvfile:
    rows_read = csv.reader(csvfile, delimiter=',')
    for row in rows_read:
        print(row)
