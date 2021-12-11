import csv
import sqlite3


def main():
    con = sqlite3.connect("adults.db")
    cur = con.cursor()
    file = open("adult.csv")
    rows = csv.reader(file)
    cur.execute("CREATE TABLE adult (age);")
    cur.executemany("INSERT INTO adult VALUES (?, ?)", rows)
    cur.execute("SELECT * FROM data")
    print(cur.fetchall())

    print('please enter your k: ')
    k = input()

