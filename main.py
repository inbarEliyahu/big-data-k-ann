
import csv, sqlite3
from queue import Queue


def main():
    #print('please enter your k: ')
    #k = input()
    numQI=4
    con = sqlite3.connect("adults.db")
    cur = con.cursor()
    file = open("adult.csv")
    rows = csv.reader(file)
    cur.execute("CREATE TABLE adult (martialStatus,relationship,race,gender);")
    cur.executemany("INSERT INTO adult VALUES (?, ?,?,?);", rows)
    cur.execute("CREATE TABLE martialStatusTable (value);")
    cur.execute("INSERT INTO martialStatusTable SELECT martialStatus FROM adult;")
    cur.execute("CREATE TABLE relationshipTable (value);")
    cur.execute("INSERT INTO relationshipTable SELECT relationship FROM adult;")
    cur.execute("CREATE TABLE raceTable (value);")
    cur.execute("INSERT INTO raceTable SELECT race FROM adult;")
    cur.execute("CREATE TABLE genderTable (value);")
    cur.execute("INSERT INTO genderTable SELECT gender FROM adult;")
    martialStatusHirarchy=[[["Divorced"],["Married-AF-spouse"],["Married-civ-spouse"],["Married-spouse-absent"],["Never-married"],["Separated"],["Widowed"]],[["Divorced","Married-AF-spouse"],["Married-civ-spouse","Married-spouse-absent"],["Never-married","Separated","Widowed"]],[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent"],["Never-married","Separated","Widowed"]],[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent","Never-married","Separated","Widowed"]]]
    relationshipHirerchy=[[["Husband"],["Not-in-family"],["Other-relative"],["Own-child"],["Unmarried"],["Wife"]],[["Husband","Not-in-family"],["Other-relative","Own-child"],["Unmarried","Wife"]],[["Husband","Not-in-family","Other-relative","Own-child"],["Unmarried","Wife"]],[["Husband","Not-in-family","Other-relative","Own-child","Unmarried","Wife"]]]
    raceHirarchy=[[["Amer-Indian-Eskimo"],["Asian-Pac-Islander"],["Black"],["Other"],["White"]],[["Amer-Indian-Eskimo","Asian-Pac-Islander"],["Black","Other"],["White"]],[["Amer-Indian-Eskimo","Asian-Pac-Islander"],["Black","Other","White"]],[["Amer-Indian-Eskimo","Asian-Pac-Islander","Black","Other","White"]]]
    genderHirarchy=[[["male"],["female"]],["male","female"]]
    cur.execute("SELECT COUNT(*) FROM adult GROUP BY race,gender;")#example for frequency set query
    cur.execute("CREATE TABLE C1 (ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1)")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('martialStatus', 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('martialStatus', 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('martialStatus', 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('martialStatus', 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('relationship', 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('relationship', 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('relationship', 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('relationship', 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('race', 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('race', 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('race', 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('race', 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('gender', 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES ('gender', 1 )")
    cur.execute("SELECT * FROM C1")
    cur.execute("CREATE TABLE E1 (Start,End)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (1,2)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (2,3)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (3,4)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (5,6)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (6,7)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (7,8)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (9,10)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (10,11)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (11,12)")
    cur.execute("INSERT INTO E1(Start,End) VALUES (13,14)")
    q = Queue(0)
    for i in range(numQI):
        if i==0:
            cur.execute("CREATE TABLE S1 AS SELECT * FROM C1")
        elif i==1:
            cur.execute("CREATE TABLE S2 AS SELECT * FROM C2")
        elif i==2:
            cur.execute("CREATE TABLE S3 AS SELECT * FROM C3")
        else:
            cur.execute("CREATE TABLE S4 AS SELECT * FROM C4")
        cur.execute("SELECT COUNT(*) FROM C1")
        numOfNodes=cur.fetchone()[0]
        cur.execute("SELECT END FROM E1")
        notRoots=cur.fetchall()
        notRootsList=[]
        for j in range(len(notRoots)):
            notRootsList.append(notRoots[j][0])
        cur.execute("SELECT * FROM C1 WHERE ID NOT IN (%s)" % ("?," * len(notRootsList))[:-1],notRootsList)
        roots=(cur.fetchall())
        print(roots)
        rootsAndHeights={}
        tupSize=len(roots[0])
        for curr in roots:
            height=0
            for j in range(int((tupSize/2)-1)):
                height=height+curr[(j+1)*2]
            rootsAndHeights.update({curr[0]:height})
        SortedByHeightRoots=sorted(rootsAndHeights, key=rootsAndHeights.get)
        print(SortedByHeightRoots)
        for curr in SortedByHeightRoots:
            q.put(curr)
        markedNodes=[]
        while not q.empty():
            node=q.get()
            marked=False
            if node in markedNodes:
                marked=True
            if not marked:
                if node in SortedByHeightRoots:
                    #compute frequency set






main()