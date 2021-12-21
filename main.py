
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
    cur.executemany("INSERT INTO adult VALUES (?,?,?,?);", rows)
    cur.execute("CREATE TABLE martialStatusTable (value);")
    cur.execute("INSERT INTO martialStatusTable SELECT martialStatus FROM adult;")
    cur.execute("CREATE TABLE relationshipTable (value);")
    cur.execute("INSERT INTO relationshipTable SELECT relationship FROM adult;")
    cur.execute("CREATE TABLE raceTable (value);")
    cur.execute("INSERT INTO raceTable SELECT race FROM adult;")
    cur.execute("CREATE TABLE genderTable (value);")
    cur.execute("INSERT INTO genderTable SELECT gender FROM adult;")
    martialStatusHirarchy=[[[["Divorced"],"Divorced"],[["Married-AF-spouse"],"Married-AF-spouse"],[["Married-civ-spouse"],"Married-civ-spouse"],[["Married-spouse-absent"],"Married-spouse-absent"],[["Never-married"],"Never-married"],[["Separated"],"Separated"],[["Widowed"],"Widowed"]],[[["Divorced","Married-AF-spouse"],"divorcedOrAFSpouse"],[["Married-civ-spouse","Married-spouse-absent"],"CIVSpouseOrSpouseAbsent"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent"],"notSeperatedWidowedNever"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent","Never-married","Separated","Widowed"],"allMartialStatus"]]]
    relationshipHirarchy=[[[["Husband"],"Husband"],[["Not-in-family"],"Not-in-family"],[["Other-relative"],"Other-relative"],[["Own-child"],"Own-child"],[["Unmarried"],"Unmarried"],[["Wife"],"Wife"]],[[["Husband","Not-in-family"],"husbandOrNotIn"],[["Other-relative","Own-child"],"otherOrOwnChild"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child"],"notUnmarriedOrWife"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child","Unmarried","Wife"],"allRelationship"]]]
    raceHirarchy=[[[["Amer-Indian-Eskimo"],"Amer-Indian-Eskimo"],[["Asian-Pac-Islander"],"Asian-Pac-Islander"],[["Black"],"Black"],[["Other"],"Other"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other"],"blackOther"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other","White"],"blackWhiteOther"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander","Black","Other","White"],"allRaces"]]]
    genderHirarchy=[[[["male"],"male"],[["female"],"female"]],[["male","female"],"allGender"]]
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
        nodesTable="C" + str(i+1)
        replicaTable="S" + str(i+1)
        edgesTable="E" + str(i+1)
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s" %(replicaTable,nodesTable))#create the replica table
        cur.execute("SELECT COUNT(*) FROM C1")#to get number of nodes
        numOfNodes=cur.fetchone()[0]
        cur.execute("SELECT END FROM E1")#to get nodes which are not roots
        notRoots=cur.fetchall()
        notRootsList=[]
        for j in range(len(notRoots)):
            notRootsList.append(notRoots[j][0])
        cur.execute("SELECT * FROM C1 WHERE ID NOT IN (%s)" % ("?," * len(notRootsList))[:-1],notRootsList)#the list of roots
        roots=(cur.fetchall())
        nodesAndHeights={}
        tupSize=len(roots[0])#size of nodes table
        for curr in roots:
            height=0
            for j in range(int((tupSize/2)-1)):
                height=height+curr[(j+1)*2]#height is the sum of the indexes in the nodes table
            nodesAndHeights.update({curr[0]:height})
        SortedByHeightNodes=sorted(nodesAndHeights, key=nodesAndHeights.get)#sort by height
        for curr in SortedByHeightNodes:
            q.put(curr)#inserted sorted by height
        markedNodes=[]
        while not q.empty():
            node=q.get()
            marked=False
            if node in markedNodes:
                marked=True
            if not marked:
                if node in SortedByHeightNodes:#if node is a root
                    nodeStr= str(node)
                    cur.execute("SELECT * FROM C1 WHERE ID=?", [nodeStr])#get the tuple of the current node
                    nodeTuple = cur.fetchone()
                    whatToSelect = nodeTuple[1]#for the query in line 97
                    for k in range(i):
                        for j in range(len(nodeTuple)-2):
                            if j%2==1:
                                whatToSelect=whatToSelect+","+nodeTuple[j+2]
                    cur.execute("CREATE TABLE generate AS SELECT %s FROM adult" % whatToSelect)#generalization table
                    for k in range(i + 1):
                        generalizationString = nodeTuple[2*k+1] + "Hirarchy"
                        generalization = locals()[generalizationString][nodeTuple[2*k+2]]#used locals to get to variable name via string
                        cur.execute("UPDATE generate SET %s=? WHERE %s=?" % (nodeTuple[2*k+1], nodeTuple[2*k+1]),[generalization[i][1], generalization[i][0][0]])#generalizing the table
                    cur.execute("SELECT COUNT(*) FROM generate GROUP BY %s ;" % nodeTuple[1])#get the frequency set
                    freqset = cur.fetchall()
                    cur.execute("DROP TABLE generate")
                else:
                   # need to change this and get freqSet using roolup
                   nodeStr = str(node)
                   cur.execute("SELECT * FROM C1 WHERE ID=?", [nodeStr])  # get the tuple of the current node
                   nodeTuple = cur.fetchone()
                   whatToSelect = nodeTuple[1]  # for the query in line 97
                   for k in range(i):
                       for j in range(len(nodeTuple) - 2):
                           if j % 2 == 1:
                               whatToSelect = whatToSelect + "," + nodeTuple[j + 2]
                   cur.execute("CREATE TABLE generate AS SELECT %s FROM adult" % whatToSelect)  # generalization table
                   for k in range(i + 1):
                       generalizationString = nodeTuple[2 * k + 1] + "Hirarchy"
                       generalization = locals()[generalizationString][
                           nodeTuple[2 * k + 2]]  # used locals to get to variable name via string
                       cur.execute("UPDATE generate SET %s=? WHERE %s=?" % (nodeTuple[2 * k + 1], nodeTuple[2 * k + 1]),
                                   [generalization[i][1], generalization[i][0][0]])  # generalizing the table
                   cur.execute("SELECT COUNT(*) FROM generate GROUP BY %s ;" % nodeTuple[1])  # get the frequency set
                   freqset = cur.fetchall()
                   cur.execute("DROP TABLE generate")
                isKanonymazation=True
                for currFreq in freqset:#check k-anonymity
                    if currFreq[0]<k:
                        currFreq=False
                        break
                cur.execute("SELECT End FROM %s WHERE Start=%s" % (edgesTable, nodeStr))#get direct generalizations
                directGenID = cur.fetchall()
                directGen=[]
                for currGenID in directGenID :
                    cur.execute("SELECT * FROM %s WHERE ID=%s" % (nodesTable, currGenID[0]))
                    directGen.append(cur.fetchone())
                if isKanonymazation:
                    if i==numQI:#in te last iteration returning the first node that satisfies k annonimity
                        return nodeTuple
                    for currGen in directGenID:
                        markedNodes.append(currGen[0])#check if need strings or ints
                else:#if node doesn`t satisfies k annonimity
                    cur.execute("DELETE FROM %s WHERE ID=%s" % (replicaTable,nodeStr))#delete node from S table
                    for curr in directGen:#inserting to the queue the direct generalization sorted by height
                        height = 0
                        for j in range(int((tupSize / 2) - 1)):
                            height = height + curr[(j + 1) * 2]
                        nodesAndHeights.update({curr[0]: height})
                    SortedByHeightNodes = sorted(nodesAndHeights, key=nodesAndHeights.get)
                    for curr in SortedByHeightNodes:
                        q.put(curr)
        cur.execute("CREATE TABLE S"+str(i+1)+"q AS SELECT * FROM S"+str(i+1))
        cur.execute("SELECT * FROM S1q")
        forCreationNodeTable="C"+str(i+2)+"(ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1,"#for the sql query in line 156
        for j in range(i+1):
            forCreationNodeTable=forCreationNodeTable+"dim"+str(j+2)+","+"index"+str(j+2)+","
        print("CREATE TABLE %s" %forCreationNodeTable+"parent1,parent2)")
        cur.execute("CREATE TABLE %s" %forCreationNodeTable+"parent1,parent2)")#creating the next nodes table





main()