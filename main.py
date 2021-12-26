
import csv, sqlite3
from queue import Queue


def main():
    print('please enter your k: ')
    k = input()
    numQI=4
    con = sqlite3.connect("adults.db")
    cur = con.cursor()
    file = open("adult.csv")
    rows = csv.reader(file)
    cur.execute("CREATE TABLE adult (martialStatus,relationship,race,gender);")
    cur.executemany("INSERT INTO adult VALUES (?,?,?,?);", rows)
    #cur.execute("CREATE TABLE martialStatusTable (value);")
    #cur.execute("INSERT INTO martialStatusTable SELECT martialStatus FROM adult;")
    #cur.execute("CREATE TABLE relationshipTable (value);")
    #cur.execute("INSERT INTO relationshipTable SELECT relationship FROM adult;")
    #cur.execute("CREATE TABLE raceTable (value);")
    #cur.execute("INSERT INTO raceTable SELECT race FROM adult;")
    #cur.execute("CREATE TABLE genderTable (value);")
    #cur.execute("INSERT INTO genderTable SELECT gender FROM adult;")
    #here we are definining the generalizations of each atribute
    martialStatusHirarchy=[[[["Divorced"],"Divorced"],[["Married-AF-spouse"],"Married-AF-spouse"],[["Married-civ-spouse"],"Married-civ-spouse"],[["Married-spouse-absent"],"Married-spouse-absent"],[["Never-married"],"Never-married"],[["Separated"],"Separated"],[["Widowed"],"Widowed"]],[[["Divorced","Married-AF-spouse"],"divorcedOrAFSpouse"],[["Married-civ-spouse","Married-spouse-absent"],"CIVSpouseOrSpouseAbsent"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent"],"notSeperatedWidowedNever"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent","Never-married","Separated","Widowed"],"allMartialStatus"]]]
    relationshipHirarchy=[[[["Husband"],"Husband"],[["Not-in-family"],"Not-in-family"],[["Other-relative"],"Other-relative"],[["Own-child"],"Own-child"],[["Unmarried"],"Unmarried"],[["Wife"],"Wife"]],[[["Husband","Not-in-family"],"husbandOrNotIn"],[["Other-relative","Own-child"],"otherOrOwnChild"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child"],"notUnmarriedOrWife"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child","Unmarried","Wife"],"allRelationship"]]]
    raceHirarchy=[[[["Amer-Indian-Eskimo"],"Amer-Indian-Eskimo"],[["Asian-Pac-Islander"],"Asian-Pac-Islander"],[["Black"],"Black"],[["Other"],"Other"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other"],"blackOther"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other","White"],"blackWhiteOther"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander","Black","Other","White"],"allRaces"]]]
    genderHirarchy=[[[["male"],"male"],[["female"],"female"]],[["male","female"],"allGender"]]
    columnsRepresentations={1:'martialStatus',2:'relationship',3:'race',4:'gender'}
    #cur.execute("SELECT COUNT(*) FROM adult GROUP BY race,gender;")#example for frequency set query
    cur.execute("CREATE TABLE C1 (ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1)")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (1, 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (1, 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (1, 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (1, 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (2, 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (2, 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (2, 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (2, 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (3, 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (3, 1 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (3, 2 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (3, 3 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (4, 0 )")
    cur.execute("INSERT INTO C1 (dim1,index1) VALUES (4, 1 )")
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

        print("we are in iteration number "+str(i))

        nodesTable="C" + str(i+1) #the name of the nodes table in the current iteration
        replicaTable="S" + str(i+1)#the name of the nodes replica table in the current iteration
        edgesTable="E" + str(i+1)#the name of the edges table in the current iteration

        print("current nodes table:")
        cur.execute("SELECT * FROM %s"%nodesTable)
        print(cur.fetchall())
        print("current edges table:")
        cur.execute("SELECT * FROM %s" % edgesTable)
        print(cur.fetchall())

        cur.execute("CREATE TABLE %s AS SELECT * FROM %s" %(replicaTable,nodesTable))#create the replica table
        cur.execute("SELECT END FROM E1")#to get nodes which are not roots
        notRoots=cur.fetchall()
        notRootsList=[]
        for curr in notRoots:
            notRootsList.append(curr[0])
        cur.execute("SELECT * FROM C"+str(i+1)+" WHERE ID NOT IN (%s)" % ("?," * len(notRootsList))[:-1],notRootsList)#the list of roots
        roots=(cur.fetchall())

        print("the roots are")
        print(roots)

        nodesAndHeights={}
        if i==0:
            numOfColumnsNodesTable=len(roots[0])#size of nodes table
        else:
            numOfColumnsNodesTable = len(roots[0])-2  # size of nodes table
        for curr in roots:
            height=0
            for j in range(int((numOfColumnsNodesTable/2)-1)):
                height=height+curr[(j+1)*2]#height is the sum of the indexes in the nodes table
            nodesAndHeights.update({curr[0]:height})
        SortedByHeightNodes=sorted(nodesAndHeights, key=nodesAndHeights.get)#sort by height

        print("roots after we sorted by height:")
        print(SortedByHeightNodes)

        for curr in SortedByHeightNodes:
            q.put(curr)#inserted sorted by height
        markedNodes=[]
        while not q.empty():
            node=q.get()

            print("current node being checked")
            print(node)

            marked=False
            if node in markedNodes:
                marked=True

            print("marked:")
            print(marked)

            if not marked:
                nodeStr= str(node)
                cur.execute("SELECT * FROM C"+str(i+1)+" WHERE ID=?", [nodeStr])#get the tuple of the current node
                nodeTuple = cur.fetchone()

                print("current node tuple")
                print(nodeTuple)

                whatToSelect = columnsRepresentations.get(nodeTuple[1])#for the query that generates the table
                for k in range(i):
                    print("len")
                    for j in range(len(nodeTuple)-4):
                        if j%2==1:
                            print("we went to place in columnsRepresentations")
                            print(nodeTuple[j+2])
                            print("what we concatenated")
                            print(columnsRepresentations.get(nodeTuple[j+2]))
                            whatToSelect=whatToSelect+","+columnsRepresentations.get(nodeTuple[j+2])
                cur.execute("CREATE TABLE generate AS SELECT %s FROM adult" % whatToSelect)#generalization table
                for k in range(i + 1):
                    print("nodeTuple:")
                    print(nodeTuple)
                    print("2k+1")
                    print(2*k+1)
                    WhatToGen=columnsRepresentations.get(nodeTuple[2*k+1])
                    nameOfGenVar = WhatToGen + "Hirarchy"
                    generalization = locals()[nameOfGenVar][nodeTuple[2*k+2]]#used locals to get to variable name via string
                    print("generalization")
                    print(generalization)
                    cur.execute("UPDATE generate SET %s=? WHERE %s=?" % (WhatToGen, WhatToGen),[generalization[i][1], generalization[i][0][0]])#generalizing the table
                    print("checking generalization query")
                    print("UPDATE generate SET %s=? WHERE %s=?" % (WhatToGen, WhatToGen),[generalization[i][1], generalization[i][0][0]])
                cur.execute("SELECT COUNT(*) FROM generate GROUP BY %s ;" % columnsRepresentations.get(nodeTuple[1]))#get the frequency set
                freqset = cur.fetchall()
                cur.execute("DROP TABLE generate")
                isKanonymazation=True
                for currFreq in freqset:#check k-anonymity
                    if currFreq[0]<k:
                        isKanonymazation=False
                        break
                cur.execute("SELECT End FROM %s WHERE Start=%s" % (edgesTable, nodeStr))#get direct generalizations id
                directGenID = cur.fetchall()
                directGen=[]
                for currGenID in directGenID :
                    cur.execute("SELECT * FROM %s WHERE ID=%s" % (nodesTable, currGenID[0]))#get tuple of direct generalization from nodes table
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
                        for j in range(int((numOfColumnsNodesTable / 2) - 1)):
                            height = height + curr[(j + 1) * 2]
                        nodesAndHeights.update({curr[0]: height})
                    SortedByHeightNodes = sorted(nodesAndHeights, key=nodesAndHeights.get)
                    for curr in SortedByHeightNodes:
                        q.put(curr)
        newNodesTable="C"+str(i+2)+" (ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1,"#for the sql query in line 156
        insert_into="C"+str(i+2)+" (dim1,index1,"
        selectQueryInNode="SELECT "
        for j in range(i+1):
            insert_into =insert_into+"dim"+str(j+2)+","+"index"+str(j+2)+","+"parent1,parent2)"
            newNodesTable=newNodesTable+"dim"+str(j+2)+","+"index"+str(j+2)+","+"parent1,parent2)"
            selectQueryInNode=selectQueryInNode+"p.dim"+str(j+1)+",p.index"+str(j+1)+","
        selectQueryInNode=selectQueryInNode+"q.dim"+str(i+1)+",q.index"+str(i+1)+",p.ID,q.ID "
        whereQueryInNode="WHERE "
        for j in range(i):
            whereQueryInNode=whereQueryInNode+"p.dim"+str(j+1)+"=q.dim"+str(j+1)+" AND p.index"+str(j+1)+"=q.index"+str(j+1)+" AND "
        whereQueryInNode=whereQueryInNode+"p.dim"+str(j+1)+"<q.dim"+str(j+1)
        cur.execute("CREATE TABLE %s" %newNodesTable)#creating the next nodes table

        print("chack new nodes query")
        print("INSERT INTO %s" %insert_into+ " %s" %selectQueryInNode + "FROM S"+str(i+1)+" p,S"+str(i+1)+" q %s"%whereQueryInNode)
        cur.execute("INSERT INTO %s" %insert_into+ " %s" %selectQueryInNode + "FROM S"+str(i+1)+" p,S"+str(i+1)+" q %s"%whereQueryInNode)
        cur.execute("SELECT * FROM C2")
        print("lets see C2")
        print(cur.fetchall())
        cur.execute("CREATE TABLE E"+str(i+2)+" (Start,End)")
        print("lets check candidateedges query")
        print("INSERT INTO CandidateEdges (Start, End) SELECT p.ID,q.ID FROM C"+str(i+2)+" p,C"+str(i+2)+" q,E"+str(i+1)+" e,E"+str(i+1)+" f WHERE (e.Start=p.parent1 AND e.End=q.parent1 AND f.Start=p.parent2 AND f.End=q.parent2) OR (e.Start=p.parent1 AND e.End=q.parent1 AND p.parent2=q.parent2) OR (e.Start=p.parent2 AND e.End=q.parent2 AND p.parent1=q.parent1)")
        cur.execute("CREATE TABLE CandidateEdges (Start, End)")
        cur.execute("INSERT INTO CandidateEdges (Start, End) SELECT p.ID,q.ID FROM C"+str(i+2)+" p,C"+str(i+2)+" q,E"+str(i+1)+" e,E"+str(i+1)+" f WHERE (e.Start=p.parent1 AND e.End=q.parent1 AND f.Start=p.parent2 AND f.End=q.parent2) OR (e.Start=p.parent1 AND e.End=q.parent1 AND p.parent2=q.parent2) OR (e.Start=p.parent2 AND e.End=q.parent2 AND p.parent1=q.parent1)")
        print("lets see candidate edges")
        cur.execute("SELECT * FROM CandidateEdges")
        print(cur.fetchall())
        cur.execute("INSERT INTO E"+str(i+2)+" (Start,End) SELECT D.Start,D.End FROM CandidateEdges D EXCEPT SELECT D1.Start, D2.End FROM CandidateEdges D1,CandidateEdges D2 WHERE D1.End=D2.Start")
        print("lets see edges new table")
        cur.execute("SELECT * FROM E"+str(i+2))
        print(cur.fetchall())
        cur.execute("DROP TABLE CandidateEdges")





main()