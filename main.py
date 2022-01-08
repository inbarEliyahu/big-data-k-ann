
import csv, sqlite3, time
from queue import Queue


def main():
    print('please enter your k: ')
    ourK = input()
    ourK=int(ourK)
    numQI=4
    con = sqlite3.connect("adults.db")
    cur = con.cursor()
    file = open("adult.csv")
    rows = csv.reader(file)
    cur.execute("CREATE TABLE adult (martialStatus,relationship,race,gender);")
    cur.executemany("INSERT INTO adult VALUES (?,?,?,?);", rows)
    #here we are definining the generalizations of each atribute
    martialStatusHirarchy=[[[["Divorced"],"Divorced"],[["Married-AF-spouse"],"Married-AF-spouse"],[["Married-civ-spouse"],"Married-civ-spouse"],[["Married-spouse-absent"],"Married-spouse-absent"],[["Never-married"],"Never-married"],[["Separated"],"Separated"],[["Widowed"],"Widowed"]],[[["Divorced","Married-AF-spouse"],"divorcedOrAFSpouse"],[["Married-civ-spouse","Married-spouse-absent"],"CIVSpouseOrSpouseAbsent"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent"],"notSeperatedWidowedNever"],[["Never-married","Separated","Widowed"],"seperatedWidowedNever"]],[[["Divorced","Married-AF-spouse","Married-civ-spouse","Married-spouse-absent","Never-married","Separated","Widowed"],"allMartialStatus"]]]
    relationshipHirarchy=[[[["Husband"],"Husband"],[["Not-in-family"],"Not-in-family"],[["Other-relative"],"Other-relative"],[["Own-child"],"Own-child"],[["Unmarried"],"Unmarried"],[["Wife"],"Wife"]],[[["Husband","Not-in-family"],"husbandOrNotIn"],[["Other-relative","Own-child"],"otherOrOwnChild"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child"],"notUnmarriedOrWife"],[["Unmarried","Wife"],"unmarriedOrWife"]],[[["Husband","Not-in-family","Other-relative","Own-child","Unmarried","Wife"],"allRelationship"]]]
    raceHirarchy=[[[["Amer-Indian-Eskimo"],"Amer-Indian-Eskimo"],[["Asian-Pac-Islander"],"Asian-Pac-Islander"],[["Black"],"Black"],[["Other"],"Other"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other"],"blackOther"],[["White"],"White"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander"],"indianAsian"],[["Black","Other","White"],"blackWhiteOther"]],[[["Amer-Indian-Eskimo","Asian-Pac-Islander","Black","Other","White"],"allRaces"]]]
    genderHirarchy=[[[["male"],"male"],[["female"],"female"]],[[["male","female"],"allGender"]]]
    columnsRepresentations={1:'martialStatus',2:'relationship',3:'race',4:'gender'} #every number in the dim columns in nodes table represents column from the original database
    cur.execute("CREATE TABLE C1 (ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1)")#creating the first nodes table
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
    cur.execute("CREATE TABLE E1 (Start,End)")#creating the first edges table
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
    q = []
    for i in range(numQI):
        nodesTable="C" + str(i+1) #the name of the nodes table in the current iteration
        replicaTable="S" + str(i+1)#the name of the nodes replica table in the current iteration
        edgesTable="E" + str(i+1)#the name of the edges table in the current iteration
        cur.execute("CREATE TABLE %s AS SELECT * FROM %s" %(replicaTable,nodesTable))#create the replica table
        cur.execute("SELECT END FROM E1")#to get nodes which are not roots
        notRoots=cur.fetchall()
        notRootsList=[]
        for curr in notRoots:
            notRootsList.append(curr[0])
        cur.execute("SELECT * FROM C"+str(i+1)+" WHERE ID NOT IN (%s)" % ("?," * len(notRootsList))[:-1],notRootsList)#the list of roots
        roots=(cur.fetchall())
        nodesAndHeights={}
        numNodeColumnsWithoutParents=(2*(i+1))+1#size of nodes table
        for curr in roots:
            height=0
            for j in range(int(numNodeColumnsWithoutParents)):
                if j!=0:
                    if (j%2)==0:
                        height=height+curr[j]#height is the sum of the indexes in the nodes table
            nodesAndHeights.update({curr[0]:height})
        SortedByHeightNodes=sorted(nodesAndHeights, key=nodesAndHeights.get)#sort by height
        nodesAndHeights={} #initializing nodes and height
        for curr in SortedByHeightNodes:
            q.append(curr)#inserted sorted by height
        markedNodes=[]
        while not len(q)==0:
            node=q.pop(0)
            marked=False
            if node in markedNodes:
                marked=True
            if not marked:
                nodeStr= str(node)
                cur.execute("SELECT * FROM C"+str(i+1)+" WHERE ID=?", [nodeStr])#get the tuple of the current node
                nodeTuple = cur.fetchone()
                whatToSelect = columnsRepresentations.get(nodeTuple[1])#for the query that generates the table
                for j in range(numNodeColumnsWithoutParents-2):
                    if j%2==1:
                        whatToSelect=whatToSelect+","+columnsRepresentations.get(nodeTuple[j+2])
                cur.execute("CREATE TABLE generate AS SELECT %s FROM adult" % whatToSelect)#generalization table
                for k in range(i + 1):
                    WhatColumnToGen=columnsRepresentations.get(nodeTuple[2*k+1])
                    nameOfGenVar = WhatColumnToGen + "Hirarchy"
                    generalization = locals()[nameOfGenVar][nodeTuple[2*k+2]]#used locals to get to variable name via string
                    for j in range(len(generalization)):
                        for t in range(len(generalization[j][0])):
                            cur.execute("UPDATE generate SET %s=? WHERE %s=?" % (WhatColumnToGen, WhatColumnToGen),[generalization[j][1], generalization[j][0][t]])#generalizing the table
                cur.execute("SELECT COUNT(*) FROM generate GROUP BY %s ;" % columnsRepresentations.get(nodeTuple[1]))#get the frequency set
                freqset = cur.fetchall()
                cur.execute("DROP TABLE generate")
                isKanonymazation=True
                for currFreq in freqset:#check k-anonymity
                    if currFreq[0]<ourK:
                        isKanonymazation=False
                        break
                cur.execute("SELECT End FROM %s WHERE Start=%s" % (edgesTable, nodeStr))#get direct generalizations id
                directGenID = cur.fetchall()
                directGen=[]
                for currGenID in directGenID :
                    cur.execute("SELECT * FROM %s WHERE ID=%s" % (nodesTable, currGenID[0]))#get tuple of direct generalization from nodes table
                    directGen.append(cur.fetchone())
                if isKanonymazation:
                    if i==(numQI-1):#in te last iteration returning the first node that satisfies k annonimity
                        firstGen=columnsRepresentations.get(nodeTuple[1])
                        secondtGen=columnsRepresentations.get(nodeTuple[3])
                        thirdGen=columnsRepresentations.get(nodeTuple[5])
                        fourthdGen=columnsRepresentations.get(nodeTuple[7])
                        print(firstGen+" is k anonymous with generalization number "+str(nodeTuple[2]))
                        print(secondtGen+" is k anonymous with generalization number "+str(nodeTuple[4]))
                        print(thirdGen+" is k anonymous with generalization number "+str(nodeTuple[6]))
                        print(fourthdGen+" is k anonymous with generalization number "+str(nodeTuple[8]))
                        return
                    for currGen in directGenID:
                        markedNodes.append(currGen[0])
                else:#if node doesn`t satisfies k annonimity
                    cur.execute("DELETE FROM %s WHERE ID=%s" % (replicaTable,nodeStr))#delete node from S table
                    for curr in directGen:#inserting to the queue the direct generalization sorted by height
                        height = 0
                        for j in range(int(numNodeColumnsWithoutParents)):
                            if j != 0:
                                if (j % 2) == 0:
                                    height = height + curr[j]  # height is the sum of the indexes in the nodes table
                        nodesAndHeights.update({curr[0]: height})
                    while len(q)>0:
                        currID=q.pop(0)
                        cur.execute("SELECT * FROM C"+str(i+1)+" WHERE ID="+str(currID))
                        current=cur.fetchone()
                        height = 0
                        for j in range(int(numNodeColumnsWithoutParents)):
                            if j != 0:
                                if (j % 2) == 0:
                                    height = height + current[j]  # height is the sum of the indexes in the nodes table
                        nodesAndHeights.update({current[0]: height})
                    SortedByHeightNodes = sorted(nodesAndHeights, key=nodesAndHeights.get)
                    nodesAndHeights={}
                    for curr in SortedByHeightNodes:
                        q.append(curr)
        newNodesTable="C"+str(i+2)+" (ID INTEGER PRIMARY KEY AUTOINCREMENT,dim1,index1,"#for the sql query
        insert_into="C"+str(i+2)+" (dim1,index1,"
        selectQueryInNode="SELECT "
        whereQueryInNode = "WHERE "
        for j in range(i+1):
            insert_into =insert_into+"dim"+str(j+2)+","+"index"+str(j+2)+","
            newNodesTable=newNodesTable+"dim"+str(j+2)+","+"index"+str(j+2)+","
            selectQueryInNode=selectQueryInNode+"p.dim"+str(j+1)+",p.index"+str(j+1)+","
            if j!=i:
                whereQueryInNode=whereQueryInNode+"p.dim"+str(j+1)+"=q.dim"+str(j+1)+" AND p.index"+str(j+1)+"=q.index"+str(j+1)+" AND "
        newNodesTable=newNodesTable+"parent1,parent2)"
        insert_into=insert_into+"parent1,parent2)"
        selectQueryInNode=selectQueryInNode+"q.dim"+str(i+1)+",q.index"+str(i+1)+",p.ID,q.ID "
        whereQueryInNode=whereQueryInNode+"p.dim"+str(i+1)+"<q.dim"+str(i+1)
        cur.execute("CREATE TABLE %s" %newNodesTable)#creating the next nodes table
        cur.execute("INSERT INTO %s" %insert_into+ " %s" %selectQueryInNode + "FROM S"+str(i+1)+" p,S"+str(i+1)+" q %s"%whereQueryInNode)
        cur.execute("CREATE TABLE CandidateEdges (Start, End)")
        cur.execute("INSERT INTO CandidateEdges (Start, End) SELECT p.ID,q.ID FROM C"+str(i+2)+" p,C"+str(i+2)+" q,E"+str(i+1)+" e,E"+str(i+1)+" f WHERE (e.Start=p.parent1 AND e.End=q.parent1 AND f.Start=p.parent2 AND f.End=q.parent2) OR (e.Start=p.parent1 AND e.End=q.parent1 AND p.parent2=q.parent2) OR (e.Start=p.parent2 AND e.End=q.parent2 AND p.parent1=q.parent1)")
        cur.execute("CREATE TABLE E"+str(i+2)+" (Start,End)")
        cur.execute("INSERT INTO E"+str(i+2)+" (Start,End) SELECT D.Start,D.End FROM CandidateEdges D EXCEPT SELECT D1.Start, D2.End FROM CandidateEdges D1,CandidateEdges D2 WHERE D1.End=D2.Start")
        cur.execute("DROP TABLE CandidateEdges")

main()