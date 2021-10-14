import optparse
import sys
import cPickle
import traceback
import datetime
import cx_Oracle
import time
import os.path
from collections import deque
from dbconan_schema import Table, ForeignKey, PrimaryKey, Column, KeyElement, Schema
from dbconan_explorationplanner import ExplorationPlan, Filter

def main(argv):
    print "***********************************************"
    print "DB CONAN 1.0 - Exploration Runner"
    print "Raka Angga Jananuraga - raka.angga@gmail.com"
    print "***********************************************"
    print 
    
    try:
        optionParser = optparse.OptionParser("usage: %prog [options]")        
        optionParser.add_option("-H", "--host", dest="dbHost", help="The hostname of the database to analyze", type="string")
        optionParser.add_option("-p", "--port", dest="dbPort", help="The port number of the database to analyze.", type="int")        
        optionParser.add_option("-u", "--user", dest="dbUser", help="The user name to access the database to analyze.", type="string")        
        optionParser.add_option("-a", "--password", dest="dbPassword", help="The password to access the database to analyze.", type="string")        
        optionParser.add_option("-n", "--dbName", dest="dbName", help="The name of the database to analyze.", type="string")        
        optionParser.add_option("-f", "--folder", dest="folder", help="The name of the folder where to store the results of queries.", type="string")
        optionParser.add_option("-x", "--plan", dest="plan", help="The name of the exploration-plan file.", type="string")                
        
        opts, args = optionParser.parse_args(argv)                

        if opts.dbPassword != None:
            inf = open(opts.plan)
            explorationPlan = cPickle.load(inf)
            inf.close() 

            updateDb = False
            if opts.dbHost is not None:
                updateDb = True
                explorationPlan.schema.dbHost = opts.dbHost
            if opts.dbPort is not None:
                updateDb = True
                explorationPlan.schema.dbPort = opts.dbPort
            if opts.dbName is not None:
                updateDb = True
                explorationPlan.schema.dbName = opts.dbName
            if opts.dbUser is not None:
                updateDb = True
                explorationPlan.schema.dbUser = opts.dbUser

            print "-------------------------------------------------------------------------------"
            print "EXPLORATION PLAN"
            print "-------------------------------------------------------------------------------"
            print "Plan name: %s" % explorationPlan.name            
            print "Plan start table: %s" % explorationPlan.startTable.name
            print "Plan date of creation: %s" % explorationPlan.dateOfCreation
            print "Plan primary-key-columns values (%s):" % ",".join(element.column.name for element in explorationPlan.startTable.pk.elements)
            for pkColumnValues in explorationPlan.pkColumnValuesList:
                print "   (%s)" % ",".join(pkColVal for pkColVal in pkColumnValues)
            print "Plan filters:"
            for fltrKey in explorationPlan.filters.keys():
                print "   Filter for table %s:" % fltrKey
                for fltr in explorationPlan.filters[fltrKey]:
                    print "      %s %s %s" % (fltr.column.name, fltr.operator, fltr.value)
            print "Plan description: %s" % explorationPlan.description
            print "Schema date of creation: %s" % explorationPlan.schema.dateOfCreation
            print "Schema description: %s" % explorationPlan.schema.description
            print "DB host: %s" % explorationPlan.schema.dbHost
            print "DB port: %s" % explorationPlan.schema.dbPort
            print "DB name: %s" % explorationPlan.schema.dbName
            print "DB user: %s" % explorationPlan.schema.dbUser
            print "-------------------------------------------------------------------------------"

            updateToPkColumnValuesList = False
            updateToNonPKColumnValuesList = False
            choice = None
                        
            if len(explorationPlan.pkColumnValuesList) < 1:
                choice = raw_input("Do you want to input primary-key values (y/n) ?: ")
            else:
                choice = raw_input("Do you want to input additional primary-key values (y/n) ?: ")
            if choice == 'y' or choice == 'Y':
                updateToPkColumnValuesList = True
                i = len(explorationPlan.pkColumnValuesList) + 1
                while True:
                    pkColumnValues = []
                    print "-------------------------------------------------------------------------------"
                    print "PRIMARY KEY #%d" % i
                    print "-------------------------------------------------------------------------------"
                    for element in explorationPlan.startTable.pk.elements:
                        inputVal = raw_input("%s: " % element.column.name) 
                        pkColumnValues.append(inputVal)
                    explorationPlan.pkColumnValuesList.append(pkColumnValues)

                    print "-------------------------------------------------------------------------------"
                    print
                    choice_2 = raw_input("Do you want to input another primary-key values (y/n) ?: ") 
                    print
                    if (choice_2 == 'y' or choice_2 == 'Y') == False:
                        break
                    i += 1
            
            if (choice == 'N' or choice == 'n') and len(explorationPlan.pkColumnValuesList) < 1:
                updateToNonPKColumnValuesList = True
                print "-------------------------------------------------------------------------------"
                print "COLUMNS IN %s" % explorationPlan.startTable.name
                print "-------------------------------------------------------------------------------"
                i = 1
                for column in explorationPlan.startTable.columns:
                    print "(%d) %s (%s)" % (i, column.name, column.type)
                    i += 1
                choice = raw_input("Type the number of the column for the query: ") 
                explorationPlan.nonPKColumnsForQuery = [explorationPlan.startTable.columns[int(choice) - 1]]
                explorationPlan.nonPKColumnValuesList = []
                i = 1
                while True:                    
                    print "-------------------------------------------------------------------------------"
                    inputVal = raw_input("Value #%d for %s: " % (i, explorationPlan.nonPKColumnsForQuery[0].name))                     
                    print "-------------------------------------------------------------------------------"
                    explorationPlan.nonPKColumnValuesList.append([inputVal])                                        
                    choice = raw_input("Do you want to input another value (y/n) ?: ") 
                    print
                    if (choice == 'y' or choice == 'Y') == False:
                        break
                    i += 1                

            updateToFilters = False
            for fltrKey in explorationPlan.filters.keys():
                for fltr in explorationPlan.filters[fltrKey]:
                    if fltr.operator == "*ask*":
                        updateToFilters = True
                        operator = raw_input("Type the operator of the filter column %s in table %s: " % (fltr.column.name, fltrKey))
                        fltr.operator = operator
                    if fltr.value == "*ask*":
                        updateToFilters = True
                        value = raw_input("Type the value of the filter column %s in table %s: " % (fltr.column.name, fltrKey))
                        fltr.value = value                                                 

            if updateToPkColumnValuesList:
                print "Plan primary-key-columns values (%s):" % ",".join(element.column.name for element in explorationPlan.startTable.pk.elements)
                for pkColumnValues in explorationPlan.pkColumnValuesList:
                    print "   (%s)" % ",".join(pkColVal for pkColVal in pkColumnValues)

            if updateToFilters:
                print "Plan filters:"
                for fltrKey in explorationPlan.filters.keys():
                    print "   Filter for table %s:" % fltrKey
                    for fltr in explorationPlan.filters[fltrKey]:
                        print "      %s %s %s" % (fltr.column.name, fltr.operator, fltr.value)

            if updateDb or updateToPkColumnValuesList or updateToFilters or updateToNonPKColumnValuesList:
                choice = raw_input("Do you want to save the updated exploration plan (y/n) ?: ") 
                if choice == 'y' or choice == 'Y':
                    description = raw_input("Type the description of the updated exploration plan: ")
                    explorationPlan.description = description
                    filename = raw_input("Type the filename for the updated exploration plan: ") + ".xpp"
                    ouf = open(filename, 'w')
                    cPickle.dump(explorationPlan, ouf)                
                    ouf.close()                
                    print "Updated exploration-plan saved: %s" % os.path.abspath(filename)

            dbConnection = cx_Oracle.connect(explorationPlan.schema.dbUser, opts.dbPassword, '%(dbHost)s:%(dbPort)s/%(dbName)s' % {"dbHost":explorationPlan.schema.dbHost, "dbPort":explorationPlan.schema.dbPort, "dbName":explorationPlan.schema.dbName})
            cursor = dbConnection.cursor()            
            while True:
                action = raw_input("Press r to run the execution plan, * to exit: ")
                if action == "r" or action == "R":
                    journalId = "".join([str(i) for i in time.localtime()])
                    folder = opts.folder
                    if folder is None: folder = "."
                    fldr = os.path.abspath(os.path.join(folder, journalId))
                    #traverse
                    print "-------------------------------------------------------------------------------"
                    print "JOURNAL "
                    print "-------------------------------------------------------------------------------"
                    zigzag = ZigzagExplorationStrategy()
                    zigzag.execute(fldr, cursor, explorationPlan)
                    print "-------------------------------------------------------------------------------"

                    print 
                    print "************* EXPLORATION COMPLETED (results under: %s) *************" % fldr
                else:
                    break

            cursor.close()
            dbConnection.close()            
        else:
            print "Missing options. Type %s -h for execution instruction." % argv[0]
            return 2
                
    except optionParser.error, msg:
        print msg
        return 2   

class ZigzagExplorationStrategy:
    def __init__(self):
        self.explorationPlanInstance = None
        self.traversedReferences = {}
        self.visitedTables = {}
        self.executing = False
        self.queryCompletionListener = None

    def execute(self, folder, cursor, explorationPlanInstance):
        if self.executing is True: raise "Illegal state error"
        self.executing = True
        
        if explorationPlanInstance is None: raise "explorationPlanInstance can not be null"

        self.explorationPlanInstance = explorationPlanInstance
        self.traversedReferences = {}
        self.visitedTables = {}  
        self.folder = folder
          
        if len(explorationPlanInstance.pkColumnValuesList) > 0:            
            self.zig(cursor, explorationPlanInstance.startTable, explorationPlanInstance.pkColumnValuesList, None)
        else:
            self.zig(cursor, explorationPlanInstance.startTable, explorationPlanInstance.nonPKColumnValuesList, None, explorationPlanInstance.nonPKColumnsForQuery)

        self.executing = False
        
    def zig(self, cursor, table, columnValuesList, transportReference, nonPKColumnsForQuery = None):
        pendingBackwardTraversals = self.traverseForward(cursor, table, columnValuesList, transportReference, nonPKColumnsForQuery)
        if len(pendingBackwardTraversals) > 0: pendingBackwardTraversals.reverse()
        for backwardTraversal in pendingBackwardTraversals:
            inRef = backwardTraversal[0]
            fkColumnValuesList = backwardTraversal[1]
            self.zag(cursor, inRef.table, fkColumnValuesList, inRef)

    def zag(self, cursor, table, fkColumnValuesList, transportReference):
        pendingForwardTraversals = self.traverseBackward(cursor, table, fkColumnValuesList, transportReference)
        if len(pendingForwardTraversals) > 0: pendingForwardTraversals.reverse()
        for forwardTraversal in pendingForwardTraversals:
            outRef = forwardTraversal[0]
            pkColumnValuesList = forwardTraversal[1]            
            self.zig(cursor, outRef.pk.table, pkColumnValuesList, outRef)

    def removeDuplicatesInKeyColumnValuesList(self, keyColumnValuesList):
        distinctKeyColumnValuesList = [keyColumnValues for keyColumnValues in keyColumnValuesList]

        i = len(distinctKeyColumnValuesList) - 1
        while i >= 1:
            j = i - 1
            while j >= 0:
                if distinctKeyColumnValuesList[i] == distinctKeyColumnValuesList[j]:
                    distinctKeyColumnValuesList.pop(i)
                    break
                j = j -1
            i = i - 1

        return distinctKeyColumnValuesList

    def filterOutNones(self, keyColumnValuesList):
        newKeyColumnValuesList = [entry for entry in keyColumnValuesList]
        i = len(newKeyColumnValuesList) - 1
        while i >= 0:
            columnValues = newKeyColumnValuesList[i]
            for colVal in columnValues:
                if colVal is None:
                    newKeyColumnValuesList.pop(i)
                    break
            i = i - 1
        return newKeyColumnValuesList

    def doQuery(self, cursor, table, keyColumns, keyColumnValuesList):        
        records = []
        keyColumnValuesList = self.filterOutNones(keyColumnValuesList)

        if len(keyColumnValuesList) < 1:
            print "Did not query table %s because no keys to be used. Returning zero-length records." % table.name
        else:
            print "-----"
            print "Querying table %s using keys:" % table.name, keyColumnValuesList

            distinctKeyColumnValuesList = self.removeDuplicatesInKeyColumnValuesList(keyColumnValuesList)
            sqlQuery = "SELECT %(columnNames)s from %(tbName)s WHERE %(whereClause)s %(filtersStr)s" % {"columnNames":self.columnsAsString(table.columns), "tbName":table.name, "whereClause":self.buildWhereClause(keyColumns, distinctKeyColumnValuesList), "filtersStr":self.buildFiltersAsString(table)}
            print sqlQuery
            cursor.execute(sqlQuery)        

            for row in cursor:
                records.append(row)

            if len(records) > 0:
                if os.path.exists(self.folder) == False:
	            os.mkdir(self.folder)    
                filename = os.path.abspath(os.path.join(self.folder, table.name)) + ".csv"
                fh = open(filename , 'w')        
                fh.write(",".join('"%s"' % column.name for column in table.columns))
                fh.write("\n")        

                i = 0
                for rec in records:
                    recCSV = []
                    for colVal in rec:
                        if colVal is None:
                            recCSV.append('')
                        else:
                            recCSV.append('"%s"' % str(colVal))
 
                    fh.write(",".join(recCSV))
                    if i < len(records) - 1: fh.write("\n")
                    i += 1

                #how to convert None to ""
                fh.close()

        if self.queryCompletionListener is not None:
            self.queryCompletionListener.notifyQueryCompletion(table, records)

        return records

    def traverseForward(self, cursor, referredTable, columnValuesList, transportReference, nonPKColumnsForQuery = None):
        if self.visitedTables.get(referredTable.name) is not None:           
            self.traversedReferences[transportReference.name] = transportReference
            return []

        queryResultRecords = None
        if transportReference is not None:
            print ">> Traversing forward to %s from %s via %s" % (referredTable.name, transportReference.table.name, transportReference.name)

        if nonPKColumnsForQuery is None:
            queryResultRecords = self.doQuery(cursor, referredTable, [element.column for element in referredTable.pk.elements], columnValuesList)
        else:
            queryResultRecords = self.doQuery(cursor, referredTable, nonPKColumnsForQuery, columnValuesList)
            
        print "%d record(s) found" % len(queryResultRecords)
        print "-----"
        self.visitedTables[referredTable.name] = referredTable
        if transportReference is not None:
            self.traversedReferences[transportReference.name] = transportReference        

        pendingBackwardTraversals = [] #array of tuples (reference, fkColumnValuesList)

        if len(queryResultRecords) > 0:            
            outgoingReferences = self.findTraversableOutgoingReferences(referredTable)
            for outRef in outgoingReferences:
                fkColumnValuesList = self.extractColumnValuesList(outRef, queryResultRecords)
                pendingBackwardTraversals.extend(self.traverseForward(cursor, outRef.pk.table, fkColumnValuesList, outRef))

            if referredTable.pk is not None:
              reverseColumnValuesList = self.extractColumnValuesList(referredTable.pk, queryResultRecords)
              incomingReferences = self.findTraversableIncomingReferences(referredTable)            
              for incomingRef in incomingReferences:                
                  pendingBackwardTraversals.append((incomingRef, reverseColumnValuesList))
            
        return pendingBackwardTraversals

    def traverseBackward(self, cursor, referringTable, fkColumnValuesList, transportReference):
        if self.visitedTables.get(referringTable.name) is not None:
            self.traversedReferences[transportReference.name] = transportReference
            return []

        print "<< Traversing backward to %s from %s via %s" % (referringTable.name, transportReference.pk.table.name, transportReference.name)
        queryResultRecords = self.doQuery(cursor, referringTable, [element.column for element in transportReference.elements], fkColumnValuesList)
        print "... %d record(s) found" % len(queryResultRecords)
        self.visitedTables[referringTable.name] = referringTable
        self.traversedReferences[transportReference.name] = transportReference

        pendingForwardTraversals = [] #array of tuples (reference, pkColumnValuesList)

        if len(queryResultRecords) > 0:
          if transportReference.table.pk is not None:
            pkColumnValuesList = self.extractColumnValuesList(transportReference.table.pk, queryResultRecords)
            incomingReferences = self.findTraversableIncomingReferences(referringTable)            
            for inRef in incomingReferences:            
              pendingForwardTraversals.extend(self.traverseBackward(cursor, inRef.table, pkColumnValuesList, inRef))

          outgoingReferences = self.findTraversableOutgoingReferences(referringTable)
          for outRef in outgoingReferences:
            reverseColumnValuesList = self.extractColumnValuesList(outRef, queryResultRecords)
            pendingForwardTraversals.append((outRef, reverseColumnValuesList))
            
        return pendingForwardTraversals

    def findTraversableOutgoingReferences(self, table):
        ret = []

        for outRef in table.fks:
          existingOutRef = self.traversedReferences.get(outRef.name)
          if existingOutRef is None:
              ret.append(outRef)
        
        return ret

    def findTraversableIncomingReferences(self, table):
        ret = []

        for inRef in table.pk.fks:
          existingInRef = self.traversedReferences.get(inRef.name)
          if existingInRef is None:
              ret.append(inRef)
        
        return ret

    def extractColumnValuesList(self, key, records):
        columnValuesList = []
        for rec in records:
          columnValues = []
          for keyElement in key.elements:
            idx = key.table.getColumn(keyElement.column.name).position - 1
            columnValues.append(rec[idx])
          columnValuesList.append(columnValues)
        return columnValuesList

    def columnsAsString(self, columns):
        columnNames = [column.name for column in columns]
        ret = ""
        i = 0
        while i < len(columnNames):
          ret += columnNames[i]
          if i < len(columnNames) - 1:
              ret += ", "
          i += 1
        return ret

    def removeEmptyColumnValues(self, columnValuesList):
        newColumnValuesList = [columnValues for columnValues in columnValuesList]
        i = len(newColumnValuesList) - 1
        while i >= 0:
          columnValues = newColumnValuesList[i]            
          if columnValues == [None for columnValue in columnValues]:
            newColumnValuesList.pop(i)
          i =- 1
        return newColumnValuesList

    def buildWhereClause(self, columns, columnValuesList):
        columnValuesList = self.removeEmptyColumnValues(columnValuesList)

        whereClause = "("   
        i = 0
        for columnValues in columnValuesList:
            whereClause += "("
            j = 0
            for colValue in columnValues:
                if colValue != None:
                    whereClause += columns[j].name + "="
                    if columns[j].type == 'CHAR' or columns[j].type == 'VARCHAR2':
                        whereClause += "'" + colValue + "'" 
                    elif columns[j].type == 'NUMBER' or columns[j].type == 'FLOAT':
                        whereClause += str(colValue)
                    elif columns[j].type == 'DATE':
                        whereClause += "TIMESTAMP'" + str(colValue) + "'"
                    elif columns[j].type[0:9] == 'TIMESTAMP':
                        whereClause += "TIMESTAMP'" + str(colValue) + "'"
                    if j < len(columnValues) - 1: whereClause += " AND "                
                j += 1
            whereClause += ")"

            if i < len(columnValuesList) - 1:
                whereClause = whereClause + " OR "
                
            i += 1    

        whereClause = whereClause + ")"

        return whereClause    

    def buildFiltersAsString(self, table):
        filtersStr = ""

        filters = self.explorationPlanInstance.filters.get(table.name)
        if filters is not None:
            for fltr in filters:
                fltrValue = None
                if fltr.operator != "is":
                    if fltr.column.type == 'CHAR' or fltr.column.type == 'VARCHAR2':
                        fltrValue = "'" + fltr.value + "'" 
                    elif fltr.column.type == 'NUMBER' or fltr.column.type == 'FLOAT':
                        fltrValue = str(fltr.value)
                    elif fltr.column.type == 'DATE':
                        fltrValue = "TIMESTAMP'" + str(fltr.value) + "'"
                    elif fltr.column.type[0:9] == 'TIMESTAMP':
                        fltrValue = "TIMESTAMP'" + str(fltr.value) + "'"
                else:
                    fltrValue = fltr.value
                filtersStr += " AND %s %s %s " % (fltr.column.name, fltr.operator, fltrValue)            

        return filtersStr

    def allowAssociation(self, schema, referenceTable, associatedTable, associationId):        
        visitedTables = []
        pendingToBeVisiteds = [referenceTable]
        trails = []
        trails.append([referenceTable])

        while pendingToBeVisiteds:
            currentTable = pendingToBeVisiteds.pop(0)                                      

            if currentTable is associatedTable: 
                for trail in trails:
                    if trail[-1] is currentTable:
                        if len(trail) == 1:
                            print
                            print "(!!!) The association %s will cause a loop in table %s. Rejected." % (associationId, trail[0].name)                        
                            print
                        elif len(trail) > 1:
                            trailStr = " --- ".join(table.name for table in trail)
                            print
                            print "(!!!) The association %s will cause a cycle. Rejected. If you want it, break any of the following association(s) first: %s" % (associationId, trailStr)
                            print

                        return False #cycle detected, break

            visitedTables.append(currentTable)
            
            trailToBeContinued = None
            i = 0
            for trail in trails:
                if trail[-1] is currentTable:
                    trailToBeContinued = trail
                    trails.pop(i)                    
                    break
                i += 1

            #append children
            referredTables = [fk.pk.table for fk in currentTable.fks]
            for referredTable in referredTables:
                if referredTable in visitedTables: 
		    continue
                pendingToBeVisiteds.append(referredTable)
                #clone the trail
                trailCopy = [trail for trail in trailToBeContinued]
                trailCopy.append(referredTable)
                trails.append(trailCopy)
            if currentTable.pk is not None:
                referringTables = [fk.table for fk in currentTable.pk.fks]
                for referringTable in referringTables:
                    if referringTable in visitedTables: 
                        continue
                    pendingToBeVisiteds.append(referringTable)
                    trailCopy = [trail for trail in trailToBeContinued]
                    trailCopy.append(referringTable)
                    trails.append(trailCopy)

        return True       
                               
if __name__ == '__main__':
    sys.exit(main(sys.argv))      
