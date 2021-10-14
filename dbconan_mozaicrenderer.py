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
from dbconan_planstitcher import Mozaic, MozaicElement, MozaicLink
from dbconan_explorationrunner import ZigzagExplorationStrategy

def main(argv):
    print "***********************************************"
    print "DB CONAN 1.0 - Mozaic Renderer"
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
        optionParser.add_option("-m", "--mozaic", dest="mozaic", help="The name of the mozaic file.", type="string")                
        
        opts, args = optionParser.parse_args(argv)                

        if opts.dbPassword != None:
            inf = open(opts.mozaic)
            mozaic = cPickle.load(inf)
            inf.close() 

            updateDb = False
            for mozaicElement in mozaic.elements:
                if opts.dbHost is not None:
                    updateDb = True
                    mozaicElement.explorationPlan.schema.dbHost = opts.dbHost
                if opts.dbPort is not None:
                    updateDb = True
                    mozaicElement.explorationPlan.schema.dbPort = opts.dbPort
                if opts.dbName is not None:
                    updateDb = True
                    mozaicElement.explorationPlan.schema.dbName = opts.dbName
                if opts.dbUser is not None:
                    updateDb = True
                    mozaicElement.explorationPlan.schema.dbUser = opts.dbUser

            print "-------------------------------------------------------------------------------"
            print "Mozaic"
            print "-------------------------------------------------------------------------------"
            print "Mozaic name: %s" % mozaic.name            
            print "Mozaic description: %s" % mozaic.description
            print "Mozaic starting element: %s" % mozaic.startElement.name          
            print "Mozaic elements:"
            for mozaicElement in mozaic.elements:
               print "   %s:" % mozaicElement.name
               print "      Exploration plan: %s" % mozaicElement.explorationPlan.name
               print "         Description: %s" % mozaicElement.explorationPlan.description
               print "         Schema name: %s" % mozaicElement.explorationPlan.schema.name
               print "            Description: %s" % mozaicElement.explorationPlan.schema.description
               print "            DB host: %s" % mozaicElement.explorationPlan.schema.dbHost
               print "            DB port: %s" % mozaicElement.explorationPlan.schema.dbPort
               print "            DB name: %s" % mozaicElement.explorationPlan.schema.dbName
               print "            DB user: %s" % mozaicElement.explorationPlan.schema.dbUser
               print "            Tables:"
               for table in mozaicElement.explorationPlan.schema.tables.values():
                  print "               %s:" % table.name
                  if table.pk is not None:
                     for fk in table.pk.fks:                      
                        print "                  - from %s via %s:" % (fk.table.name, fk.name)
                  for fk in table.fks:                      
                     print "                  - to %s via %s:" % (fk.pk.table.name, fk.name)
               print "         Start table: %s" % mozaicElement.explorationPlan.startTable.name
               print "      Outgoing links:"
               for link in mozaicElement.outgoingLinks:
                   originColumnNames = [column.name for column in link.originColumns]
                   destinationColumnNames = [column.name for column in link.destinationColumns]
                   print "        - to plan %s, by columns %s in table %s, and columns %s in table %s" % (link.destinationMozaicElement.name, "(%s)" % ",".join(originColumnNames), link.originTableName, "(%s)" % ",".join(destinationColumnNames), link.destinationMozaicElement.explorationPlan.startTable.name)                                            
            print "-------------------------------------------------------------------------------"
            
            
            updateToPkColumnValuesList = False
            updateToNonPKColumnValuesList = False
            choice = None
            if len(mozaic.startElement.explorationPlan.pkColumnValuesList) < 1:
                choice = raw_input("Do you want to input primary-key values (y/n) ?: ")
            else:
                choice = raw_input("Do you want to input additional primary-key values (y/n) ?: ")
            if choice == 'y' or choice == 'Y':
                updateToPkColumnValuesList = True
                i = len(mozaic.startElement.explorationPlan.pkColumnValuesList) + 1
                while True:
                    pkColumnValues = []
                    print "-------------------------------------------------------------------------------"
                    print "PRIMARY KEY #%d" % i
                    print "-------------------------------------------------------------------------------"
                    for element in mozaic.startElement.explorationPlan.startTable.pk.elements:
                        inputVal = raw_input("%s: " % element.column.name) 
                        pkColumnValues.append(inputVal)
                    mozaic.startElement.explorationPlan.pkColumnValuesList.append(pkColumnValues)

                    print "-------------------------------------------------------------------------------"
                    print
                    choice = raw_input("Do you want to input another primary-key values (y/n) ?: ") 
                    print
                    if (choice == 'y' or choice == 'Y') == False:
                        break
                    i += 1
            else:
                updateToNonPKColumnValuesList = True
                print "-------------------------------------------------------------------------------"
                print "COLUMNS IN %s" % mozaic.startElement.explorationPlan.startTable.name
                print "-------------------------------------------------------------------------------"
                i = 1
                for column in mozaic.startElement.explorationPlan.startTable.columns:
                    print "(%d) %s (%s)" % (i, column.name, column.type)
                    i += 1
                choice = raw_input("Type the number of the column for the query: ") 
                mozaic.startElement.explorationPlan.nonPKColumnsForQuery = [mozaic.startElement.explorationPlan.startTable.columns[int(choice) - 1]]
                mozaic.startElement.explorationPlan.nonPKColumnValuesList = []
                i = 1
                while True:                    
                    print "-------------------------------------------------------------------------------"
                    inputVal = raw_input("Value #%d for %s: " % (i, mozaic.startElement.explorationPlan.nonPKColumnsForQuery[0].name))                     
                    print "-------------------------------------------------------------------------------"
                    mozaic.startElement.explorationPlan.nonPKColumnValuesList.append([inputVal])                                        
                    choice = raw_input("Do you want to input another value (y/n) ?: ") 
                    print
                    if (choice == 'y' or choice == 'Y') == False:
                        break
                    i += 1                                    

            updateToFilters = False
            for mozaicElement in mozaic.elements:
                for fltrKey in mozaicElement.explorationPlan.filters.keys():
                    for fltr in mozaicElement.explorationPlan.filters[fltrKey]:
                        if fltr.operator == "*ask*":
                            updateToFilters = True
                            operator = raw_input("Type the operator of the filter column %s in table %s: " % (fltr.column.name, fltrKey))
                            fltr.operator = operator
                        if fltr.value == "*ask*":
                            updateToFilters = True
                            value = raw_input("Type the value of the filter column %s in table %s: " % (fltr.column.name, fltrKey))
                            fltr.value = value                                                    

            if updateToPkColumnValuesList:
                print "Starting element's plan primary-key-columns values (%s):" % ",".join(element.column.name for element in mozaic.startElement.explorationPlan.startTable.pk.elements)
                for pkColumnValues in mozaic.startElement.explorationPlan.pkColumnValuesList:
                    print "   (%s)" % ",".join(pkColVal for pkColVal in pkColumnValues)

            if updateToFilters:
                for mozaicElement in mozaic.elements:                    
                    for fltrKey in mozaicElement.explorationPlan.filters.keys():
                        print "   Filter for table %s in mozaic-element's (%s) plan:" % (fltrKey, mozaicElement.name)
                        for fltr in mozaicElement.explorationPlan.filters[fltrKey]:
                            print "      %s %s %s" % (fltr.column.name, fltr.operator, fltr.value)

            if updateDb or updateToPkColumnValuesList or updateToFilters:
                choice = raw_input("Do you want to save the updated mozaic (y/n) ?: ") 
                if choice == 'y' or choice == 'Y':
                    description = raw_input("Type the description of the updated mozaic: ")
                    mozaic.description = description
                    filename = raw_input("Type the filename for the updated mozaic: ") + ".mzc"
                    ouf = open(filename, 'w')
                    cPickle.dump(mozaic, ouf)                
                    ouf.close()                
                    print "Updated mozaic saved: %s" % os.path.abspath(filename)

            mozaicRenderer = MozaicRenderer(mozaic)
            folder = opts.folder
            if folder is None: folder = "."
            mozaicRenderer.render(opts.dbPassword, folder)

        else:
            print "Missing options. Type %s -h for execution instruction." % argv[0]
            return 2
                
    except optionParser.error, msg:
        print msg
        return 2   

class MozaicRenderer:
    def __init__(self, mozaic):
        self.mozaic = mozaic
        self.mozaicElementsToBeRendered = []
        self.currentMozaicElementBeingRendered = None

    def render(self, dbPassword, folder):
        self.mozaicElementsToBeRendered.append(self.mozaic.startElement)
        mozaicRenderingId = "".join([str(i) for i in time.localtime()])

        fldr = os.path.abspath(os.path.join(folder, mozaicRenderingId))
        if os.path.exists(fldr) == False:
	    os.mkdir(fldr)    

        print " >>>>>>>>>>>>>>> DATA-MOZAIC RENDERING STARTED >>>>>>>>>>>>>>>"
        while len(self.mozaicElementsToBeRendered) > 0:
            self.currentMozaicElementBeingRendered = self.mozaicElementsToBeRendered.pop(0)
 
            dbConnection = cx_Oracle.connect(self.currentMozaicElementBeingRendered.explorationPlan.schema.dbUser, dbPassword, '%(dbHost)s:%(dbPort)s/%(dbName)s' % {"dbHost":self.currentMozaicElementBeingRendered.explorationPlan.schema.dbHost, "dbPort":self.currentMozaicElementBeingRendered.explorationPlan.schema.dbPort, "dbName":self.currentMozaicElementBeingRendered.explorationPlan.schema.dbName})
            cursor = dbConnection.cursor()            

            fldr = os.path.abspath(os.path.join(folder, mozaicRenderingId, self.currentMozaicElementBeingRendered.name))
            #traverse
            print "-------------------------------------------------------------------------------"
            print "JOURNAL of RENDERING of MOZAIC ELEMENT %s" % self.currentMozaicElementBeingRendered.name
            print "-------------------------------------------------------------------------------"
            zigzag = ZigzagExplorationStrategy()
            zigzag.queryCompletionListener = self
            zigzag.execute(fldr, cursor, self.currentMozaicElementBeingRendered.explorationPlan)
            print "-------------------------------------------------------------------------------"

            print 
            print "************* EXPLORATION COMPLETED (results under: %s) *************" % fldr

            cursor.close()
            dbConnection.close()      

        print " >>>>>>>>>>>>>>> DATA-MOZAIC RENDERING COMPLETED >>>>>>>>>>>>>>>"

    def notifyQueryCompletion(self, table, records):
        if len(records) < 0: 
            print "There's no matching records in the table %s in mozaic element %s." % (table.name, self.currentMozaicElementBeingRendered.name)
            return
        for outgoingLink in self.currentMozaicElementBeingRendered.outgoingLinks:
            if outgoingLink.originTableName == table.name:
                outgoingLink.destinationMozaicElement.explorationPlan.nonPKColumnsForQuery = outgoingLink.destinationColumns
                outgoingLink.destinationMozaicElement.explorationPlan.nonPKColumnValuesList = self.extractColumnValuesList(table, outgoingLink.originColumns, records)
                self.mozaicElementsToBeRendered.append(outgoingLink.destinationMozaicElement)                

    def extractColumnValuesList(self, table, columns, records):
        columnValuesList = []
        for rec in records:
            columnValues = []
            for column in columns:
                idx = table.getColumn(column.name).position - 1
                columnValues.append(rec[idx])
            columnValuesList.append(columnValues)
        return columnValuesList
                               
if __name__ == '__main__':
    sys.exit(main(sys.argv))      
