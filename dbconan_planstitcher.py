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
    print "DB CONAN 1.0 - Plan Stitcher"
    print "Raka Angga Jananuraga - raka.angga@gmail.com"
    print "***********************************************"
    print 
    
    try:
        optionParser = optparse.OptionParser("usage: %prog [options] [plan_1] [plan_2]... (plan_1-plan_n are exploration plans to be stitched together)")        
        optionParser.add_option("-H", "--host", dest="dbHost", help="The hostname of the database to analyze", type="string")
        optionParser.add_option("-p", "--port", dest="dbPort", help="The port number of the database to analyze.", type="int")        
        optionParser.add_option("-u", "--user", dest="dbUser", help="The user name to access the database to analyze.", type="string")        
        optionParser.add_option("-a", "--password", dest="dbPassword", help="The password to access the database to analyze.", type="string")        
        optionParser.add_option("-n", "--dbName", dest="dbName", help="The name of the database to analyze.", type="string")                

        #can have different database !!! This is so cool !!! REMOVE THAT RESTRICTION. THE ONLY RESTRICTION IS: the fk in the referring table (in plan A) must have the same number of elements as the pk in the referred table (in plan B)

        opts, args = optionParser.parse_args(argv)
        if len(args) < 3:
            print "You must specify at least two exploration plans to be stitched together."
            sys.exit(0)

        mozaic = Mozaic()
        for plan in args[1:]:
            print plan
            inf = open(plan)
            explorationPlan = cPickle.load(inf)
            inf.close()

            if opts.dbHost is not None:
                explorationPlan.schema.dbHost = opts.dbHost
            if opts.dbPort is not None:
                explorationPlan.schema.dbPort = opts.dbPort
            if opts.dbName is not None:
                explorationPlan.schema.dbName = opts.dbName
            if opts.dbUser is not None:
                explorationPlan.schema.dbUser = opts.dbUser

            mozaicElement = MozaicElement(explorationPlan)
            mozaic.addElement(mozaicElement)

        while True:
            print "-------------------------------------------------------------------------------"
            print "EXPLORATION PLANS"
            print "-------------------------------------------------------------------------------"
            i = 1
            for element in mozaic.elements:                
                print "(%d) %s" % (i, element.name)
                print "     Exploration plan: %s" % element.explorationPlan.name
                print "        Description: %s" % element.explorationPlan.description
                print "        Schema name: %s" % element.explorationPlan.schema.name
                print "           Description: %s" % element.explorationPlan.schema.description
                print "           DB host: %s" % element.explorationPlan.schema.dbHost
                print "           DB port: %s" % element.explorationPlan.schema.dbPort
                print "           DB name: %s" % element.explorationPlan.schema.dbName
                print "           DB user: %s" % element.explorationPlan.schema.dbUser
                print "           Tables:"
                for table in element.explorationPlan.schema.tables.values():
                   print "              %s:" % table.name
                   if table.pk is not None:
                      for fk in table.pk.fks:                      
                         print "                 - from %s via %s:" % (fk.table.name, fk.name)
                   for fk in table.fks:                      
                      print "                 - to %s via %s:" % (fk.pk.table.name, fk.name)
                print "        Start table: %s" % element.explorationPlan.startTable.name
                print "     Outgoing links:"
                for link in element.outgoingLinks:
                    originColumnNames = [column.name for column in link.originColumns]
                    destinationColumnNames = [column.name for column in link.destinationColumns]
                    print "        - to plan %s, by columns %s in table %s, and columns %s in table %s" % (link.destinationMozaicElement.name, "(%s)" % ",".join(originColumnNames), link.originTableName, "(%s)" % ",".join(destinationColumnNames), link.destinationMozaicElement.explorationPlan.startTable.name)
                i += 1
            print "-------------------------------------------------------------------------------"
            action = raw_input("Type the id of the (two) plans to be linked (e.g.: 1,2), * to exit this loop: ") 
            if action == "*":
                break
            else:
                idxs = None
                originIdx = None
                destinationIdx = None
                
                try:
                    idxs = action.split(",")                                
                    originIdx = int(idxs[0]) - 1
                    destinationIdx = int(idxs[1]) - 1
                except: continue

                if originIdx == destinationIdx:
                    print "A plan can not link to itself"
                    continue
                
                originMozaicElement = mozaic.elements[originIdx]
                destinationMozaicElement = mozaic.elements[destinationIdx]
                linkedElementsFromDestination = [destinationOutgoingLink.destinationMozaicElement for destinationOutgoingLink in destinationMozaicElement.outgoingLinks]
               
                if originMozaicElement in linkedElementsFromDestination:
                    print "There's already a link from plan %s to plan %s" % (destinationMozaicElement.name, originMozaicElement.name)
                elif destinationMozaicElement.incomingLink is not None:
                    print "%s already has an incoming link (a mozaic element can only have 1 incoming link)" % (destinationMozaicElement.name)
                else:                    
                    print "-------------------------------------------------------------------------------"
                    print "SELECT TABLE IN PLAN %s" % originMozaicElement.name
                    print "-------------------------------------------------------------------------------"
                    tables = [table for table in originMozaicElement.explorationPlan.schema.tables.values()]
                    i = 1
                    for table in tables:                
                        print "(%d) %s" % (i, table.name)
                        i += 1
                    print "-------------------------------------------------------------------------------"
                    idx = None
                    try:
                        idx = int(raw_input("Type the id of the table: ")) - 1
                    except: continue

                    originTable = tables[idx]
                    destinationPk = destinationMozaicElement.explorationPlan.startTable.pk

                    eligibleKeys = []
                    if originTable.pk is not None and len(originTable.pk.elements) == len(destinationPk.elements):
                        eligibleKeys.append(originTable.pk)
                    for fk in originTable.allPossibleFks:
                        if len(fk.elements) == len(destinationPk.elements):
                            eligibleKeys.append(fk)
                     
                    #if len(eligibleKeys) == 0:
                    #    print "Can not link table %s in origin plan %s to table %s in destination plan %s, no compatible keys between the two tables were found." % (originTable.name, originMozaicElement.name, destinationMozaicElement.explorationPlan.startTable.name, destinationMozaicElement.name)
                        
                    choice = 'c'
                    if len(eligibleKeys) > 0:
                        choice = raw_input("Do you want to link by key or by column name (k for key, c for colum name)?: ")
                    
                    if choice == 'k':
                        print "-------------------------------------------------------------------------------"
                        print "SELECT KEY IN TABLE %s IN PLAN %s" % (originTable.name, originMozaicElement.name)
                        print "-------------------------------------------------------------------------------"
                        i = 1
                        for key in eligibleKeys:                
                            print "(%d) %s (%s)" % (i, key.name, ",".join(element.column.name for element in key.elements))
                            i += 1
                        print "-------------------------------------------------------------------------------"
                        idx = None
                        try:
                            idx = int(raw_input("Type the id of the key: ")) - 1
                        except: continue
                        selectedKey = eligibleKeys[idx]
                        originColumns = None
                                                
                        if originTable.pk is not None and originTable.pk.name == selectedKey.name:
                            originColumns = [element.column for element in originTable.pk.elements]
                        else:
                            for fk in originTable.fks:
                                if fk.name == selectedKey:
                                    originColumns = [element.column for element in fk.elements]
                                    break
                        
                        destinationColumns = [element.column for element in destinationMozaicElement.explorationPlan.startTable.pk.elements]
                        
                        if mozaic.startElement is None:
                            mozaic.startElement = originMozaicElement
                        originMozaicElement.addOutgoingLink(originTable, originColumns, destinationMozaicElement, destinationColumns)
                    elif choice == 'c':
                        originColumns = []
                        originTableColumns = [column for column in originTable.columns]
                        while True:
                            print "-------------------------------------------------------------------------------"
                            print "SELECT COLUMN IN TABLE %s IN PLAN %s" % (originTable.name, originMozaicElement.name)
                            print "-------------------------------------------------------------------------------"
                            i = 1
                            for column in originTableColumns:
                                print "(%d) %s" % (i, column.name)
                                i += 1
                            print "-------------------------------------------------------------------------------"
                            idx = None
                            try:
                                idx = int(raw_input("Type the id of the column (* to end): ")) - 1
                                originColumns.append(originTableColumns.pop(idx))
                                
                                print "-------------------------------------------------------------------------------"
                                print "SELECTED COLUMNS IN TABLE %s IN PLAN %s" % (originTable.name, originMozaicElement.name)
                                print "-------------------------------------------------------------------------------"
                                i = 1
                                for column in originColumns:
                                    print "(%d) %s" % (i, column.name)
                                    i += 1
                                print "-------------------------------------------------------------------------------"                                
                            except:
                                break                                                    
                                                                            
                        
                        destinationColumns = []
                        destinationTableColumns = [column for column in destinationMozaicElement.explorationPlan.startTable.columns]
                        for originColumn in originColumns:
                            while True:
                                print "----------------------------------------------------------------------------------------------------------------"
                                print "SELECT COLUMN IN TABLE %s IN PLAN %s that corresponds to column %s in table %s in PLAN %s" % (destinationMozaicElement.explorationPlan.startTable.name, destinationMozaicElement.name, originColumn.name, originTable.name, originMozaicElement.name)
                                print "----------------------------------------------------------------------------------------------------------------"
                                i = 1
                                for column in destinationTableColumns:
                                    print "(%d) %s" % (i, column.name)
                                    i += 1
                                print "-------------------------------------------------------------------------------"
                                idx = None
                                try:
                                    idx = int(raw_input("Type the id of the column: ")) - 1
                                    destinationColumns.append(destinationTableColumns.pop(idx))                                    
                                    break
                                except: continue
                                
                            print "-------------------------------------------------------------------------------"
                            print "SELECTED COLUMNS IN TABLE %s IN PLAN %s" % (destinationMozaicElement.explorationPlan.startTable.name, destinationMozaicElement.name)
                            print "-------------------------------------------------------------------------------"
                            i = 1
                            for column in destinationColumns:
                                print "(%d) %s" % (i, column.name)
                                i += 1
                            print "-------------------------------------------------------------------------------"                                                                
                        
                        if mozaic.startElement is None:
                            mozaic.startElement = originMozaicElement
                        originMozaicElement.addOutgoingLink(originTable, originColumns, destinationMozaicElement, destinationColumns)
                        

        if mozaic.startElement is not None:
           mozaic.name = raw_input("Type the name of this mozaic: ")
           mozaic.description = raw_input("Type the description of this mozaic: ")  
           filename = raw_input("Type the name of the file to save the mozaic: ") + ".mzc"
             
           ouf = open(filename, 'w')
           cPickle.dump(mozaic, ouf)                
           ouf.close()                
    
           print "mozaic %s has been saved as %s" % (mozaic.name, os.path.abspath(filename))
        else:
           print "No mozaic was created."
    except optionParser.error, msg:
        print msg
        return 2           
             
class Mozaic:
    def __init__(self):
        self.elements = []
        self.name = ""
        self.description = ""    
        self.startElement = None

    def addElement(self, element):
        dupFound = 0
        for elmt in self.elements:
            if elmt.name == element.name:
                dupFound += 1
        if dupFound > 0:
            element.name = "%s (%d)" % (element.name, dupFound + 1)
        self.elements.append(element)

class MozaicElement:
    def __init__(self, explorationPlan):
        self.name = explorationPlan.name
        self.explorationPlan = explorationPlan
        self.incomingLink = None
        self.outgoingLinks = []

    def addOutgoingLink(self, originTable, originColumns, destinationMozaicElement, destinationColumns):
        mozaicLink = MozaicLink(self, originTable.name, originColumns, destinationMozaicElement, destinationColumns)
        if destinationMozaicElement.incomingLink is not None: raise "destinationMozaicElement.incomingLink is not None"         
        destinationMozaicElement.incomingLink = mozaicLink
        self.outgoingLinks.append(mozaicLink)

class MozaicLink:
    def __init__(self, originMozaicElement, originTableName, originColumns, destinationMozaicElement, destinationColumns):
        self.originMozaicElement = originMozaicElement
        self.originTableName = originTableName
        self.originColumns = originColumns        
        self.destinationMozaicElement = destinationMozaicElement
        self.destinationColumns = destinationColumns

if __name__ == '__main__':
    sys.exit(main(sys.argv))
