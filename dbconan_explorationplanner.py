import optparse
import sys
import cPickle
import traceback
import datetime
import os.path
from collections import deque
from dbconan_schema import Table, ForeignKey, PrimaryKey, Column, KeyElement, Schema

def main(argv):
    print "***********************************************"
    print "DB CONAN 1.0 - Schema Planner"
    print "Raka Angga Jananuraga - raka.angga@gmail.com"
    print "***********************************************"
    print 
    
    try:
        optionParser = optparse.OptionParser("usage: %prog [options]")        
        optionParser.add_option("-s", "--schema", dest="schema", help="The name of the schema file.", type="string")                
        optionParser.add_option("-x", "--plan", dest="plan", help="The name of the exploration-plan file.", type="string")
        
        opts, args = optionParser.parse_args(argv)

	if opts.schema is not None and opts.plan is not None:
            print "Specify either schema or plan, not both."
            sys.exit(0)                
	elif opts.schema is None and opts.plan is None:
            print "Specify either schema or plan."
            sys.exit(0)                

        schemaSubset = None
        explorationPlan = None
        if opts.schema is not None:
            inf = open(opts.schema)
            schemaSubset = cPickle.load(inf)
            inf.close() 
        else:
            inf = open(opts.plan)
            explorationPlan = cPickle.load(inf)
            schemaSubset = explorationPlan.schema
            inf.close()          

        print "-------------------------------------------------------------------------------"
        print "SCHEMA"
        print "-------------------------------------------------------------------------------"
        print "Name: %s" % schemaSubset.name
        if explorationPlan is not None:
            print "Start table: %s" % explorationPlan.startTable.name
            if len(explorationPlan.filters.keys()) > 0:
                print "Filters: "
                for tableName in explorationPlan.filters.keys():
                    fltrs = explorationPlan.filters[tableName]
                    if len(fltrs) > 0:
                        print "  %s:" % tableName                            
                        for fltr in fltrs:
                            print "    %s %s %s" % (fltr.column.name, fltr.operator, str(fltr.value))
        print "DB host: %s" % schemaSubset.dbHost
        print "DB port: %s" % schemaSubset.dbPort
        print "DB name: %s" % schemaSubset.dbName
        print "DB user: %s" % schemaSubset.dbUser
        print "Date of creation: %s" % schemaSubset.dateOfCreation
        print "Description: %s" % schemaSubset.description
        print "-------------------------------------------------------------------------------"
        
        startTable = None
        if opts.schema is not None:
            startTable = selectTable(schemaSubset)
        else:
            startTable = explorationPlan.startTable

        pkColumnValuesList = []
        filters = {}	
                
        choice = None
        if opts.schema is not None:
            choice = raw_input("Do you want to specify filters for tables in the schema (y/n)?: ")
        else:
            filters = explorationPlan.filters
            choice = raw_input("Do you want to modify filters for tables in the schema (y/n)?: ")

        if choice == 'y':                
            for table in schemaSubset.tables.values():
                if table.name != startTable.name:
                    columnsForFilters = []
                    for column in table.columns:        
                        if column.keyName is None:
                            columnsForFilters.append(column)            
	
                    if len(columnsForFilters) > 0:
                        fltrList = None
                        if filters.has_key(table.name):
                            fltrList = filters[table.name]
                        else:
                            fltrList = []
                        filters[table.name] = specifyFilters(table, fltrList) #map (tableName, list of filters). a filter is: column, operator, value

        name = raw_input("Type the name of this exploration plan: ")
        description = raw_input("Type the description of this exploration plan: ")

        explorationPlan = ExplorationPlan(name, schemaSubset, startTable, pkColumnValuesList, filters, description, datetime.datetime.now())

        filename = raw_input("Type the filename for this exploration plan: ") + ".xpp"

        ouf = open(filename, 'w')
        cPickle.dump(explorationPlan, ouf)                
        ouf.close()                

        print "Exploration plan saved as %s." %  os.path.abspath(filename)
                        
    except optionParser.error, msg:
        print msg
        return 2

def specifyFilters(table, filters):    
    while True:
        print "-------------------------------------------------------------------------------"
        print "FILTER FOR %s" % table.name
        print "-------------------------------------------------------------------------------"
        i = 1
        for fltr in filters:
            print "(%d) %s %s %s" % (i, fltr.column.name, fltr.operator, fltr.value)
            i += 1
        print "-------------------------------------------------------------------------------"
        action = raw_input("Press 'a' to add filter, press 'r' to remove filter  (* to leave this menu): ") 
        if action == "*":
            break
        elif action == "r":
	    idx = None
	    try:
		idx = int(raw_input("Type the number of filter to be removed: ")) - 1
	    except: continue
            filters.pop(idx)
        elif action == "a":
            column = selectColumnForFilter(table)
            if column is not None:
                print "-------------------------------------------------------------------------------"
                print "FILTER FOR %s (%s)" % (column.name, column.type)
                print "-------------------------------------------------------------------------------"
                operator = raw_input("Type the operator of the filter (read the manual, type carefully, make no mistake): ") 
                value = raw_input("Type the value of the filter (read the manual, type carefully, make no mistake): ")
                fltr = Filter(column, operator, value)
                filters.append(fltr)                    

    return filters

def selectColumnForFilter(table):
    columnsForFilters = []
    for column in table.columns:
	#remove the restriction of only being able to use non-key columns
	columnsForFilters.append(column)            
        #if column.keyName is None:
        #    columnsForFilters.append(column)            

    if len(columnsForFilters) == 0:
       return None 

    print "-------------------------------------------------------------------------------"
    print "COLUMNS IN %s" % table.name
    print "-------------------------------------------------------------------------------"
    i = 1
    for column in columnsForFilters:
        print "(%d) %s (%s)" % (i, column.name, column.type)
        i += 1
    action = raw_input("Type the number of the column for the filter  (* to leave this menu): ") 
    if action == "*":
        return None
    else:
	return columnsForFilters[int(action) - 1]

class ExplorationPlan:
    def __init__(self, name, schema, startTable, pkColumnValuesList, filters, description, dateOfCreation, nonPKColumnValuesList = None, nonPKColumnsForQuery = None):
        self.name = name
        self.schema = schema
        self.startTable = startTable
        self.pkColumnValuesList = pkColumnValuesList
        self.filters = filters
        self.description = description
        self.dateOfCreation = dateOfCreation
	self.nonPKColumnValuesList = nonPKColumnValuesList
	self.nonPKColumnsForQuery = nonPKColumnsForQuery

class Filter:
    def __init__(self, column, operator, value):
        self.column = column
        self.operator = operator
        self.value = value

def selectTable(schemaSubset):
    print
    print "-------------------------------------------------------------------------------"
    print "AVAILABLE TABLES"
    print "-------------------------------------------------------------------------------"

    i = 1
    tableNames = schemaSubset.tables.keys()
    for tableName in tableNames:
        print "(" + str(i) + ") " + tableName
        table = schemaSubset.tables[tableName]
        for fk in table.fks:
            print "  to %s via %s" % (fk.pk.table.name, fk.name)
        if table.pk is not None:
            for fk in table.pk.fks:
                for fkOut in table.fks:
                    if fkOut.name == fk.name:
                        break
                else:
                    print "  from %s via %s" % (fk.table.name, fk.name)                                
        i += 1
    print "-------------------------------------------------------------------------------"

    while True:
        choice = raw_input("Select the start table, type a number from 1 to %d. (* to exit): " % (i-1)) 
        if choice == '*':
            sys.exit(0)
        else:         
            try:       
                return schemaSubset.tables[tableNames[int(choice) - 1]]
            except Exception, e:
                print "Try better, watch your fingers"
                               
if __name__ == '__main__':
    sys.exit(main(sys.argv))      
