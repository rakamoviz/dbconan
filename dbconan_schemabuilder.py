#!/usr/bin/env python
import cx_Oracle
import optparse
import sys
import datetime
import cPickle
import traceback
import os.path
from dbconan_schema import Table, Column, ForeignKey, PrimaryKey, KeyElement, Schema
from dbconan_explorationrunner import ZigzagExplorationStrategy

def main(argv):
    print "***********************************************"
    print "DB CONAN 1.0 - Schema Builder"
    print "Raka Angga Jananuraga - raka.angga@gmail.com"
    print "***********************************************"
    print 
    
    try:
        optionParser = optparse.OptionParser("usage: %prog [options]")        
        optionParser.add_option("-H", "--host", dest="dbHost", help="The hostname of the database to analyze", type="string")
        optionParser.add_option("-p", "--port", dest="dbPort", help="The port number of the database to analyze.", type="int")        
        optionParser.add_option("-u", "--user", dest="dbUser", help="The user name to access the database to analyze.", type="string")        
        optionParser.add_option("-a", "--password", dest="dbPassword", help="The password to access the database to analyze.", type="string")
        optionParser.add_option("-o", "--owner", dest="dbOwner", help="The owner of the database to analyze.", type="string")        
        optionParser.add_option("-n", "--dbName", dest="dbName", help="The name of the database to analyze.", type="string")
        optionParser.add_option("-s", "--dbSchema", dest="dbSchema", help="The name of the scheme to edit (optional).", type="string")        
        
        opts, args = optionParser.parse_args(argv)                

        if opts.dbHost != None and opts.dbPort != None and opts.dbUser != None and opts.dbPassword != None and opts.dbName != None and opts.dbOwner != None:
            schemaBuilder = SchemaBuilder()
            schema = schemaBuilder.build(opts.dbHost, opts.dbPort, opts.dbName, opts.dbUser, opts.dbPassword, datetime.datetime.now(), opts.dbOwner)
            
	    existingSchemaSubset = None

            if opts.dbSchema is not None:
                inf = open(opts.dbSchema)
                existingSchemaSubset = cPickle.load(inf)                
                inf.close()
                                    
                for table in existingSchemaSubset.tables.values():
                    comparingTable = schema.getTable(table.name)
                    if comparingTable is None: raise "comparing table is none"                
                    if table.pk is not None and comparingTable.pk is not None:
                        if len(table.pk.elements) != len(comparingTable.pk.elements):
                            raise "the length of the primary key %s in table %s is not the same" % (table.pk.name, table.name)
                        for elm1 in table.pk.elements:
                            for elm2 in comparingTable.pk.elements:
                                sys.stdout.flush()
                                if elm1.column.name == elm2.column.name: break
                            else: raise "the name of the column in pk doesn't match"
                    elif (table.pk is None and comparingTable.pk is not None) or (table.pk is not None and comparingTable.pk is None):
                        raise "pk doesn't match"
                
                    for fk1 in table.fks:
                        for fk2 in comparingTable.fks:
                            if fk1.name == fk2.name:
                                if len(fk1.elements) != len(fk2.elements):
                                    raise "the length of the fk key %s in table %s is not the same" % (fk1.name, table.name)
                                for elm1 in fk1.elements:
                                    for elm2 in fk2.elements:
                                        if elm1.column.name == elm2.column.name: break
                                    else: raise "the name of the column in fk doesn't match"

                                break
                        else:
                            raise "fk %s can not be found in table in database" % fk1.name
                    
                    if len(table.columns) != len(comparingTable.columns): raise "lenght of columsn doesn't match"
                    for column1 in table.columns:
                        for column2 in comparingTable.columns:
                            if column1.name == column2.name: break
                        else: raise "the name of the column in doesn't match"                                    

            consoleUI = ConsoleUI(schema, ZigzagExplorationStrategy())
            consoleUI.start(existingSchemaSubset)
        else:
            print "Missing options. Type %s -h for execution instruction." % argv[0]
            return 2
                
    except optionParser.error, msg:
        print msg
        return 2   

class ConsoleUI:
    def __init__(self, schema, explorationStrategy):
        if schema is None: raise "schema can not be None"                
        self.schema = schema
        self.explorationStrategy = explorationStrategy

    def start(self, existingSchemaSubset):
        if existingSchemaSubset is not None:
            startTableName = None
            schemaSubset = existingSchemaSubset
            while True:            
                self.startBuildingSchemaSubset(startTableName, schemaSubset)            
                print
                print "---------"
                print "MAIN MENU"
                print "---------"
                startTableName = raw_input("Type the name of a table in your conceived schema-segment. We'll start the building from there. (* for 'exit'): ")            
                if startTableName == '*':
                    break
                else:
                    schemaSubset = None
        else:        
            while True:
                print
                print "---------"
                print "MAIN MENU"
                print "---------"
                startTableName = raw_input("Type the name of a table in your conceived schema-segment. We'll start the building from there. (* for 'exit'): ")            
                if startTableName == '*':
                    break
                else:
                    self.startBuildingSchemaSubset(startTableName, None)

        print "Console UI, exiting...."

    def startBuildingSchemaSubset(self, startTableName, schemaSubset):
        if schemaSubset is None:        
            fullTable = self.schema.getTable(startTableName)

            if fullTable is None:
                print "The schema doesn't contain a table named %s" % startTableName
                return

            liteTable = fullTable.spawnLite()
            schemaSubset = Schema(self.schema.dbHost, self.schema.dbPort, self.schema.dbName, self.schema.dbUser, self.schema.dateOfCreation)
            schemaSubset.addTable(liteTable)
 
        ret = self.displayTablesInSchemaSubset(schemaSubset)
        if ret is not None:
            if len(schemaSubset.tables) < 1:
                print "The schema subset doesn't have any tables, it's not going to be saved."
            else:
                name = raw_input("Type the name of this schema subset: ")
                description = raw_input("Type the description of this schema subset: ")  
                schemaSubset.name = name
                schemaSubset.description = description
                filename = raw_input("Type the name of the file to save the schema subset: ") + ".scm"
            
                ouf = open(filename, 'w')
                cPickle.dump(schemaSubset, ouf)                
                ouf.close()                
    
                print "Schema subset %s has been saved as %s" % (name, os.path.abspath(filename))

    def displayTablesInSchemaSubset(self, schemaSubset):
        while True:
            print
            print "---------------"
            print "SELECTED TABLES"
            print "---------------" 

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
            print "--------------"                

            
            choice = raw_input("Jump to a table, type a number from 1 to %d. (* for finish and save): " % (i-1)) 
            if choice == '*':
                return schemaSubset
            else:
                idx = None
                try: 
                    idx = int(choice) - 1
		    selectedTable = schemaSubset.tables[tableNames[idx]]
		    self.displayExistingAssociationsFromAndToTable(selectedTable, schemaSubset)
    
		    if len(schemaSubset.tables) < 1:
			return None        		    
                except:
                    continue                

    def displayExistingAssociationsFromAndToTable(self, selectedTable, schemaSubset):
        while True:
            print
            print "------------------------------------------------"
            print "EXISTING ASSOCIATIONS (%s)" % selectedTable.name
            print "------------------------------------------------"
            
	    existingAssociations = []
            OUT, IN = 1, 2
            i = 1
 
            for fkFromLiteTable in selectedTable.fks:
                existingAssoc = (OUT, fkFromLiteTable.pk.table, fkFromLiteTable)
                existingAssociations.append(existingAssoc)
                print "(%d) to %s via %s" % (i, existingAssoc[1].name, existingAssoc[2].name)   
                i += 1

            if selectedTable.pk is not None:
              for fkToLiteTable in selectedTable.pk.fks:
                  for xstAssoc in existingAssociations:
                      if xstAssoc[2].name == fkToLiteTable.name:
                          break
                  else:
                      existingAssoc = (IN, fkToLiteTable.table, fkToLiteTable)
                      existingAssociations.append(existingAssoc)
                      print "(%d) from %s via %s" % (i, existingAssoc[1].name, existingAssoc[2].name)     	    
                      i += 1

            if i == 1:
               ret = self.displayAdditionalAssociationsFromAndToTable(selectedTable, schemaSubset)
               if ret is None:
                   return
            else:
                try:
                    action = raw_input("Press 'a' to add association, press 'r' to remove association  (* for previous menu): ") 
                    if action == '*':
                        return
                    elif action == 'a':
                        self.displayAdditionalAssociationsFromAndToTable(selectedTable, schemaSubset)
                    elif action == 'r':                
                        choice = raw_input("Type the number of the association to be removed (* not to select anything): ") 
                        if choice == '*':
                            return
                        
                        idx = None
                        try:
                            idx = int(choice) - 1
                        except:
                            continue
                        
                        existingAssoc = existingAssociations[idx]
                        if existingAssoc[0] == OUT:
                            selectedTable.disconnect(existingAssoc[2].name)
                        elif existingAssoc[0] == IN:
                            existingAssoc[1].disconnect(existingAssoc[2].name)

                        if existingAssoc[1].isConnected() == False:                            
	    	            schemaSubset.removeTable(existingAssoc[1].name)
                        if selectedTable.isConnected() == False:                            
	                    schemaSubset.removeTable(selectedTable.name)

                        if schemaSubset.getTable(selectedTable.name) is None:
                            return
                except Exception, exc:
                    traceback.print_exc(file=sys.stdout)
                    return

    def displayAdditionalAssociationsFromAndToTable(self, selectedTable, schemaSubset):
        while True:
            print
            print "--------------------------------------------------"
            print "ADDITIONAL ASSOCIATIONS (%s)" % selectedTable.name
            print "--------------------------------------------------"

            fullTable = self.schema.getTable(selectedTable.name)
            additionalAssociations = []

            OUT, IN = 1, 2
            i = 1

            for fkFromFullTable in fullTable.fks:
                for fkFromLiteTable in selectedTable.fks:
                    if fkFromLiteTable.name == fkFromFullTable.name:
                        break
                else:
                    additionalAssoc = (OUT, fkFromFullTable.pk.table, fkFromFullTable)
                    additionalAssociations.append(additionalAssoc)                    
                    print "(%d) to %s via %s" % (i, additionalAssoc[1].name, additionalAssoc[2].name)   
                    i += 1

            if fullTable.pk is not None:
              for fkToFullTable in fullTable.pk.fks:
                  for fkToLiteTable in selectedTable.pk.fks:
                      if fkToLiteTable.name == fkToFullTable.name:
                          break
                  else:
                      for addAssoc in additionalAssociations:
                          if addAssoc[2].name == fkToFullTable.name:
                              break
                      else:
                          additionalAssoc = (IN, fkToFullTable.table, fkToFullTable)
                          additionalAssociations.append(additionalAssoc)                                        
                          print "(%d) from %s via %s" % (i, additionalAssoc[1].name, additionalAssoc[2].name)     	                        
                          i += 1

            if len(additionalAssociations) < 1:
                return

            try:
                choice = raw_input("Select an association, type a number from 1 to %d. (* for previous menu): " % (i-1)) 
                if choice == '*':
                    return
                else:
                    idx = None
                    try:
                        idx = int(choice) - 1
                    except: continue
                    
                    additionalAssoc = additionalAssociations[idx]                                        
                    if additionalAssoc[0] == OUT:                                                
                        referredTable = schemaSubset.getTable(additionalAssoc[1].name)
	    		if referredTable is None:
			    referredTable = additionalAssoc[1].spawnLite()                                
                        if self.explorationStrategy.allowAssociation(schemaSubset, selectedTable, referredTable, additionalAssoc[2].name):            
			    if schemaSubset.getTable(additionalAssoc[1].name) is None:
                                schemaSubset.addTable(referredTable)
                            fkColumnNames = [element.column.name for element in additionalAssoc[2].elements]
                            selectedTable.connect(referredTable, additionalAssoc[2].name, fkColumnNames)
                    elif additionalAssoc[0] == IN:                            
                        referringTable = schemaSubset.getTable(additionalAssoc[1].name)
                        if referringTable is None:
                            referringTable = additionalAssoc[1].spawnLite()                                
                        if self.explorationStrategy.allowAssociation(schemaSubset, selectedTable, referringTable, additionalAssoc[2].name):            
                            if schemaSubset.getTable(additionalAssoc[1].name) is None:
                                schemaSubset.addTable(referringTable)
                            fkColumnNames = [element.column.name for element in additionalAssoc[2].elements]
                            referringTable.connect(selectedTable, additionalAssoc[2].name, fkColumnNames)
                    return
            except Exception, exc:
                traceback.print_exc(file=sys.stdout)
                return
    
class SchemaBuilder:
    sqlTemplate = """select X.table_name, X.constraint_name, X.column_name, X.r_table_name, X.position from (
        select B.table_name, B.constraint_name, B.column_name, B.position, D.table_name as r_table_name, 
         B.r_constraint_name from (
           select C.table_name, A.column_name, A.position, C.constraint_name, C.r_constraint_name, M.comments from 
           ALL_CONS_COLUMNS A inner join ALL_CONSTRAINTS C on A.CONSTRAINT_NAME = C.CONSTRAINT_NAME 
           inner join ALL_COL_COMMENTS M on M.table_name=C.table_name and M.column_name=A.column_name where 
           C.constraint_type = 'R' and C.owner='%(dbOwner)s' and A.owner = '%(dbOwner)s' and M.owner = '%(dbOwner)s'
         ) B inner join ALL_CONSTRAINTS D on B.r_constraint_name = D.constraint_name where 
         D.constraint_type='P' and D.owner='%(dbOwner)s' and B.r_constraint_name in (
           select distinct(constraint_name) from ALL_CONS_COLUMNS where owner = '%(dbOwner)s'
         )
        ) X inner join ALL_CONS_COLUMNS Y on X.r_constraint_name=Y.constraint_name and X.position=Y.position 
        where Y.owner='%(dbOwner)s' order by X.table_name, X.constraint_name, X.position"""
        
    sqlTemplate2 = """select A.colno, A.cname, A.coltype, D.constraint_name, D.position as KEY_ELEMENT_POSITION, D.constraint_type from COL A 
        left outer join (select C.column_name, C.constraint_name, C.position, B.constraint_type 
        from all_constraints B inner join all_cons_columns C
        on B.constraint_name = C.constraint_name and B.table_name=C.table_name
        where B.table_name='%(tableName)s' and B.owner='%(dbOwner)s' and C.owner='%(dbOwner)s' 
        order by C.position) D on A.cname=D.column_name where A.TNAME='%(tableName)s' and 
        ((D.constraint_name is not null and D.position is not null) or (D.constraint_name is null and D.position is null)
        or (D.constraint_name is not null and D.position is null and D.constraint_type = 'C')) 
        order by A.colno"""
        
    sqlTemplate3 = """select distinct(table_name) from all_constraints where owner = '%(dbOwner)s'"""    
    
    def build(self, dbHost, dbPort, dbName, dbUser, dbPassword, dateOfCreation, dbOwner):        
        dbConnection = cx_Oracle.connect(dbUser, dbPassword, '%(dbHost)s:%(dbPort)s/%(dbName)s' % {"dbHost":dbHost, "dbPort":dbPort, "dbName":dbName})
        cursor = dbConnection.cursor()
        loadTableCursor = dbConnection.cursor()
        sql = SchemaBuilder.sqlTemplate % {"dbOwner":dbOwner}
        print "Querying database schema...",
        #print sql
        sys.stdout.flush()
        cursor.execute(sql)
        print " [Completed]"
        print "Building in-memory structure...",
        sys.stdout.flush()

        TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME, R_TABLE_NAME, KEY_ELEMENT_POSITION = range(5)        

        schema = Schema(dbHost, dbPort, dbName, dbUser, dateOfCreation)
        
        lastKeyElementPosition = 0
        outstandingReferringTable = None
        outstandingReferringTableFkName = None
        outstandingReferringTableFkColumnNames = []
        outstandingReferredTable = None
        
        for row in cursor:
            if row[KEY_ELEMENT_POSITION] <= lastKeyElementPosition:
                outstandingReferringTable.connect(outstandingReferredTable, outstandingReferringTableFkName, outstandingReferringTableFkColumnNames)
                
                lastKeyElementPosition = 0
                outstandingReferringTable = None
                outstandingReferringTableFkName = None
                outstandingReferringTableFkColumnNames = []                
                outstandingReferredTable = None
            
            referringTableName = row[TABLE_NAME]
            referredTableName = row[R_TABLE_NAME]            
            
            outstandingReferringTable = schema.getTable(referringTableName)
            if outstandingReferringTable is None:
                outstandingReferringTable = self.loadTable(referringTableName, loadTableCursor, schema, dbOwner)
                              
            outstandingReferredTable = schema.getTable(referredTableName)
            if outstandingReferredTable is None:
                outstandingReferredTable = self.loadTable(referredTableName, loadTableCursor, schema, dbOwner)
                
            outstandingReferringTableFkName = row[CONSTRAINT_NAME]
            outstandingReferringTableFkColumnNames.append(row[COLUMN_NAME])
            
            lastKeyElementPosition = row[KEY_ELEMENT_POSITION]                            

        if outstandingReferringTable is not None and outstandingReferredTable is not None:
            outstandingReferringTable.connect(outstandingReferredTable, outstandingReferringTableFkName, outstandingReferringTableFkColumnNames)
            
            
        sql = SchemaBuilder.sqlTemplate3 % {"dbOwner":dbOwner}
        cursor.execute(sql)
        for row in cursor:
            if schema.getTable(row[0]) is None:
                self.loadTable(row[0], loadTableCursor, schema, dbOwner)

        loadTableCursor.close()
        cursor.close()
        dbConnection.close()
        
        print " [Completed]"
        return schema
    
    def loadTable(self, tableName, cursor, schema, dbOwner):
        sql = SchemaBuilder.sqlTemplate2 % {"dbOwner":dbOwner, "tableName":tableName}
        cursor.execute(sql)

        COLUMN_POSITION, COLUMN_NAME, COLUMN_TYPE, CONSTRAINT_NAME, KEY_ELEMENT_POSITION, CONSTRAINT_TYPE = range(6)
        columnAttrsTuples = []
        pkName = None
        pkElementAttrsTuples = None
        
        for row in cursor:            
            if row[CONSTRAINT_TYPE] == 'U' or row[CONSTRAINT_TYPE] == 'C' or row[CONSTRAINT_TYPE] is None:
                keyName = None
                keyType = None
                colTuple = (row[COLUMN_NAME], row[COLUMN_TYPE], row[COLUMN_POSITION], keyName, keyType)
                #avoid duplicate
                for existingTuple in columnAttrsTuples:
                    if existingTuple[0] == colTuple[0]: break
                else:
                    columnAttrsTuples.append(colTuple)
            else:
                keyName = row[CONSTRAINT_NAME]
                keyType = row[CONSTRAINT_TYPE]
                colTuple = (row[COLUMN_NAME], row[COLUMN_TYPE], row[COLUMN_POSITION], keyName, keyType)
		if row[CONSTRAINT_TYPE] == 'P':
		    if pkName is None:
		        pkName = keyName
		        pkElementAttrsTuples = []
		    pkTuple = (row[COLUMN_NAME], row[KEY_ELEMENT_POSITION])
		    pkElementAttrsTuples.append(pkTuple)

                if row[CONSTRAINT_TYPE] == 'P' or row[CONSTRAINT_TYPE] == 'R':
                    i = 0
                    for existingTuple in columnAttrsTuples:
                        if existingTuple[0] == colTuple[0]: 
                            if existingTuple[4] is None:
                                columnAttrsTuples[i] = colTuple
                            elif existingTuple[4] == 'R' and colTuple[4] == 'P':
                                columnAttrsTuples[i] = colTuple
                            break
                        i += 1
                    else:
                        columnAttrsTuples.append(colTuple)
                
        
        columnAttrsTuples.sort(cmp=lambda x, y: x[2] - y[2])                
        columnNames = []
        columnTypes = []
        columnKeyNames = []
        for tuple in columnAttrsTuples:
            columnNames.append(tuple[0])
            columnTypes.append(tuple[1])
            columnKeyNames.append(tuple[3])
            
        pkColumnNames = None
        pkColumnNames = []
        if pkElementAttrsTuples is not None:
            pkElementAttrsTuples.sort(cmp=lambda x, y: x[1] - y[1])            
            for tuple in pkElementAttrsTuples:
                pkColumnNames.append(tuple[0])
               
        table = Table(tableName, columnNames, columnTypes, columnKeyNames, pkName, pkColumnNames)        
        schema.addTable(table)            
        
        return table   

if __name__ == '__main__':
    sys.exit(main(sys.argv))
