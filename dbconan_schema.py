import datetime
import sys

class Schema:
    def __init__(self, dbHost, dbPort, dbName, dbUser, dateOfCreation, description = None):
        if dbHost is None: raise "dbHost can not be None"
        if dbPort is None: raise "dbPort can not be None"
        if dbName is None: raise "dbName can not be None"
        if dbUser is None: raise "dbUser can not be None"
        if dateOfCreation is None: raise "dateOfCreation can not be None"
        if isinstance(dateOfCreation, datetime.datetime) == False:
            raise "dateOfCreation must be of type datetime.datetime" 
        
        self.dbHost = dbHost
        self.dbPort = int(dbPort)
        self.dbName = dbName
        self.dbUser = dbUser
        self.dateOfCreation = dateOfCreation
        self.description = description
        
        self.tables = {}
        
    def addTable(self, table):
        if table is None: raise "table can not be None"
        if self.tables.has_key(table.name):
            raise "table %s is already registered in the schema" % table.name
        
        self.tables[table.name] = table
        
    def getTable(self, tableName):
        if tableName is None: raise "tableName can not be None"
        return self.tables.get(tableName)

    def removeTable(self, tableName):
        if tableName is None: raise "tableName can not be None"
        del self.tables[tableName]
    
class Table:
    def __init__(self, name, columnNames, columnTypes, columnKeyNames, pkName, pkColumnNames):
        if name is None: raise "name can not be None"
        if columnNames is None: raise "columnNames can not be None"
        if columnTypes is None: raise "columnTypes can not be None"
        if columnKeyNames is None: raise "columnKeyNames can not be None"
        if len(columnNames) < 1: raise "number of columns must be at least 1"
        if len(columnNames) != len(columnTypes): raise "len(columnNames) != len(columnTypes)"
        
        #TODO: there shouldn't be duplicate in the columns (the names)
        columns = []
        columnPosition = 1
        for columnName in columnNames:
            column = Column(columnName, columnTypes[columnPosition - 1], columnKeyNames[columnPosition - 1], columnPosition)
            columns.append(column)
            columnPosition += 1
        
        pk = None
        if pkName is None:
            if len(pkColumnNames) > 0:
                raise "pkColumnNames must be zero-length when pkName is None"
        else:
            if len(pkColumnNames) == 0:
                raise "pkColumnNames must not be zero-length when pkName is not None"
            if len(pkColumnNames) > len(columnNames):
                raise "length pkColumnNames must not be greater than that of columnNames"
            
            pkElements = []
            pkElementPosition = 1
            for pkColumnName in pkColumnNames:                
                for column in columns:
                    if column.name == pkColumnName:
                        pkElement = KeyElement(column, pkElementPosition)
                        pkElementPosition += 1
                        pkElements.append(pkElement)
                        break
                else:
                    raise "a column named %s can not be found in the table %s" % (pkColumnName, name)
        
            pk = PrimaryKey(self, pkName, pkElements)
        
        self.name = name
        self.columns = columns
        self.pk = pk
        self.fks = []
        self.allPossibleFks = []
        self.allPossibleFksLocked = False

    def isConnected(self):
        anyOutgoing = len(self.fks) > 0
        anyIncoming = (self.pk is not None) and len(self.pk.fks) > 0

        return anyOutgoing or anyIncoming

    def spawnLite(self):
        columnNames = []
        columnTypes = []
        columnKeyNames = []
        pkColumnNames = []
        pkName = None

        for col in self.columns:
            columnNames.append(col.name)
            columnTypes.append(col.type)
            columnKeyNames.append(col.keyName)

        if self.pk is not None:
            for element in self.pk.elements:
                pkColumnNames.append(element.column.name)    
            pkName = self.pk.name
        
        liteTable = Table(self.name, columnNames, columnTypes, columnKeyNames, pkName, pkColumnNames)
        liteTable.allPossibleFks = self.allPossibleFks
        liteTable.allPossibleFksLocked = True
        return liteTable
        
    def referencesFrom(self, referringTable):
        referringFks = []
        for fk in self.pk.fks:
            if fk.table is referringTable:
                referringFks.append(fk)

        if len(referringFks) < 1: return None
        return referringFks
        
    def referencesTo(self, referredTable):
        referringFks = []
        for fk in self.fks:
            if fk.pk.table is referredTable:
                referringFks.append(fk)

        if len(referringFks) < 1: return None
        return referringFks

    def getColumn(self, name):
        for col in self.columns:
            if col.name == name:
                return col
        else:
            return None

    #forward-looking
    def disconnect(self, fkName):
        if fkName is None: raise "fkName can not be null"

        fk = None
        i = 0
        for existingFk in self.fks:
            print existingFk.name
            if existingFk.name == fkName:
                existingFk.pk.table.unregisterIncomingConnection(existingFk)
                self.fks.pop(i)
                break
            i += 1      
        else:
            raise "fk with name %s can not be found in the table %s" % (fkName, self.name)
        
    #forward-looking
    def connect(self, referredTable, fkName, fkColumnNames):
        if referredTable is None: raise "referredTable can not be null"
        if fkColumnNames is None: raise "fkColumnName can not be null"        
        if len(fkColumnNames) is None: raise "fkColumnName must have at least 1 entry"
        if len(fkColumnNames) != len(referredTable.pk.elements):
            raise "The length of fkColumnNames != referredTable.pk.elements"
        
        fkElements = []
        fkElementPosition = 1
        for fkColumnName in fkColumnNames:
                for column in self.columns:
                    if column.name == fkColumnName:
                        correspondingColumn = referredTable.pk.elements[fkElementPosition - 1].column
                        #if column.type != correspondingColumn.type:
                        #    raise "The type of column %s in this table (%s) doesn't match the type of column %s in table %s" % (column.name, self.name, referredTable.name, correspondingColumn.name)                        
                        fkElement = KeyElement(column, fkElementPosition)
                        fkElementPosition += 1
                        fkElements.append(fkElement)
                        break
                else:
                    raise "a column named %s can not be found in the table %s" % (fkColumnName, name)
                                
        fksReferringToReferredTable = self.referencesTo(referredTable)
        if fksReferringToReferredTable is not None:
            for fk in fksReferringToReferredTable:
                if fk.name == fkName:
                    raise "Foreign key with name %s already exists in this table (%s)" % (fkName, self.name)
                if fk.elements == fkElements:
                    raise "this table (%s) is already connected to table %s with columns: %s" % (self.name, referredTable.name, fkColumnNames)
                
        fk = ForeignKey(self, fkName, fkElements, referredTable.pk)
        self.fks.append(fk)
        if self.allPossibleFksLocked == False: 
            tableLite = fk.table.spawnLite()
            tableLite2 = fk.pk.table.spawnLite()
            fkLite = ForeignKey(tableLite, fk.name, fk.elements, tableLite2.pk)
            self.allPossibleFks.append(fkLite)
        referredTable.registerIncomingConnection(fk)

    def unregisterIncomingConnection(self, fkInReferringTable):
        if fkInReferringTable is None: raise "fkInReferringTable can not be None"
        if self.referencesFrom(fkInReferringTable.table) is None:
            raise "Table %s is not referring to this table (%s)" % (fkInReferringTable.table.name, self.name)

        i = 0
        for referringFk in self.pk.fks:
            if fkInReferringTable.name == referringFk.name:
                self.pk.fks.pop(i)
                break
            i += 1
                
    def registerIncomingConnection(self, fkInReferringTable):
        if fkInReferringTable is None: raise "fkInReferringTable can not be None"
        if fkInReferringTable.table.referencesTo(self) is None:
            raise "Table %s is not referring to this table (%s)" % (fkInReferringTable.table.name, self.name)
        
        for referringFk in self.pk.fks:
            if fkInReferringTable.name == referringFk.name:
                raise "foreign key with name %s is already registered in this table (%s)" % (referringFk.name, self.name)
            
        self.pk.fks.append(fkInReferringTable)
        
class Column:
    def __init__(self, name, type, keyName, position, description=None):
        if name is None: raise "name can not be None"
        if type is None: raise "type can not be None"
        if position is None: raise "position can not be None"
        if int(position) < 1: raise "position can not be less than 1"
        
        self.name = name
        self.type = type
        self.keyName = keyName
        self.position = int(position)
        self.description = description        
        
    def __cmp__(self, otherColumn):
        if isinstance(otherColumn, Column) == False: return False
        return otherColumn.name == self.name
    
    def __str__(self):
        return self.name
    
class KeyElement:
    def __init__(self, column, position):                
        self.column = column
        self.position = int(position)
    def __cmp__(self, otherKeyElement):
        if isinstance(otherKeyElement, KeyElement) == False: return False
        return otherKeyElement.column == self.column
    def __str__(self):
        return self.column.name
        
class PrimaryKey:
    def __init__(self, table, name, elements):
        self.table = table
        self.name = name
        self.elements = elements
        self.fks = []
        
    def __str__(self):
        return self.name    
        
class ForeignKey:
    def __init__(self, table, name, elements, pk):
        self.table = table
        self.name = name
        self.elements = elements
        self.pk = pk
        
    def __str__(self):
        return self.name    
