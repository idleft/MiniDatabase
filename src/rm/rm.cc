
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	_pfm = PagedFileManager::instance();
	_rbfm = RecordBasedFileManager::instance();

	createTableCatalog();
	createColumnCatalog();
	createIndexCatalog();

	if( _pfm->fileExists( TABLE_CATALOG_FILE_NAME  ) )
		loadCatalog();
	else
	{
		createCatalogFile( "table", tableCatalog );
		createCatalogFile( "column", columnCatalog );
		createCatalogFile( "index", indexCatalog );
	}

}

RelationManager::~RelationManager()
{
	_rm = NULL;

	map< string, map<int, RID> >:: iterator it;
//	for( it= tableRIDMap.begin(); it != tableRIDMap.end(); it++)
//		delete it->second;
	tableRIDMap.clear();

	map< int, map<int, RID> >:: iterator cit;
//	for( cit= columnRIDMap.begin(); cit != columnRIDMap.end(); cit++)
//			delete cit->second;
	columnRIDMap.clear();

}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC result = -1;
	// check reserved table names
	if( tableName.compare("table") == 0 ||
			tableName.compare("column") == 0 ||
			tableName.compare("index") == 0 )
	{
		cout << "Not allowed table name=" << tableName << "in the system. try different name" << endl;
		return result;
	}

    result = createCatalogFile( tableName, attrs );

    return result;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RC result = -1;

	FileHandle fileHandle;

	map<int,RID> *tableRID = &tableRIDMap[tableName];
	int tableID =  tableRID->begin()->first;

	// delete column
	result =  _rbfm->openFile( COLUMN_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
	{
		_rbfm->closeFile( fileHandle );
		return result;
	}

	map<int,RID> *columnRID = &columnRIDMap[tableID];

	RID rid;
	map<int,RID>::iterator it;

	for(it=(*columnRID).begin(); it != (*columnRID).end();++it)
	{
		rid = it->second;
		result = _rbfm->deleteRecord( fileHandle, columnCatalog, rid );
		if( result != 0 ) {
			_rbfm->closeFile( fileHandle );
			return result;
		}

	}

	delete( columnRID );
	columnRIDMap.erase( tableID );

	result = _rbfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	// delete table
	result =  _rbfm->openFile( TABLE_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
	{
		_rbfm->closeFile( fileHandle );
		return result;
	}

	rid =  tableRID->begin()->second;
	result = _rbfm->deleteRecord( fileHandle, tableCatalog, rid );
	if( result != 0 )
		return result;

	delete( tableRID );
	tableRIDMap.erase( tableName );

    return result;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// fix the recursive call
	// should read the table info from columnCatalog and return
	short res = -1;
	if(tableAttributesCache.find(tableName) != tableAttributesCache.end()){
		attrs = tableAttributesCache[tableName];
		res = 0;
	}
	return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> tableAttributes;
    getAttributes(tableName, tableAttributes);
    // Initialize fileHandle everytime?
    _rbfm->insertRecord(fileHandle, tableAttributes, data, rid);
    return 0;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	RC result = -1;
	result = _rbfm->deleteRecords(fileHandle);
    return result;
}



RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = _rbfm->deleteRecord(fileHandle, tableAttributes, rid);
    return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = _rbfm->updateRecord(fileHandle, tableAttributes, data, rid);
    return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = _rbfm->readRecord(fileHandle, tableAttributes, rid, data);
    return result;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = _rbfm->readAttribute(fileHandle, tableAttributes, rid, attributeName, data);
    return result;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = _rbfm->reorganizePage(fileHandle, tableAttributes, pageNumber);
    return result;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
    return -1;
}

RC RelationManager::createTableCatalog()
{

	Attribute attr;

	attr.name = "tableID";
	attr.type = TypeInt;
	attr.length = sizeof(int);

	tableCatalog.push_back(attr);

	attr.name = "tableName";
	attr.type = TypeVarChar;
	attr.length = 256;

	tableCatalog.push_back(attr);

	attr.name = "fileName";
	attr.type = TypeVarChar;
	attr.length = 256;

	tableCatalog.push_back(attr);

	attr.name = "numOfColumns";
	attr.type = TypeInt;
	attr.length = sizeof(int);

	tableCatalog.push_back(attr);

	return 0;

}

RC RelationManager::createColumnCatalog()
{
	// [tableID][tableName][columnName][columnType][maxLength]
	Attribute attr;
	attr.name = "tableID";
	attr.type = TypeInt;
	attr.length = sizeof(int);

	columnCatalog.push_back(attr);

	attr.name = "tableName";
	attr.type = TypeVarChar;
	attr.length = 256;

	columnCatalog.push_back(attr);

	attr.name = "columnStart";
	attr.type = TypeInt;
	attr.length = 4;

	columnCatalog.push_back(attr);

	attr.name = "columnName";
	attr.type = TypeVarChar;
	attr.length = 256;

	columnCatalog.push_back(attr);

	attr.name = "columnType";
	attr.type = TypeInt;
	attr.length = sizeof(int);

	columnCatalog.push_back(attr);

	attr.name = "maxLength";
	attr.type = TypeInt;
	attr.length = sizeof(int);

	columnCatalog.push_back(attr);

	return 0;


}

RC RelationManager::createIndexCatalog()
{

	Attribute attr;

	attr.name = "indexName";
	attr.type = TypeVarChar;

	indexCatalog.push_back(attr);

	attr.name = "indexStructure";
	attr.type = TypeVarChar;

	indexCatalog.push_back(attr);

	return 0;
}

RC RelationManager::loadCatalog()
{
	RC result = -1;

	RM_ScanIterator	scanIterator;
	vector<string> attributeNames;

	RID rid;
	char* data = (char*)malloc(MAX_SIZE_OF_CATALOG_RECORD);
	char* start = data;

	int tableID = 0;
	string tableName;

	int offset = 0;

	// load table catalog
	attributeNames.push_back(tableCatalog[0].name);	// tableID
	attributeNames.push_back(tableCatalog[1].name);	// tableName

	scan( "table", tableCatalog[0].name, NO_OP, NULL, attributeNames, scanIterator);

	while( scanIterator.getNextTuple( rid, data) != RM_EOF )
	{
		// [tableID][tableName][catFileName][numOfColums]
		memcpy( &tableID, data,  sizeof(int));
		offset += sizeof(int);

		int tableNameLen = 0;
		memcpy( &tableNameLen, data+offset, sizeof(int));
		offset += sizeof(int);

		memcpy( &tableName, data+ offset, tableNameLen);
		offset += tableNameLen;

		map<int, RID> *tableRID = new map<int, RID>();
		(*tableRID)[tableID] = rid;

		tableRIDMap[tableName] = *tableRID;
		data = start;
	}

	// load column catalog
	attributeNames.clear();

	attributeNames.push_back(columnCatalog[0].name);	// tableID
	attributeNames.push_back(columnCatalog[2].name);	// columnStart

	scan( "column", columnCatalog[0].name, NO_OP, NULL, attributeNames, scanIterator);

	int columnStart = 0;

	while( scanIterator.getNextTuple( rid, data ) != RM_EOF )
	{
		// [tableID][tableName][columnStart][columnName][columnType][maxLength]
		memcpy( &tableID, data,  sizeof(int));
		offset += sizeof(int);

		memcpy( &columnStart, data+offset,  sizeof(int));
		offset += sizeof(int);

		// tableID already exists in the column map
		if( columnRIDMap.find(tableID) != columnRIDMap.end() )
		{
			// Handle later
		}
		else
		{
			map<int, RID> *columnRID = new map<int, RID>();
			(*columnRID)[columnStart] = rid;

			columnRIDMap[tableID] = *columnRID;
		}

		data = start;

	}

	scanIterator.close();

	// load index catalog
	/*
	while( scanIterator.getNextTuple( rid, data) != RM_EOF )
	{

	}
	*/

	return 0;
}

RC RelationManager::createCatalogFile(const string& tableName, const vector<Attribute>& attrVector)
{
	RC result = -1;
	FileHandle fileHandle;

	string catFileName = tableName + ".tbl";

	// CREATE TABLE/INDEX FILE. BUT DO NOT CREATE COLUMN CATALOG FIRST.
	if( catFileName.compare( COLUMN_CATALOG_FILE_NAME ) != 0 )
	{
		result = _rbfm->createFile( catFileName );
		if( result != 0 )
			return result;
	}

	// TABLE CATALOG EXISTS. CREATE COLUMN CATALOG
	if( catFileName.compare( TABLE_CATALOG_FILE_NAME)  == 0 )
	{
		result =  _rbfm->createFile( COLUMN_CATALOG_FILE_NAME);
		if( result != 0 )
			return result;
	}

	// INSERT TABLE CATALOG ENTRIES
	result = _rbfm->openFile( TABLE_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
		return result;

	RID rid;
	result = insertTableEntry( TABLE_ID, tableName, catFileName, fileHandle, attrVector.size(), rid);
	if( result != 0 )
		return result;

	map<int, RID> *tableRID = new map<int, RID>();
	(*tableRID)[TABLE_ID] = rid;

	tableRIDMap[tableName] = *tableRID; // modified to fix the pointer to object

	result = _rbfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	// INSERT COLUMN CATALOG ENTRIES
	result = _rbfm->openFile( COLUMN_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
		return result;

	map<int, RID> *columnRID = new map<int, RID>();
	for(int i = 0; i < (int)attrVector.size(); i++)
	{
		result = insertColumnEntry( TABLE_ID, tableName, i+1, attrVector.at(i).name, attrVector.at(i).type, attrVector.at(i).length, fileHandle, rid);
		(*columnRID)[i] = rid;
	}
	columnRIDMap[TABLE_ID] = *columnRID; // same with line362

	result = _rbfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	TABLE_ID += 1;

	// INSERT INDEX CATALOG ENTRIES

	return result;
}

RC RelationManager::insertTableEntry( int tableID, string tableName, string catFileName, FileHandle &fileHandle, int size, RID &rid)
{
	RC result = -1;

	int offset = 0;

	int catalogSize = getCatalogSize(tableCatalog);
	char* data = (char*)malloc(catalogSize);

	// [tableID][tableName][catFileName][numOfColums]
	memcpy( (void*)data, &tableID, sizeof(int));
	offset += sizeof(int);

	int varCharLen = tableName.length();
	memcpy( data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( data + offset, &tableName, varCharLen );
	offset += varCharLen;

	varCharLen = catFileName.length();
	memcpy( data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( data + offset, &catFileName, catFileName.length() );
	offset += varCharLen;

	int numOfColumns = tableCatalog.size();
	memcpy( data + offset, &numOfColumns, sizeof(int));
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, tableCatalog, data, rid );
	if( result != 0 )
		return result;

	free( data );

	return result;
}

RC RelationManager::insertColumnEntry(int tableID, string tableName, int columnStart, string columnName, AttrType columnType, AttrLength maxLength, FileHandle &fileHandle, RID& rid)
{
	RC result = -1;
	int offset = 0;

	int catalogSize = getCatalogSize(columnCatalog);
	char* data = (char*)malloc(catalogSize);

	// [tableID][tableName][columnName][columnType][maxLength]
	memcpy( data + offset, &tableID, sizeof(int));
	offset += sizeof(int);

	int varCharLen = tableName.length();
	memcpy( data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( data + offset, &tableName, varCharLen);
	offset += varCharLen;

	memcpy( data + offset, &columnStart, sizeof(int));
	offset += sizeof(int);

	varCharLen = columnName.length();
	memcpy( data + offset, &varCharLen, sizeof(int) );
	offset += sizeof(int);
	memcpy( data + offset, &columnName, varCharLen );
	offset += varCharLen;

	memcpy( data + offset, (int*)&columnType, sizeof(int) );
	offset += sizeof(int);

	memcpy( data + offset, &maxLength, sizeof(int));
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, columnCatalog, data, rid );
	if( result != 0 )
		return -1;

	free( data );

	return result;
}

unsigned RelationManager::getCatalogSize(vector<Attribute> catalog)
{
	unsigned length = 0;

	for(int i=0; i < (int)catalog.size(); i++)
	{
		length += catalog.at(i).length;

		cout << "i=" << i << " size=" << length << endl;

		if( catalog.at(i).type == TypeVarChar )
			length +=  sizeof(int);

	}

	return length;
}
