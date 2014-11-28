
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
	cout<<"Table:: "<<tableCatalog.at(0).name<<endl;

	if( _pfm->fileExists( TABLE_CATALOG_FILE_NAME  ) )
		loadCatalog();
	else
	{
		createCatalogFile( "Tables", tableCatalog );
		createCatalogFile( "Columns", columnCatalog );
		createCatalogFile( "index", indexCatalog );
	}

}

RelationManager::~RelationManager()
{
	_rm = NULL;

	map< string, map<int, RID> *>:: iterator it;
	for( it= tableRIDMap.begin(); it != tableRIDMap.end(); it++)
		delete it->second;
//	tableRIDMap.clear();

	map< int, map<int, RID> *>:: iterator cit;
	for( cit= columnRIDMap.begin(); cit != columnRIDMap.end(); cit++)
			delete cit->second;
//	columnRIDMap.clear();

}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC result = -1;
	// check reserved table names
	if( tableName.compare("Tables") == 0 ||
			tableName.compare("Columns") == 0 ||
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

	map<int,RID> *tableRID = tableRIDMap[tableName];
	int tableID =  tableRID->begin()->first;

	// delete column
	result =  _rbfm->openFile( COLUMN_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
	{
		_rbfm->closeFile( fileHandle );
		return result;
	}

	map<int,RID> *columnRID = columnRIDMap[tableID];

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

RC RelationManager::colDescriptorToAttri(void* data, Attribute &colAttri){
	int offset = sizeof(int); //skip the tableID
	int varLen = 0,columnStart;
	memcpy(&varLen, (char*) data + offset, sizeof(int));
	offset = offset + sizeof(int) + sizeof(char)*varLen;// skip tableName

	memcpy(&columnStart, (char*) data + offset, sizeof(int));
	offset += sizeof(int);

	memcpy(&varLen, (char*) data + offset, sizeof(int));
	offset += sizeof(int);

	char * nameChar = (char *)malloc(varLen+1);
	memset(nameChar,0,varLen+1);
	memcpy(nameChar, (char*)data+offset, varLen); // store columnName
	colAttri.name = string((char *)nameChar);
//	cout<<"Read attribute name: "<<colAttri.name<<endl;
	free(nameChar);
	offset = offset + sizeof(char)*varLen;

	memcpy(&colAttri.type,(char*)data+offset, sizeof(int)); // store columnType
	offset = offset + sizeof(AttrType);

	memcpy(&colAttri.length, (char*) data+offset, sizeof(int)); //store colMaxLength

	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RC result = -1;

	FileHandle fileHandle;

	attrs.clear();

	if (tableRIDMap.find(tableName) == tableRIDMap.end() )
		return result;

//	cout<<"Get attribute for table: "<< tableName <<endl;
	_rbfm->openFile(COLUMN_CATALOG_FILE_NAME, fileHandle);
	int colCatalogSize = getCatalogSize(columnCatalog);

//	cout<<"Get colCatalogSize: "<< colCatalogSize<<endl;

	map<int, RID>* tableIdSet = tableRIDMap[tableName];
//	printf("Table RID map size: %d  Table id set: %d\n",tableRIDMap.size(),tableIdSet->size());
	for(map<int, RID>::iterator iter1 = tableIdSet->begin(); iter1!=tableIdSet->end(); iter1++){

		int tableId = iter1->first; // only require tableId

		map<int, RID> *attrMap = columnRIDMap[tableId];

		for(map<int, RID>::iterator iter2 = attrMap->begin(); iter2 != attrMap->end(); iter2++){

			Attribute colAttri;

			RID attrRid = iter2->second;

			void* colDescriptor = malloc(colCatalogSize);
			result = _rbfm->readRecord(fileHandle, columnCatalog, attrRid, colDescriptor);
			if( result != 0 )
				return result;

			result = colDescriptorToAttri(colDescriptor, colAttri);
			if( result != 0 )
				return result;
			attrs.push_back( colAttri );

			free( colDescriptor );
		}
	}

	_rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RC result = -1;
	FileHandle fileHandle;
    vector<Attribute> tableAttributes;

    result = getAttributes(tableName, tableAttributes);
    if( result != 0 )
    	return result;
    result = _rbfm->openFile(tableName+".tbl",fileHandle);
    if( result != 0 )
    	return result;

    result = _rbfm->insertRecord(fileHandle, tableAttributes, data, rid);
    if( result != 0 )
    {
    	_rbfm->closeFile(fileHandle);
    	return result;
    }

    result = _rbfm->closeFile(fileHandle);

    return result;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	FileHandle fileHandle;
	RC result = -1;

    result = _rbfm->openFile(tableName+".tbl",fileHandle);
    if( result != 0 )
    	return -1;

	result = _rbfm->deleteRecords(fileHandle);
	if( result != 0 )
	{
		_rbfm->closeFile( fileHandle );
		return -1;
	}
	result = _rbfm->closeFile(fileHandle);


    return result;
}



RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;
    result = _rbfm->openFile(tableName+".tbl",fileHandle);
    if( result != 0 )
    	return result;

	result = getAttributes(tableName, tableAttributes);
	if( result != 0 )
		return result;

	result = _rbfm->deleteRecord(fileHandle, tableAttributes, rid);
	if( result != 0 )
		return result;

    result = _rbfm->closeFile(fileHandle);
    return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;
    _rbfm->openFile(tableName+".tbl",fileHandle);
//    cout<<1<<endl;
	getAttributes(tableName, tableAttributes);
//    cout<<2<<endl;
	result = _rbfm->updateRecord(fileHandle, tableAttributes, data, rid);
//    cout<<3<<endl;
    _rbfm->closeFile(fileHandle);
//    cout<<4<<endl;
    return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;

//	cout << "readTuple tableName:" << tableName << endl;

	if( tableRIDMap.find(tableName) == tableRIDMap.end() )
	{
		return result;
	}

//	cout << "readTuple after finding RIDMap result:" << result << endl;

    result = getAttributes(tableName, tableAttributes);
    if( result != 0 )
    	return result;

//    cout << "readTuple after getAttributes:" << result << endl;

    result = _rbfm->openFile(tableName+".tbl",fileHandle);
	if(result !=0)
		return result;

//    cout << "readTuple after fopenFile:" << result << endl;

    result = _rbfm->readRecord(fileHandle, tableAttributes, rid, data);
    if( result != 0 )
    {
    	_rbfm->closeFile(fileHandle);
    	return -1;
    }

//    cout << "readTuple after readRecord:" << result << endl;

    result = _rbfm->closeFile(fileHandle);

//    cout << "readTuple after closeFile:" << result << endl;
    return result;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;
    _rbfm->openFile(tableName+".tbl",fileHandle);
	getAttributes(tableName, tableAttributes);
	result = _rbfm->readAttribute(fileHandle, tableAttributes, rid, attributeName, data);
    _rbfm->closeFile(fileHandle);
    return result;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;
    _rbfm->openFile(tableName+".tbl",fileHandle);

	result = getAttributes(tableName, tableAttributes);
	if( result != 0 )
		return result;

	result = _rbfm->reorganizePage(fileHandle, tableAttributes, pageNumber);
	if( result != 0 )
		return result;

	result = _rbfm->closeFile(fileHandle);
    return result;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	FileHandle fileHandle;
	RC result = -1;

	string fileName =  tableName + ".tbl";
	result = _rbfm->openFile( fileName, rm_ScanIterator.fileHandle );
	if( result == -1 )
		return result;

	if( fileName.compare(TABLE_CATALOG_FILE_NAME) == 0 )
	{
		result = rm_ScanIterator.initialize(tableCatalog, conditionAttribute, compOp, value, attributeNames);
	}
	else if( fileName.compare(COLUMN_CATALOG_FILE_NAME) == 0 )
	{
		result = rm_ScanIterator.initialize(columnCatalog, conditionAttribute, compOp, value, attributeNames);
	}
	else
	{
		vector<Attribute>	catalogAttributes;
		result = getAttributes( tableName, catalogAttributes );
		if( result == 0 )
			result = rm_ScanIterator.initialize(catalogAttributes, conditionAttribute, compOp, value, attributeNames);
	}
	return 0;
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
	// [tableID][tableName][columnStart][columnName][columnType][maxLength]
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
	RM_ScanIterator	scanIterator;
	vector<string> attributeNames;

	RID rid;
	char* data = (char*)malloc(MAX_SIZE_OF_CATALOG_RECORD);
	char* start = data;

	int tableID = 0;

	// load table catalog
	// we the value we want is tableID and tableName
	attributeNames.push_back(tableCatalog.at(0).name);	// tableID
	attributeNames.push_back(tableCatalog.at(1).name);	// tableName
	scan( "Tables", tableCatalog.at(1).name, NO_OP, NULL, attributeNames, scanIterator);
	cout<<"Loading table..."<<endl;


	while( scanIterator.getNextTuple( rid, data ) != RM_EOF )
	{
		// [tableID][tableName][catFileName][numOfColums]
		memcpy( &tableID, data,  sizeof(int));

		data = data + sizeof(int);

		int tableNameLen;
		memcpy( &tableNameLen, data, sizeof(int));
		data = data+sizeof(int);

		string tableName = string(data, tableNameLen);
		cout<<"load table name: "<<tableName<<" table id: "<<tableID<<endl;

		map<int, RID> *tableRID = new map<int, RID>();
		(*tableRID)[tableID] = rid;

		tableRIDMap[tableName] = tableRID;
		data = start;
	}
	scanIterator.close();

	// load column catalog
	attributeNames.clear();

	attributeNames.push_back(columnCatalog.at(0).name);	// tableID
	attributeNames.push_back(columnCatalog.at(2).name);	// columnStart

	scan( "Columns", columnCatalog.at(2).name, NO_OP, NULL, attributeNames, scanIterator);

	int columnStart = 0;
	int pCounter = 0;

	while( scanIterator.getNextTuple( rid, data ) != RM_EOF )
	{
		// [tableID][tableName][columnStart][columnName][columnType][maxLength]
		memcpy( &tableID, data,  sizeof(int));
		data = data + sizeof(int);

		memcpy( &columnStart, data,  sizeof(int));

		map<int, RID> *columnRID;
		// tableID already exists in the column map
		if( columnRIDMap.find(tableID) != columnRIDMap.end() )
		{
			// Handle later
			columnRID = columnRIDMap[tableID];
			(*columnRID)[pCounter] = rid;
		}
		else
		{
			columnRID = new map<int, RID>();
			(*columnRID)[pCounter] = rid;
			columnRIDMap[tableID] = columnRID;
		}
		pCounter++;
		data = start;

	}

	scanIterator.close();

	return 0;
}

RC RelationManager::createCatalogFile(const string& tableName, const vector<Attribute>& attrVector)
{
	RC result = -1;
	FileHandle fileHandle;

	string catFileName = tableName + ".tbl";
//	cout<<"Creating "<<catFileName<<endl;
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

	tableRIDMap[tableName] = tableRID; // modified to fix the pointer to object
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
	columnRIDMap[TABLE_ID] = columnRID; // same with line362

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
//	cout << "varCharLen=" << varCharLen << endl;
	offset += sizeof(int);
	memcpy( data + offset, tableName.c_str(), varCharLen ); // modified by xk
	offset += varCharLen;

	varCharLen = catFileName.length();
	memcpy( data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( data + offset, catFileName.c_str(), catFileName.length() );// modified by xk
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

	memcpy( data + offset, tableName.c_str(), varCharLen);
	offset += varCharLen;

	memcpy( data + offset, &columnStart, sizeof(int));
	offset += sizeof(int);

	varCharLen = columnName.length();
	memcpy( data + offset, &varCharLen, sizeof(int) );
	offset += sizeof(int);
	const char *cstrColumn = &columnName[0];
	memcpy( data + offset, cstrColumn, varCharLen);
//	cout << "columnName:" << cstrColumn << endl;
	offset += varCharLen;

	memcpy( data + offset, (int*)&columnType, sizeof(int) );
	offset += sizeof(int);

	memcpy( data + offset, &maxLength, sizeof(int));
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, columnCatalog, data, rid );
	if( result != 0 )
		return -1;

//	cout<<"For table name :"<< tableName << " TABLE_ID=" << tableID << " rid pageNum: " << rid.pageNum <<
//			" rid slotNum: " << rid.slotNum << " result:"<< result << endl;

	free( data );

	return result;
}

unsigned RelationManager::getCatalogSize(vector<Attribute> catalog)
{
	unsigned length = 0;

	for(int i=0; i < (int)catalog.size(); i++)
	{
		length += catalog.at(i).length;
		if( catalog.at(i).type == TypeVarChar )
			length +=  sizeof(int);

	}

	return length;
}

RC RM_ScanIterator::initialize(vector<Attribute> recordDescriptor,
		  const string &conditionAttribute,
	      const CompOp compOp,
	      const void *value,
	      const vector<string> &attributeNames)
{
	return _rbfm_scanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);

}

// "data" follows the same format as RelationManager::insertTuple()
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	RC result  = _rbfm_scanIterator.getNextRecord(rid, data);
	return result;
};

RC RM_ScanIterator::close()
{
	_rbfm_scanIterator.close();
	return _rbfm->closeFile( fileHandle );
};
