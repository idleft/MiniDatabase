
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
//	cout<< "Table:: "<< tableCatalog.at(0).name <<endl;
	indexMap.clear();

	if( _pfm->fileExists( TABLE_CATALOG_FILE_NAME  ) )
		loadCatalog();
	else
	{
		createCatalogFile( "Tables", tableCatalog );
		createCatalogFile( "Columns", columnCatalog );
		//createCatalogFile( "index", indexCatalog );
		createIndexFile();
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
	writeIndexList();

}

RC RelationManager::createIndexFile(){
	return _rbfm->createFile(INDEX_CATALOG_FILE_NAME);
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC result = -1;
	// check reserved table names
	if( tableName.compare("Tables") == 0 ||
			tableName.compare("Columns") == 0 ||
			tableName.compare("Index") == 0 )
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

	// [EUNJEONG.SHIN] close file after deleting table record
	result = _rbfm->closeFile( fileHandle );
	if ( result != 0 ) {
		return -1;
	}

	// [EUNJEONG.SHIN] delete physical file after deleting table, column record
	string fileName = tableName + ".tbl";
	result = _rbfm->destroyFile( fileName );
	
	// delete index 
	vector<Attribute> attrList = indexMap[tableName];
	for(unsigned iter1 = 0; iter1<attrList.size();iter1++)
		destroyIndex(tableName, attrList.at(iter1).name);
	indexMap.erase(tableName);
    return result;
}

RC RelationManager::colDescriptorToAttri(char* data, Attribute &colAttri){
	// [tableID][tableName][columnStart][columnName][columnType][maxLength]
	int offset = 0;
	int varLen = 0, columnStart;

	// [EUNJEONG.SHIN] rewrote the whole function not to use offset
	data = data + sizeof(int);	//skip the tableID

	memcpy(&varLen, data, sizeof(int));
	data = data + sizeof(int) + varLen;// skip tableName

	memcpy(&columnStart, data, sizeof(int));
	data += sizeof(int);	// skip columnStart

	memcpy(&varLen, (char*)data, sizeof(int));
	data += sizeof(int);	// skip columnName length

	colAttri.name = string(data, varLen);
	data += varLen;

	/*
	char * nameChar = (char *)malloc(varLen+1);
	memset(nameChar,0,varLen+1);
	memcpy(nameChar, (char*)data+offset, varLen); // store columnName
	colAttri.name = string((char *)nameChar);

//	cout<<"*************Read attribute name: "<<colAttri.name<<endl;
	free(nameChar);
	*/

	/*
	memcpy(&varLen, (char *)data, sizeof(int));
	data += sizeof(int);		// skip columnType size
	*/

	AttrType type;
	memcpy(&type,(char*)data, sizeof(AttrType)); // store columnType
	colAttri.type = type;
	data += sizeof(int);

	unsigned mLength;
	memcpy(&mLength, (char*)data, sizeof(int)); // store colMaxLength
	colAttri.length = mLength;

//	cout << "name=" << colAttri.name << " type=" << colAttri.type << " length=" << colAttri.length << endl;

	return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RC rc = 0;
	string index;

	/* index name = table name + "_" + attribute name */
	index = tableName + "." + attributeName;	// [EUNJEONG.SHIN] Change index name connected with "."

	const unsigned numberOfPages = 4;	/* to be changed */
	rc = _im->createFile( index, numberOfPages );
	if( rc != 0 )
		return -1;

	IXFileHandle ixFileHandle;
	rc = _im->openFile( index, ixFileHandle );	// [EUNJEONG.SHIN] Bug fix input from tableName+".tbl" -> index
//	cout << "_im->openFile, tableName:" << tableName << ":" << rc << endl;
	if( rc != 0 )
	    return rc;

	RM_ScanIterator rmsi;

	vector<string> attributeNames;
	attributeNames.push_back(attributeName);

	rc = scan( tableName, "", NO_OP, NULL, attributeNames, rmsi );
	if( rc != 0 )
		return -1;

	void* key = malloc(PAGE_SIZE);
	RID rid;
	Attribute attr;
	rc = findAttributeFromCatalog( tableName, attributeName, attr );
	if( rc != 0 )
		return rc;

	while( rmsi.getNextTuple( rid, key ) != RM_EOF ) {
		rc = _im->insertEntry( ixFileHandle, attr, key, rid );
		if( rc != 0 )
			return -1;
	}

	free(key);

	indexMap[tableName].push_back(attr);
	return rc;
}

RC RelationManager::writeIndexList(){
	string tableName, attrName;
	FileHandle fileHandle;
	RID rid;
	// for simplicity. just create a new file
	_rbfm->destroyFile(INDEX_CATALOG_FILE_NAME);
	_rbfm->createFile(INDEX_CATALOG_FILE_NAME);
	_rbfm->openFile(INDEX_CATALOG_FILE_NAME, fileHandle);
	short eSize = _rbfm->getEstimatedRecordDataSize(indexCatalog);
	for( map< string, vector<Attribute> >::iterator iter1=indexMap.begin();iter1!=indexMap.end();iter1++){
		vector<Attribute> attrList = iter1->second;
		tableName = iter1->first;
		for(vector<Attribute>::iterator iter2 = attrList.begin(); iter2!=attrList.end(); iter2++){
			attrName = iter2->name;
			int offset = 0;
			void *data = malloc(eSize);

			*(int*)data = tableName.length();
			offset += sizeof(int);

			memcpy((char*)data+offset, tableName.c_str(), tableName.length());
			offset += tableName.length();

			*(int*)((char *)data + offset) = attrName.length();
			offset += sizeof(int);

			memcpy((char*)data+offset, attrName.c_str(), attrName.length());

			_rbfm->insertRecord(fileHandle,indexCatalog, data, rid);
			free(data);
		}
	}
	_rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	string index;
	/* index name = table name + "_" + attribute name */
	index = tableName + "." + attributeName;	// [EUNJEONG.SHIN] Change index name connected with "."

	rc = _im->destroyFile( index );
	if( rc != 0 )
		return rc;

	return rc;
}

RC RelationManager::indexScan(const string &tableName, const string &attributeName, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator)
{
	RC rc;
	string index;

	/* index name = table name + "_" + attribute name */
//	index = tableName + "." + attributeName;		// [EUNJEONG.SHIN ] attributeName itself is already an index name

	IXFileHandle ixFileHandle;
	rc = _im->openFile( attributeName, ixFileHandle );
	if( rc != 0 )
	    return rc;

	Attribute attribute;
	rc = findAttributeFromCatalog( tableName, attributeName, attribute );
//	cout << "[EJSHIN FOR DEBUG] [findAttributeFromCatalog] rc=" << rc << endl;
	if( rc != 0 )
		return rc;

	rc = _im->scan( ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator._ix_ScanIterator);
//	cout << "[EJSHIN FOR DEBUG] [scan] rc=" << rc << endl;
	if( rc != 0 )
		return rc;

	return rc;
}

bool RelationManager::findAttributeFromCatalog(const string &tableName, const string &attributeName, Attribute &attribute)
{
	RC rc;
	bool found = false;

	vector<Attribute> attrs;
	rc = getAttributes( tableName, attrs );
	if( rc != -1 )
		return rc;

	for(Attribute attr : attrs)
	{
		if( attributeName.compare(attr.name) == 0 )
		{
			attribute.length = attr.length;
			attribute.name = attr.name;
			attribute.type = attr.type;
			found = true;
		}
	}

	if( found == false )
		return RM_ATTRIBUTE_NOT_FOUND;

	return found;

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
// EUNJEONG.SHIN 120714
	for(map<int, RID>::iterator iter1 = tableIdSet->begin(); iter1!=tableIdSet->end(); iter1++){

		int tableId = iter1->first; // only require tableId
//		int tableId = tableIdSet->begin()->first;
//		cout << "tableName:" << tableName << ",tableId:" << tableId << endl;

		map<int, RID> *attrMap = columnRIDMap[tableId];

		for(map<int, RID>::iterator iter2 = attrMap->begin(); iter2 != attrMap->end(); iter2++){

			Attribute colAttri;

			RID attrRid = iter2->second;

			char* colDescriptor = (char*)malloc(colCatalogSize);
			result = _rbfm->readRecord(fileHandle, columnCatalog, attrRid, colDescriptor);
			if( result != 0 )
				return result;

			result = colDescriptorToAttri(colDescriptor, colAttri);
			if( result != 0 )
				return result;

			if( colAttri.name.compare("tableID") != 0
				&& colAttri.name.compare("fileName") != 0 )
					attrs.push_back( colAttri );

/*	EUNJEONG.SHIN && colAttri.name.compare("tableName") != 0 */



			free( colDescriptor );
		}
	}

//	cout << "getAttributes:" << attrs.size() << endl;

	_rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RC result = -1;
	FileHandle fileHandle;
    vector<Attribute> tableAttributes;
    Attribute indexAttr;
    string attrName;

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

    // insert index
    vector<Attribute> attrList = indexMap[tableName];
    for(unsigned iter1 = 0; iter1< attrList.size(); iter1++){
    	void* key = malloc(attrList.at(iter1).length);
    	string indexFileName = tableName + "_" + attrList.at(iter1).name;
    	IXFileHandle ixfileHandle;
    	_im->openFile(indexFileName, ixfileHandle);
    	for(unsigned iter2 = 0; iter2<tableAttributes.size();iter2++){
    		if(attrList.at(iter1).name == tableAttributes.at(iter2).name){
    			indexAttr = attrList.at(iter1);
    			getAttrFromData( tableAttributes,data, key,attrList.at(iter1).name );
    			_im->insertEntry(ixfileHandle, indexAttr, key, rid);
    		}
    	}
    	free(key);
    	_im->closeFile(ixfileHandle);
    }

    return result;
}

RC RelationManager::getAttrFromData(const vector<Attribute> attrs, const void* data, void* key, string attrName){
	short attrSize = 0;
	return _rbfm->getAttrFromData(attrs, (void*)data, key, attrName, attrSize);
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

	// empty index
	vector<Attribute> attrList = indexMap[tableName];
	for(unsigned iter1 = 0; iter1 < attrList.size(); iter1++){
		string indexFileName = getIndexName(tableName, attrList.at(iter1).name);
		_im->destroyFile(indexFileName);
		result = createIndex(tableName, attrList.at(iter1).name);
	}

    return result;
}

string RelationManager::getIndexName(string tableName, string attrName){
	return tableName + "." + attrName;		// [EUNJEONG.SHIN] Change index name connected with "."
}


RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle fileHandle;
	RC result = -1;
	vector<Attribute> tableAttributes;
	vector<Attribute> indexAttrList;
	IXFileHandle ixFileHandle;
	short estimatedRecordSize = 0;
    result = _rbfm->openFile(tableName+".tbl",fileHandle);
    if( result != 0 )
    	return result;

	result = getAttributes(tableName, tableAttributes);
	indexAttrList = indexMap[tableName];
	// delete Index first then record itself
	estimatedRecordSize = _rbfm->getEstimatedRecordDataSize(tableAttributes);
//	void *recordData = malloc(estimatedRecordSize);
	for(unsigned iter1 = 0; iter1<indexAttrList.size(); iter1++){
		void *key = malloc(tableAttributes.at(iter1).length);
		string indexFileName = getIndexName(tableName, indexAttrList.at(iter1).name);
		_im->openFile(indexFileName, ixFileHandle);
		_rbfm->readAttribute(fileHandle, tableAttributes, rid, indexAttrList.at(iter1).name,key);
		_im->deleteEntry(ixFileHandle, indexAttrList.at(iter1), key, rid);
		_im->closeFile(ixFileHandle);
		free(key);
	}
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
	IXFileHandle ixFileHandle;
    _rbfm->openFile(tableName+".tbl",fileHandle);
//    cout<<1<<endl;
	getAttributes(tableName, tableAttributes);
	// update index
	vector<Attribute> attrList = indexMap[tableName];
	for(unsigned iter1 = 0; iter1<attrList.size(); iter1++){
		void* key = malloc(attrList.at(iter1).length);
		void* updatedKey = malloc(attrList.at(iter1).length);
		string indexFileName = getIndexName(tableName, attrList.at(iter1).name);
		_im->openFile(indexFileName, ixFileHandle);
		_rbfm->readAttribute(fileHandle, tableAttributes, rid, attrList.at(iter1).name,key);
		// delete old one
		_im->deleteEntry(ixFileHandle, attrList.at(iter1),key, rid);
		// insert new one
		getAttrFromData(tableAttributes, data, key, attrList.at(iter1).name);
		_im->insertEntry(ixFileHandle, attrList.at(iter1), key, rid);
		_im->closeFile(ixFileHandle);
		free(key);
		free(updatedKey);
	}


	result = _rbfm->updateRecord(fileHandle, tableAttributes, data, rid);
    _rbfm->closeFile(fileHandle);
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
//	cout<<"Loading table..."<<endl;


	while( scanIterator.getNextTuple( rid, data ) != RM_EOF )
	{
		// [tableID][tableName][catFileName][numOfColums]
		memcpy( &tableID, data,  sizeof(int));

		data = data + sizeof(int);

		int tableNameLen;
		memcpy( &tableNameLen, data, sizeof(int));
		data = data+sizeof(int);

		string tableName = string(data, tableNameLen);
//		cout<<"load table name: "<<tableName<<" table id: "<<tableID<<endl;

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
//		cout << "_rbfm->createFile="<< "["<< catFileName << "]" << result << endl;
		if( result != 0 )
			return result;
	}

	// TABLE CATALOG EXISTS. CREATE COLUMN CATALOG
	if( catFileName.compare( TABLE_CATALOG_FILE_NAME)  == 0 )
	{
		result =  _rbfm->createFile( COLUMN_CATALOG_FILE_NAME);
//		cout << "_rbfm->createFile="<< "["<< COLUMN_CATALOG_FILE_NAME << "]" << result << endl;
		if( result != 0 )
			return result;
	}

	// INSERT TABLE CATALOG ENTRIES
	result = _rbfm->openFile( TABLE_CATALOG_FILE_NAME, fileHandle );
//	cout << "_rbfm->openFile="<< "["<< TABLE_CATALOG_FILE_NAME << "]" << result << endl;
	if( result != 0 )
		return result;

	RID rid;
	result = insertTableEntry( TABLE_ID, tableName, catFileName, fileHandle, attrVector.size(), rid);
//	cout << "insertTableEntry="<< "["<< tableName << "]" << result << endl;
	if( result != 0 )
		return result;

	map<int, RID> *tableRID = new map<int, RID>();
	(*tableRID)[TABLE_ID] = rid;

	tableRIDMap[tableName] = tableRID; // modified to fix the pointer to object
	result = _rbfm->closeFile( fileHandle );
//	cout << "closeFile="<< "["<< fileHandle.getFileName() << "]" << result << endl;
	if( result != 0 )
		return result;

	// INSERT COLUMN CATALOG ENTRIES
	result = _rbfm->openFile( COLUMN_CATALOG_FILE_NAME, fileHandle );
//	cout << "openFile="<< "["<< COLUMN_CATALOG_FILE_NAME << "]" << result << endl;
	if( result != 0 )
		return result;

//  [EUNJEONG.SHIN] Add each column for the table with TABLE_ID
//	map<int, RID> *columnRID = new map<int, RID>();
	for(unsigned i = 0; i < attrVector.size(); i++)
	{
		int columnPos = i + 1;
		result = insertColumnEntry( TABLE_ID, tableName, i+1, attrVector.at(i).name, attrVector.at(i).type, attrVector.at(i).length, fileHandle, rid);
//		(*columnRID)[i] = rid;
//		cout << "******insertColumnEntry="<< "["<< i << "]" << attrVector.at(i).name << "," << result << endl;

		if(columnRIDMap.find(TABLE_ID) != columnRIDMap.end())
		{
			(*columnRIDMap[TABLE_ID])[columnPos] = rid;
		}
		else
		{
			map<int, RID> *columnEntryMap = new map<int, RID>();
			(*columnEntryMap)[columnPos] = rid;
//			cout << "columnEntryMap="<< "["<< columnPos << "]" << endl;
			(columnRIDMap[TABLE_ID]) = columnEntryMap; // same with line362
		}

	}
//	columnRIDMap[TABLE_ID] = columnRID; // same with line362

	if( result == 0 )
		TABLE_ID += 1;

	result = _rbfm->closeFile( fileHandle );
//	cout << "closeFile="<< "["<< fileHandle.getFileName() << "]" << result << endl;
	if( result != 0 )
		return result;

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
	memcpy( data + offset, catFileName.c_str(), varCharLen );// modified by xk
	offset += varCharLen;

	int numOfColumns = tableCatalog.size();
	memcpy( data + offset, &numOfColumns, sizeof(int));
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, tableCatalog, data, rid );

	free( data );

	return result;
}

RC RelationManager::insertColumnEntry(int tableID, string tableName, int columnStart, string columnName, AttrType columnType, AttrLength maxLength, FileHandle &fileHandle, RID& rid)
{
	RC result = -1;
	int offset = 0;

	int catalogSize = getCatalogSize(columnCatalog);
	char* data = (char*)malloc(catalogSize);

	// [tableID][tableName][columnStart][columnName][columnType][maxLength]
	memcpy( data, &tableID, sizeof(int));
	offset += sizeof(int);

	int varCharLen = tableName.length();
	memcpy( data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);

	memcpy( data + offset, tableName.c_str(), varCharLen );
	offset += varCharLen;

	memcpy( data + offset, &columnStart, sizeof(int));
	offset += sizeof(int);

	int columnLen = columnName.length();
	memcpy( data + offset, &columnLen, sizeof(int) );
	offset += sizeof(int);
//	const char *cstrColumn = &columnName[0];
	// [EUNJEONG.SHIN] String to const char* conversion error fix
	/*
	char* y = new char[columnName.size() + 1];
	strcpy( y, columnName.c_str());
	cout << y <<" columnName.size()" << columnName.size() << endl;

	memcpy( data + offset, y, strlen(y)+1);
	*/
	memcpy( data + offset, columnName.c_str(), columnLen );

	/*
	memcpy( data + offset, columnName.c_str(), varCharLen);
	cout << "columnName:" << columnName.c_str() << endl;
	*/
	offset += columnLen;
	
	memcpy( data + offset, (int*)&columnType, sizeof(int) );
	offset += sizeof(int);

	memcpy( data + offset, &maxLength, sizeof(int));
//	memcpy( data + offset, &maxLength, sizeof(AttrLength));
//	cout << "maxLength:" << maxLength << endl;
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, columnCatalog, data, rid );

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
//	cout << "getNextTuple:" << result;
	return result;
};

RC RM_ScanIterator::close()
{
	_rbfm_scanIterator.close();
	return _rbfm->closeFile( fileHandle );
};


RC RM_IndexScanIterator::initialize(const Attribute attribute,
 	    const void      *lowKey,
 	    const void      *highKey,
 	    bool			lowKeyInclusive,
 	    bool        	highKeyInclusive)
{
	// [EUNJEONG.SHIN] bug fix, (_im->scan -> _ix_ScanIterator.initialize)
	return _ix_ScanIterator.initialize( idxFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive );
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void* key)
{
	return _ix_ScanIterator.getNextEntry( rid, key );
}

RC RM_IndexScanIterator::close()
{
	return _ix_ScanIterator.close();
}
