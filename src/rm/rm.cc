
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
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuples(const string &tableName)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
    return -1;
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

	return result;
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

	result = _rbfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	// INSERT COLUMN CATALOG ENTRIES
	result = _rbfm->openFile( COLUMN_CATALOG_FILE_NAME, fileHandle );
	if( result != 0 )
		return result;

	for(int i = 0; i < attrVector.size(); i++)
	{
		result = insertColumnEntry( TABLE_ID, tableName, attrVector[i].name, attrVector[i].type, attrVector[i].length, fileHandle, rid);
	}

	result = _rbfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	TABLE_ID += 1;

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
	memcpy( (void*)data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( (void*)data + offset, &tableName, varCharLen );
	offset += varCharLen;

	varCharLen = catFileName.length();
	memcpy( (void*)data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( (void*)data + offset, &catFileName, catFileName.length() );
	offset += varCharLen;

	int numOfColumns = tableCatalog.size();
	memcpy( (void*)data + offset, &numOfColumns, sizeof(int));
	offset += sizeof(int);

	result = _rbfm->insertRecord( fileHandle, tableCatalog, data, rid );
	if( result != 0 )
		return result;

	free( data );

	return result;
}

RC RelationManager::insertColumnEntry(int tableID, string tableName, string columnName, AttrType columnType, AttrLength maxLength, FileHandle &fileHandle, RID& rid)
{
	RC result = -1;
	int offset = 0;

	int catalogSize = getCatalogSize(columnCatalog);
	char* data = (char*)malloc(catalogSize);

	// [tableID][tableName][columnName][columnType][maxLength]
	memcpy( (void*)data + offset, &tableID, sizeof(int));
	offset += sizeof(int);

	int varCharLen = tableName.length();
	memcpy( (void*)data + offset, &varCharLen, sizeof(int));
	offset += sizeof(int);
	memcpy( (void*)data + offset, &tableName, varCharLen);
	offset += varCharLen;

	varCharLen = columnName.length();
	memcpy( (void*)data + offset, &varCharLen, sizeof(int) );
	offset += sizeof(int);
	memcpy( (void*)data + offset, &columnName, varCharLen );
	offset += varCharLen;

	memcpy( (void*)data + offset, (int*)&columnType, sizeof(int) );
	offset += sizeof(int);

	memcpy( (void*)data + offset, &maxLength, sizeof(int));
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
		length += catalog[i].length;

		cout << "i=" << i << " size=" << length << endl;

		if( catalog[i].type == TypeVarChar )
			length +=  sizeof(int);

	}

	return length;
}
