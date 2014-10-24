
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
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	// fix the recursive call
	if(tableAttributesCache.find(tableName) == tableAttributesCache.end()){
		//	read table from system catalog
		tableAttributesCache[tableName] = attrs;
	}
	else
		attrs = tableAttributesCache[tableName];
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    vector<Attribute> tableAttributes;
    getAttributes(tableName, tableAttributes);
    // Initialize fileHandle everytime?
    rbfm->insertRecord(fileHandle, tableAttributes, data, rid);

    return 0;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	RC result = -1;
	result = rbfm->deleteRecords(fileHandle);
    return result;
}



RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = rbfm->deleteRecord(fileHandle, tableAttributes, rid);
    return result;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = rbfm->updateRecord(fileHandle, tableAttributes, data, rid);
    return result;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = rbfm->readRecord(fileHandle, tableAttributes, rid, data);
    return result;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = rbfm->readAttribute(fileHandle, tableAttributes, rid, attributeName, data);
    return result;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	RC result = -1;
	vector<Attribute> tableAttributes;
	getAttributes(tableName, tableAttributes);
	result = rbfm->reorganizePage(fileHandle, tableAttributes, pageNumber);
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
