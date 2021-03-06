
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

#define TABLE_CATALOG_FILE_NAME "Tables.tbl"
#define COLUMN_CATALOG_FILE_NAME "Columns.tbl"
#define INDEX_CATALOG_FILE_NAME "Index.tbl"

# define RM_EOF (-1)  // end of a scan operator

#define MAX_SIZE_OF_CATALOG_RECORD	1024

#define RM_ATTRIBUTE_NOT_FOUND 10
#define RM_NOT_FOUND 11

// RM_ScanIterator is an iterator to go through tuples
// The way to use it is like the following:
//  RM_ScanIterator rmScanIterator;
//  rm.open(..., rmScanIterator);
//  while (rmScanIterator(rid, data) != RM_EOF) {
//    process the data;
//  }
//  rmScanIterator.close();

class RM_ScanIterator {
public:
  RM_ScanIterator() { _rbfm = RecordBasedFileManager::instance(); };
  ~RM_ScanIterator() {};

  RC initialize(vector<Attribute> catalogAttribute,
		  const string &conditionAttribute,
	      const CompOp compOp,
	      const void *value,
	      const vector<string> &attributeNames);

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

  FileHandle	fileHandle;

private:
  RBFM_ScanIterator	_rbfm_scanIterator;
  RecordBasedFileManager *_rbfm;
};

/* extension for project 4 */
class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() { _im = IndexManager::instance(); }; 	// Constructor
  ~RM_IndexScanIterator() {}; 	// Destructor

  RC initialize(const Attribute attribute,
  	    const void      *lowKey,
  	    const void      *highKey,
  	    bool			lowKeyInclusive,
  	    bool        	highKeyInclusive);

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);  	// Get next matching entry
  RC close();             					// Terminate index scan

  IXFileHandle	idxFileHandle;
  IX_ScanIterator _ix_ScanIterator;

 private:
  IndexManager *_im;
};
/* extension for project 4 */

// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuples(const string &tableName);

  RC deleteTuple(const string &tableName, const RID &rid);

  // Assume the rid does not change after update
  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  RC reorganizePage(const string &tableName, const unsigned pageNumber);

  // scan returns an iterator to allow the caller to go through the results one by one. 
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

  vector<Attribute> tableCatalog;
  vector<Attribute> columnCatalog;
  vector<Attribute> indexCatalog;

  RC createTableCatalog();

  RC createColumnCatalog();

  RC createIndexCatalog();

  RC loadCatalog();

  RC insertTableEntry( int tableID, string tableName, string catFileName, FileHandle &fileHandle, int size, RID &rid );

  RC insertColumnEntry(int tableID, string tableName, int columnStart, string columnName, AttrType columnType, AttrLength maxLength, FileHandle &fileHandle, RID& rid);

  unsigned getCatalogSize(vector<Attribute> catalog);

  RC createCatalogFile(const string& tableName, const vector<Attribute>& attrVector);

  RC colDescriptorToAttri(char* data, Attribute &colAttri);

  /* extension for project 4 */
  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  RC indexScan(const string &tableName, 
  		const string &attributeName, 
		const void *lowKey, 
		const void *highKey, 
		bool lowKeyInclusive, 
		bool highKeyInclusive, 
		RM_IndexScanIterator &rm_IndexScanIterator);

  RC findAttributeFromCatalog(const string &tableName, const string &attributeName, Attribute &attribute);
  /* extension for project 4 */
  string getIndexName(string tableName, string attrName);

  RC loadIndexList();
  RC createIndexFile();
  RC writeIndexList();


  RC getAttrFromData(const vector<Attribute> attrs, const void* data, void* key, string attrName);

  int TABLE_ID;

// Extra credit
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);

  RC reorganizeTable(const string &tableName);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  map< string, vector<Attribute> > tableAttributesCache;
  RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
  PagedFileManager *_pfm = PagedFileManager::instance();
  IndexManager *_im = IndexManager::instance();

  map< string, map<int,RID> *> tableRIDMap;
  map< int, map<int,RID> *> columnRIDMap;
  map< string, vector<Attribute> > indexMap;

};
#endif
