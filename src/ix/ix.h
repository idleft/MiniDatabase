#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan
# define MAX_OVERFLOW_PAGE_INFO (500) // max overflow page info can stored in one meta page
# define MAX_INDEX_PAGE_SLOT_NUM (200)
class IX_ScanIterator;
class IXFileHandle;
const string METASUFFIX = ".meta", BUCKETSUFFIX = ".idx";
//
//typedef struct{
//	unsigned recordNum;
//	unsigned nextPageId;
//} OverflowPageInfo;
//

typedef struct
{
	short idxRecordLength;
	short nextOffset;
	unsigned recordPageId;
	unsigned recordSlotId;
} IdxRecordHeader;

typedef struct
{
	short slotIdxNum;
	short nextIdxOffset;
} IdxSlot;

typedef struct
{
	short numOfIdx;
	short freeSpaceOffset;
	short freeSpaceNum;
	unsigned nextPageId;
} DirectoryOfIdxInfo;


typedef struct{
	unsigned next;
	unsigned level;
	unsigned N;
	unsigned primaryPgNum; // bucketNum
	unsigned overFlowPgNum;
	unsigned physicalPrimaryPgNum;
	unsigned physicalOverflowPgNum;
} IdxMetaHeader;

class IndexManager {
 public:
  static IndexManager* instance();

  // Create index file(s) to manage an index
  RC createFile(const string &fileName, const unsigned &numberOfPages);

  // Delete index file(s)
  RC destroyFile(const string &fileName);

  // Open an index and returns an IXFileHandle
  RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

  // Close an IXFileHandle. 
  RC closeFile(IXFileHandle &ixfileHandle);


  // The following functions  are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For INT and REAL: use 4 bytes to store the value;
  //     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

  // Insert an entry to the given index that is indicated by the given IXFileHandle
  RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // Delete an entry from the given index that is indicated by the given IXFileHandle
  RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  
  // Initialize and IX_ScanIterator to supports a range search
  RC scan(IXFileHandle &ixfileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // Generate and return the hash value (unsigned) for the given key
  unsigned hash(const Attribute &attribute, const void *key);
  
  unsigned hashInt(int key);
  int hash32shift(int key);
  unsigned floatHash( float f );
  
  int hashForString(string word);

  unsigned long stringHash(unsigned char* string);
  unsigned int generateHash(const char *string, size_t len);
  unsigned int RSHash(const std::string& str);
  // Print all index entries in a primary page including associated overflow pages
  // Format should be:
  // Number of total entries in the page (+ overflow pages) : ?? 
  // primary Page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx] [xx]
  // overflow Page No.?? liked to [primary | overflow] page No.??
  // # of entries : ??
  // entries: [xx] [xx] [xx] [xx] [xx]
  // where [xx] shows each entry.
  RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);
  
  // Get the number of primary pages
  RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

  // Get the number of all pages (primary + overflow)
  RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);

  unsigned getIdxPgId(unsigned bucketId, IdxMetaHeader* idxMetaHeader);
  int getKeyRecordSize(const Attribute &attr, const void *key);
//  RC getKeyRecordAttrSet(Attribute attribute, vector<Attribute> &keyAttrSet);
//  OverflowPageInfo* goToOverflowPageInfo(void *metaPageData, unsigned overflowPageId);
  unsigned getOverFlowPageRecordNumber(IXFileHandle ixFileHandle, unsigned overflowPageId);
  unsigned getOverflowPageId(IXFileHandle ixfileHandle, unsigned nextPageId, int keyRecordSize, int &splitPage);
  DirectoryOfIdxInfo* goToDirectoryOfIdx(void *pageData);
  IdxSlot* goToIdxSlot(void* pageData, unsigned slotId);
  RC insertIdxToPage(FileHandle &fileHandle, const Attribute &keyAttribute,
			const void *key, const RID &keyRID, RID &idxRID, unsigned pageId, unsigned hashKey);
  unsigned getSlotId(unsigned hashKey);
  bool checkEqualKey(Attribute attr, const void *key, void *cmpKey);
  RC removePage(FileHandle prePageFileHandle, DirectoryOfIdxInfo *curDirInfo, unsigned prePgId);
  RC emptyPage(FileHandle pageFileHandle, unsigned pgId);

  RC appendEmptyPage(FileHandle &fileHandle);
  RC flagInsertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, bool splitFlag);
  unsigned getKeySize(IdxRecordHeader *idxRecordHeader);

  void debug(IXFileHandle ixFileHandle);
  
  IdxMetaHeader* getCurrentIndexMetaHeader()
  {
	  return currentIndexMetaHeader;
  }
  void setCurrentIndexMetaHeader(IdxMetaHeader* metaHeader)
  {
	  currentIndexMetaHeader = metaHeader;
  }
 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
  PagedFileManager *_pfm;
//  RecordBasedFileManager *_rbfm;
  Attribute pageIdAttr, slotIdAttr;
  IdxMetaHeader* currentIndexMetaHeader;
  unsigned SIZE_OF_IDX_HEADER;
};


class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    IXFileHandle();  							// Constructor
    ~IXFileHandle(); 							// Destructor
    FileHandle metaFileHandle;
    FileHandle idxFileHandle;

private:
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;

};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor
  bool checkValueSpan(Attribute attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, void *keyRecordData);
  int getKeyDataSize(Attribute attr, void *keyRecordData);


  RC initialize(IXFileHandle &ixfileHandle,
  	    const Attribute &attribute,
  	    const void      *lowKey,
  	    const void      *highKey,
  	    bool			lowKeyInclusive,
  	    bool        	highKeyInclusive);
  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC getNextEntryForOverflowPage(RID &rid, void *key); 	// Get next entry for overflow page only
  RC close();             						// Terminate index scan


 private:
  IXFileHandle ixfileHandle;
  Attribute attribute;
  const void *lowKey;
  const void *highKey;
  bool lowKeyInclusive;
  bool highKeyInclusive;
  unsigned SIZE_OF_IDX_HEADER;
  // curPageId for both primary page and overflow page, bucketId
  // for primary page only
  unsigned curBucketId, curRecId,totalBucketNum;
  void* pageData;
  Attribute keyAttri;
  short curInPageOffset;

  DirectoryOfIdxInfo *dirInfo;
  IndexManager *_ixm;
};



// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
