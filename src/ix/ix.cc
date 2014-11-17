
#include "ix.h"
#include "math.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	_rbfm = RecordBasedFileManager::instance();

	pageIdAttr.length = 4;
	pageIdAttr.name = "pageId";
	pageIdAttr.type = TypeInt;

	slotIdAttr.length = 4;
	slotIdAttr.name = "slotId";
	slotIdAttr.type = TypeInt;
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	RC rc1,rc2;
	string metaFileName, idxFileName;
	void *data;
	FILE *file;
	FileHandle idxFileHandle;
	metaFileName = fileName + METASUFFIX;
	idxFileName = fileName + BUCKETSUFFIX;
	rc1 = _rbfm->createFile(metaFileName);
	rc2 = _rbfm->createFile(idxFileName);

	if(rc1!=0||rc2!=0)
		return -1;

	// Prepare data
	_rbfm->openFile(idxFileName, idxFileHandle);
	for(int iter1 = 0; iter1<numberOfPages; iter1++)
		_rbfm->appendEmptyPage(idxFileHandle);
	_rbfm->debug(idxFileHandle);
	_rbfm->closeFile(idxFileHandle);

	// Deal with the first page of meta data
	data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	IdxMetaHeader *metaHeader = (IdxMetaHeader *)data;
	metaHeader->N = numberOfPages;
	metaHeader->level = 0;
	metaHeader->next = 0;
	file = fopen(&metaFileName[0],"rb+");
	fwrite(data, 1, PAGE_SIZE, file);
	fclose(file);
	free(data);

	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc1,rc2;
	string metaFileName, idxFileName;
	metaFileName = fileName + METASUFFIX;
	idxFileName = fileName + BUCKETSUFFIX;
	rc1 = _pfm->destroyFile(&metaFileName[0]);
	rc2 = _pfm->destroyFile(&idxFileName[0]);
	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	RC rc1,rc2;
	string metaFileName, idxFilename;
	metaFileName = fileName + METASUFFIX;
	idxFilename = fileName + BUCKETSUFFIX;

	rc1 = _rbfm->openFile(metaFileName, ixFileHandle.metaFileHandle);
	rc2 = _rbfm->openFile(idxFilename,ixFileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC rc1,rc2;
	rc1 = _rbfm->closeFile(ixfileHandle.metaFileHandle);
	rc2 = _rbfm->closeFile(ixfileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

unsigned IndexManager:: getIdxPgId(unsigned bucketId, IdxMetaHeader* idxMetaHeader)
{
	unsigned res = 0;
	unsigned currentListLength = idxMetaHeader->N*pow(2,idxMetaHeader->level);
	res = bucketId%currentListLength;
	if(res<idxMetaHeader->next){
		idxMetaHeader->level++;
		currentListLength = idxMetaHeader->N*pow(2,idxMetaHeader->level);
		res = bucketId%currentListLength;
	}
	return res;
}

int IndexManager:: getKeyRecordSize(const Attribute &attr, const void *key)
{
	int res = 0;
	if(attr.type == TypeVarChar){
		int varLen = *((int *)key);
		res = sizeof(int) + varLen;
	}
	else if(attr.type == TypeInt)
		res = sizeof(int);
	else
		res = sizeof(float);
	// plus the rid.pageNum & rid.slotNum
	res += 2*sizeof(unsigned);
	return res;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC res = -1;
	unsigned bucketId,idxPgId;
	vector<Attribute> keyAttrSet;
	void *idxMetaPage,*pageData,*keyRecordData;
	char *endOfPage;
	IdxMetaHeader *idxMetaHeader;
	DirectoryOfSlotsInfo *pageDirInfo;
	int keyRecordSize;
	RID keyRecordRID;
	// Get the index meta data
	idxMetaPage = malloc(PAGE_SIZE);
	pageData = malloc(PAGE_SIZE);
	endOfPage = (char*)pageData + PAGE_SIZE;

	res = ixfileHandle.metaFileHandle.readPage(0,idxMetaPage);

	if(res<0)
		return -1;

	idxMetaHeader = (IdxMetaHeader*)idxMetaPage;

	keyAttrSet.push_back(attribute);
	keyAttrSet.push_back(pageIdAttr);
	keyAttrSet.push_back(slotIdAttr);
	keyRecordSize = getKeyRecordSize(attribute, key);

	// generate key record data
	keyRecordData = malloc(keyRecordSize);
	memcpy(keyRecordData, key, keyRecordSize-2*sizeof(unsigned));
	*((unsigned *)((char*)keyRecordData+ keyRecordSize-2*sizeof(unsigned))) = rid.pageNum;
	*((unsigned *)((char*)keyRecordData+ keyRecordSize-sizeof(unsigned))) = rid.slotNum;

	bucketId = hash(attribute, key);

	idxPgId = getIdxPgId(bucketId, idxMetaHeader);

	// get the page id see if over flow page needed
	ixfileHandle.idxFileHandle.readPage(idxPgId, pageData);
	pageDirInfo = _rbfm->goToDirectoryOfSlotsInfo(endOfPage);

	if(pageDirInfo->freeSpaceNum > keyRecordSize){
		res = _rbfm->insertRecord(ixfileHandle.idxFileHandle, keyAttrSet, keyRecordData, keyRecordRID);
	}
	else{
		// find a suitable over flow page
		cout<<"Over flow not implemented yet"<<endl;
	}

	free(idxMetaPage);
	free(pageData);
	free(keyRecordData);

	return res;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	return 0;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	int pageNum = 0;
	pageNum = ixfileHandle.idxFileHandle.getNumberOfPages();
	numberOfPrimaryPages = pageNum;
	return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	int pageNum = 0;
	pageNum+=ixfileHandle.idxFileHandle.getNumberOfPages();
	pageNum+=ixfileHandle.metaFileHandle.getNumberOfPages();
	numberOfAllPages = pageNum;
	return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{
	return -1;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	return -1;
}

RC IX_ScanIterator::close()
{
	return -1;
}


IXFileHandle::IXFileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	/*
	 * Currently, only update the counter in IXFileHandle
	 * when collect is called.
	 * */
	unsigned s1,s2,s3,e1,e2,e3;
	metaFileHandle.collectCounterValues(s1,s2,s3);
	idxFileHandle.collectCounterValues(e1,e2,e3);
	this->readPageCounter = s1+e1;
	this->writePageCounter = s2+e2;
	this->appendPageCounter = s3+e3;
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}

void IX_PrintError (RC rc)
{
}
