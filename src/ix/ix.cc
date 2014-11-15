
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	RC rc1,rc2;
	string metaFileName, idxFileName;
	metaFileName = fileName + METASUFFIX;
	idxFileName = fileName + BUCKETSUFFIX;
	rc1 = _pfm->createFile(metaFileName.c_str());
	rc2 = _pfm->createFile(idxFileName.c_str());

	void *data = malloc(PAGE_SIZE*numberOfPages);
	FILE *file = fopen(idxFileName.c_str(),"rb");
	fwrite(data, 1, PAGE_SIZE*numberOfPages, file);
	fclose(file);
	free(data);

	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
	RC rc1,rc2;
	string metaFileName, idxFileName;
	metaFileName = fileName + METASUFFIX;
	idxFileName = fileName + BUCKETSUFFIX;
	rc1 = _pfm->destroyFile(metaFileName.c_str());
	rc2 = _pfm->destroyFile(idxFileName.c_str());
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

	rc1 = _pfm->openFile(metaFileName.c_str(), ixFileHandle.metaFileHandle);
	rc2 = _pfm->openFile(idxFilename.c_str(),ixFileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
			return 0;
	else
		return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	RC rc1,rc2;
	rc1 = _pfm->closeFile(ixfileHandle.metaFileHandle);
	rc2 = _pfm->closeFile(ixfileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
			return 0;
	else
		return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return -1;
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
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}

void IX_PrintError (RC rc)
{
}
