
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
	return -1;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
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

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const unsigned &primaryPageNumber) 
{
	return -1;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	return -1;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	return -1;
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
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}

void IX_PrintError (RC rc)
{
}
