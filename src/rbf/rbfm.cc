
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	map<string,vector<short>*>::iterator it;

	for (it=directoryOfSlots.begin(); it!=directoryOfSlots.end(); ++it)
		delete it->second;

	pfm = NULL;
	_rbf_manager = NULL;
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
	int result = pfm->createFile( fileName.c_str() );

	if( result == 0 )
	{
		result = pfm->createFile(  (fileName+"_desc").c_str() );
	}

	FileHandle descFileHandle;

	if( result == 0 )
	{
		result = pfm->openFile( (fileName+"_desc").c_str(), descFileHandle );
	}

	if( result == 0 )
	{
		char * page = (char*)malloc(PAGE_SIZE);
		*(short*)page = 0;

		result = descFileHandle.appendPage( page );
		if( result == 0 )
			pfm->closeFile ( descFileHandle );

		free( page );
	}

	// create file succeeded.
	return result;

}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	RC result = pfm->destroyFile( fileName.c_str() );

	if( result == 0 )
		result = pfm->destroyFile( (fileName+"_desc").c_str() );

	if( result == 0 )
		directoryOfSlots.erase(fileName);

	return result;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	RC result = -1;

	result = pfm->openFile( fileName.c_str(), fileHandle );
	if( result == 0 ) {
		if( directoryOfSlots.find(fileName) == directoryOfSlots.end() )	// fileName does not exist in directory of slots
		{
			// read description
			FileHandle descFileHandle;
			result =  pfm->openFile( (fileName + "_desc").c_str() , descFileHandle );
			if( result != 0 )
				return -1;

			vector<short>* freeSpace = new vector<short>();

			for (unsigned pageNo = 0; pageNo < descFileHandle.getNumberOfPages(); pageNo++)
			{
				result = readHeader( descFileHandle, pageNo, freeSpace );
				if( result != 0 )
					return -1;
			}

			result = pfm->closeFile( descFileHandle );

			directoryOfSlots[fileName] = freeSpace;

		}
	}

	return result;
}

RC RecordBasedFileManager::readHeader(FileHandle &headerFileHandle, unsigned pageNo, vector<short> *freeSpace)
{
	char *page = (char *)malloc(PAGE_SIZE);

	int result = headerFileHandle.readPage( pageNo, page );
	if( result == 0 )
	{
		short numOfPages = *(short*)page;

		for(short i = 1; i <= numOfPages; i++ )
		{
			freeSpace->push_back(*((short *)(page + sizeof(short) * i)));
		}
	}

	free(page);
	return result;
}

RC RecordBasedFileManager::writeHeader(FileHandle &headerFileHandle, unsigned pageHeaderNo, unsigned &currentHeader, vector<short> *freeSpace)
{
	RC result = -1;
	char * page = (char *)malloc(PAGE_SIZE);
	short numOfPages  = 0;
	int offset = sizeof(short);

	while( (currentHeader < freeSpace->size()) &&
			(numOfPages < HEADER_PAGE_SIZE) )
	{
		*(short*)(page + offset) = (*freeSpace)[currentHeader];
		numOfPages++;
		currentHeader++;
		offset += sizeof(short);
	}

	*(short *)page = numOfPages;

	result = headerFileHandle.writePage( pageHeaderNo, page );
	free(page);

	return result;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
	int result = -1;

	if( fileHandle.getFile() == NULL )
		return result;

	cout << fileHandle.getFileName() << endl;

	if( directoryOfSlots.find( fileHandle.getFileName() ) == directoryOfSlots.end() )
		return result;

	vector<short>* freeSpace = directoryOfSlots[fileHandle.getFileName()];
	int numOfPages = (int)freeSpace->size();
	int numOfHeaderPages = numOfPages / HEADER_PAGE_SIZE;
	if( numOfPages % HEADER_PAGE_SIZE != 0 )
		numOfHeaderPages++;

	FileHandle headerFileHandle;
	const char* desc = "_desc";

	string fileName = fileHandle.getFileName();
	string descFileName = string(fileName) + string(desc);

	pfm->openFile( descFileName.c_str(), headerFileHandle );

	char* page = (char*)malloc(PAGE_SIZE);
	unsigned pageNo = 0;
	while(headerFileHandle.getNumberOfPages() < numOfHeaderPages)
	{
		headerFileHandle.appendPage( page );
	}
	free( page );
	unsigned currentHeader = 0;

	for( unsigned pageNo = 0; pageNo < numOfHeaderPages; pageNo++)
	{
		result = writeHeader(headerFileHandle, pageNo, currentHeader, freeSpace);
		if( result != 0 )
			return result;
	}

	result = pfm->closeFile( headerFileHandle );
	if( result != 0 )
		return result;

	result = pfm->closeFile( fileHandle );

	return result;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	RC result = -1;
	PageNum pageNum = 0;
	PageNum pageNumToAddNewPage = 0;

	if( fileHandle.getFile() == NULL )
		return result;

	if( directoryOfSlots.find(fileHandle.getFileName())
			== directoryOfSlots.end() )
		return result;

	// allocation memory for record
	short sizeOfRecord = getSizeOfRecord( recordDescriptor, data );

	void *record = malloc( sizeOfRecord );
	dataToRecord( data, recordDescriptor, record );

	vector<short> *slotDirectory = directoryOfSlots[fileHandle.getFileName()];

	char *page = (char*)malloc(PAGE_SIZE);

	// find first blank slot
	for(; pageNum < slotDirectory->size(); pageNum++ )
	{
		if( (sizeOfRecord +  sizeof(Slot)) <= (*slotDirectory)[pageNum] )
		{
			// add record to the page
			fileHandle.readPage( pageNum, page );

			const char* endOfPage = page + PAGE_SIZE;
			DirectoryOfSlotsInfo *info = getDirectoryOfSlotsInfo(endOfPage);

			unsigned slotNum = info->numOfSlots + 1;

			appendRecord(page, record, sizeOfRecord, slotNum);

			(*slotDirectory)[pageNum] -= sizeOfRecord + sizeof(Slot);

			result = fileHandle.writePage(pageNum, page);
			if( result == 0 )
			{
				rid.pageNum = pageNum;
				rid.slotNum = slotNum;
			}

			free(record);
			free(page);

			return result;
		}
	}

	// If slotDirecotory is null
	result = appendPageWithRecord( fileHandle, record,  sizeOfRecord );
	if( result == 0 )
	{
		rid.pageNum = pageNum;
		rid.slotNum = 1;
	}

	free(record);
	free(page);

	return result;
}

RC RecordBasedFileManager::appendPageWithRecord(FileHandle &fileHandle, const void* record, int sizeOfRecord)
{
	void * page = malloc(PAGE_SIZE);

	newPageForRecord(record, page, sizeOfRecord);

	RC result = fileHandle.appendPage( page );
	if( result == 0 )
	{
		vector<short>* freeSpace = directoryOfSlots[fileHandle.getFileName()];
		freeSpace->push_back( PAGE_SIZE - sizeOfRecord - sizeof(directoryOfSlotsInfo) - sizeof(Slot)*2);
	}

	free(page);
	return result;
}

RC RecordBasedFileManager::appendRecord(char *page, const void *record, short sizeOfRecord, unsigned slotNum)
{
	const char* endOfPage = page + PAGE_SIZE;

	DirectoryOfSlotsInfo* info = getDirectoryOfSlotsInfo(endOfPage);

	if( slotNum > (unsigned)info->numOfSlots + 1 )
		return -1;

	Slot* slot = goToSlot(endOfPage, slotNum);
	short freeSpaceOffset = info->freeSpaceOffset;
	memcpy(page + freeSpaceOffset, record, sizeOfRecord);

	slot->begin = freeSpaceOffset;
	slot->end = freeSpaceOffset + sizeOfRecord;

	info->freeSpaceOffset = slot->end;

	if( slotNum > (unsigned)info->numOfSlots )
		info->numOfSlots++;

	return 0;
}

RC RecordBasedFileManager::newPageForRecord(const void* record, void * page, int sizeOfRecord)
{
	memcpy( page, record, sizeOfRecord );

	char *endOfPage = (char *)page + PAGE_SIZE;

	// update directoryOfSlots Info.
	DirectoryOfSlotsInfo *info =  goToDirectoryOfSlotsInfo(endOfPage);
	info->numOfSlots = 1;
	info->freeSpaceOffset = sizeOfRecord;

	// add Slot Information
	Slot* slot = goToSlot( endOfPage, 1 );
	slot->begin = 0;
	slot->end = sizeOfRecord;

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

	RC result = 0;

	if( fileHandle.getFile() == NULL )
		return -1;

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

	char* page = (char*)malloc(PAGE_SIZE);

	// find page
	result = fileHandle.readPage(pageNum, page);
	if( result != 0 )
		return result;

	// reach to the end of Page
	const char* endOfPage = page + PAGE_SIZE;

	// find slot
	Slot* slot = goToSlot(endOfPage, slotNum);
	if( slot->begin < 0 )
		return result;

	char* beginOfRecord = page + slot->begin;

	short lengthOfRecord = slot->end - slot->begin;
	void* record = malloc(lengthOfRecord);

	// read record
	memcpy( record, beginOfRecord, lengthOfRecord);

	result = recordToData(record, recordDescriptor, data);

	free(record);
	free(page);

	return result;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	int offset  = 0;

	printf("recordDescriptor.size() = %d\n", recordDescriptor.size());
	for (unsigned i = 0; i < recordDescriptor.size(); ++i)
	{
		Attribute attr = recordDescriptor.at(i);

		if( attr.type == TypeInt )
		{
			printf("%d\n", *((int*)((char*)data + offset)));
			offset += attr.length;
		}
		else if( attr.type == TypeReal )
		{
			printf("%.4f\n", *((float*)((char*)data + offset)));
			offset += attr.length;
		}
		else if( attr.type == TypeVarChar )
		{
			int varLength = *((int*)((char*)data + offset));
			offset += sizeof(int);

			for(int j = 0; j < varLength; j++)
			{
				printf("%c", *((char*)data + offset));
				offset++;
			}
			printf("\n");

		}
	}

	printf("\n");

    return 0;
}

short RecordBasedFileManager::getSizeOfRecord(const vector<Attribute> &recordDescriptor, const void* data)
{

	short length = 2 * sizeof(short);
	short offset = 0;
	int varLength = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		length += sizeof(short);	 // increment by the size of record-directory entry

		Attribute attr = recordDescriptor[i];

		if( attr.type == TypeInt )
		{
			length += attr.length;
			offset += attr.length;
		}
		else if( attr.type == TypeReal )
		{
			length += attr.length;
			offset += attr.length;
		}
		else if( attr.type == TypeVarChar )
		{
			memcpy( &varLength, (char *)data + offset, sizeof(int));
			length += varLength;
			offset += varLength + sizeof(int);
		}
	}

	return length;
}

RC RecordBasedFileManager::recordToData(void* record, const vector<Attribute> &recordDescriptor, const void* data)
{
	short offset = 0;
	short elementStart = 0;

	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		Attribute attr = recordDescriptor[i];

		elementStart  = *((short*)record + i + 1);

		if( attr.type == TypeInt )
		{
			memcpy( (char*)data+offset, (char*)record+elementStart, sizeof(int) );
			offset += sizeof(int);
		}
		else if( attr.type == TypeReal )
		{
			memcpy( (char*)data+offset, (char*)record+elementStart, sizeof(float) );
			offset += sizeof(float);
		}
		else if( attr.type == TypeVarChar )
		{
			// Digest and elaborate more
			short elementEnd = *((short*)record + i + 2);
			int varLength = elementEnd - elementStart;

			memcpy( (char*)data+offset, &varLength, sizeof(int));
			offset += sizeof(int);

			memcpy( (char*)data+offset, (char*)record+elementStart, varLength);
			offset += varLength;
		}
	}

	return 0;
}

RC RecordBasedFileManager::dataToRecord(const void* data, const vector<Attribute> &recordDescriptor, void* record)
{
	short elementStart = 0;
	short offset = (recordDescriptor.size() + 2)* sizeof(short);	// [data field1][data field2]...[output offset]
	int varLength = 0;

	unsigned i = 0;
	for (; i < recordDescriptor.size(); i++)
	{
		Attribute attr = recordDescriptor[i];

		*((short*)record + i + 1) = offset;	// Digest!

		if( attr.type == TypeInt )
		{
			memcpy( (char*)record + offset, (char*)data + elementStart, sizeof(int));
			offset += sizeof(int);
			elementStart += sizeof(int);
		}
		else if( attr.type == TypeReal )
		{
			memcpy( (char*)record + offset, (char*)data + elementStart, sizeof(float));
			offset += sizeof(float);
			elementStart += sizeof(float);
		}
		else if( attr.type == TypeVarChar )
		{
			memcpy( &varLength, (char*)data + elementStart, sizeof(int));
			elementStart += sizeof(int);

			memcpy( (char*)record + offset, (char*)data + elementStart, varLength);

			offset += varLength;
			elementStart += varLength;
		}


	}

	*((short *) record + i + 1 ) = offset;

	return 0;

}

Slot*  RecordBasedFileManager::goToSlot(const void* endOfPage, unsigned slotNo)
{
	char* slot = (char*)endOfPage;
	slot = slot - sizeof(DirectoryOfSlotsInfo) - slotNo * sizeof(Slot);
	return (Slot*)slot;

}

DirectoryOfSlotsInfo* RecordBasedFileManager::goToDirectoryOfSlotsInfo(const char* endOfPage)
{
	char* directoryInfo = (char*)endOfPage;
	directoryInfo =(char*)directoryInfo - sizeof(DirectoryOfSlotsInfo);
	return (DirectoryOfSlotsInfo*)directoryInfo;
}