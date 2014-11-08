
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
//	printf("$%s$",fileName.c_str());
//	cout<<fileName.c_str()<<endl;
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

//	cout << fileHandle.getFileName() << endl;

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
//	unsigned pageNo = 0;
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

	if( fileHandle.getFile() == NULL ) //IERROR Run case 6, sisgtrp
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
	memset(page, 0, PAGE_SIZE);

	// find first blank slot
//	for(; slotDirectory!=NULL && pageNum < slotDirectory->size(); pageNum++ )
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

			(*slotDirectory)[pageNum] -= (sizeOfRecord + sizeof(Slot));

//			cout<<"insertRecord pageNum=" << pageNum << endl;

			result = fileHandle.writePage(pageNum, page);
			if( result == 0 )
			{
				rid.pageNum = pageNum;
				rid.slotNum = slotNum;
			}

//			cout<<"insertRecord writePage=" << result << endl;

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
	string filename = fileHandle.getFileName();
	RC result = fileHandle.appendPage( page );
	if( result == 0 )
	{
		vector<short>* freeSpace = directoryOfSlots[fileHandle.getFileName()];
		freeSpace->push_back( PAGE_SIZE - sizeOfRecord - sizeof(directoryOfSlotsInfo) - sizeof(Slot)*2);
//		for(int iter1 = 0; iter1<freeSpace->size(); iter1++)
//			cout <<"size: "<< freeSpace->at(iter1) << endl;

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

	RC result = -1;
//	Slot* slot;

	if( fileHandle.getFile() == NULL )
		return result;

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

	char* page = (char*)malloc(PAGE_SIZE);

	bool isTombStone = true;
	while( isTombStone )
	{
			// find page
			result = fileHandle.readPage(pageNum, page);
			if( result != 0 )
				break;

			// reach to the end of Page
			const char* endOfPage = page + PAGE_SIZE;

			Slot* slot = goToSlot(endOfPage, slotNum);
			if( slot->begin < 0 )
			{
				result = -1;
				break;
			}

			char* beginOfRecord = page + slot->begin;
			isTombStone = isRecordTombStone(beginOfRecord, pageNum, slotNum);

			if( !isTombStone ) {

				short lengthOfRecord = slot->end - slot->begin;
				void* record = malloc(lengthOfRecord);

				// read record
				memcpy( record, beginOfRecord, lengthOfRecord);
				result = recordToData(record, recordDescriptor, data);
				free(record);
			}
	}

	free(page);

	return result;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	int offset  = 0;

	//printf("recordDescriptor.size() = %d\n", recordDescriptor.size());
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
	short tmpSize = recordDescriptor.size();
	for (short i = 0; i < recordDescriptor.size(); i++)
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
			/*
			if(varLength<0){
				puts("Hi");
				printRecord(recordDescriptor, record);
			}
			*/
//			printf("%d %d %d\n",offset, elementStart, varLength);
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


RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	RC result = -1;

//	cout << "deleteRecords: fileHandle.getFileName()=" << fileHandle.getFileName() << endl;

	string fileName = fileHandle.getFileName();

	const char* cfileName = fileName.c_str();

//	cout << "fileName:" << cfileName << endl;

	result = pfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: closeFile:" << result << endl;

	result = pfm->destroyFile( cfileName );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: destroyFile:" << result << endl;

	result = pfm->createFile( cfileName );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: createFile:" << result << endl;

	result = pfm->openFile( cfileName, fileHandle );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: openFile:" << result << endl;
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	RC result = -1;

	if( fileHandle.getFile() == NULL )
			return result;

//	cout << "deleteRecord: fileHandle.getFile()" << result << endl;

	if( directoryOfSlots.find( fileHandle.getFileName() ) ==
			directoryOfSlots.end() )
		return result;

//	cout << "deleteRecord:directoryOfSlots" << result << endl;

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;

//	cout << "deleteRecord:pageNum=" << pageNum << " slotNum="<< slotNum << endl;

	vector<short> *slotDirectory = directoryOfSlots[fileHandle.getFileName()];

//	cout << "deleteRecord:slotDirectory size" << slotDirectory->size() << endl;

	char* page = (char*)malloc( PAGE_SIZE );

	bool isTombStone = true;
	while( isTombStone )
	{
		result = fileHandle.readPage( pageNum, page );
		if( result != 0 )
			return result;

//		cout << "deleteRecord:readPage" << result << endl;

		const char* endOfPage = page + PAGE_SIZE;

		Slot* slot = goToSlot( endOfPage, slotNum );
		if( slot->begin < 0 ) // delete a deleted record
		{
			result = -1;
			break;
		}

//		cout << "deleteRecord:goToSlot" << result << endl;
		char* data = page + slot->begin;
		isTombStone = isRecordTombStone( data, pageNum, slotNum );
		slot->begin = -1 - slot->begin;

//		cout << "deleteRecord:data set to -1" << endl;

//		(*slotDirectory)[pageNum] += sizeOfRecord;// increase multiple times?
		result = fileHandle.writePage( pageNum, page );
		if( result != 0 )
			break;

//		cout << "deleteRecord:writePage" << endl;

	}

	free(page);
	return result;
}

RC RecordBasedFileManager::shiftSlotInfo(void* pageData, short shiftOffset, short slotNum){
	// shift all slot behind with offset, not include
	char* endOfPage = (char *) pageData + PAGE_SIZE;
	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);
	Slot* slot;
	short slotN = dirInfo->numOfSlots;
	for(int iter1 = slotNum+1; iter1<=slotN; iter1++){
		slot = goToSlot(endOfPage,iter1);
		slot->begin+=shiftOffset;
		slot->end+=shiftOffset;
	}
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	// Routine of update record.
	RC result = -1;
//	cout<<2.1<<endl;
	void * pageData = malloc(PAGE_SIZE);
	char * endOfPage = (char*)pageData + PAGE_SIZE;

	result = fileHandle.readPage(rid.pageNum,pageData);
	if( result != 0 )
	{
		free( pageData );
		return result;
	}

	vector<short> *freeSpace = directoryOfSlots[fileHandle.getFileName()];

//	cout<<2.2<<endl;
	Slot* slot = goToSlot(endOfPage,rid.slotNum);
	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);

//	cout<<2.3<<endl;
	short oldRecordSize = slot->end - slot->begin;
	short newRecordSize = getSizeOfRecord(recordDescriptor, data);
	void *newRecordData = malloc(newRecordSize);
	short sizeDiff = newRecordSize - oldRecordSize;
	dataToRecord(data, recordDescriptor, newRecordData);
	/*
	if(PAGE_SIZE - dirInfo->freeSpaceOffset == freeSpace->at(rid.pageNum))
					cout<<"true"<<endl;
				else
					cout<<"false"<<endl;
					*/
	if(oldRecordSize==newRecordSize){
//		cout<<"equal"<<endl;
		memcpy((char*) pageData + slot->begin, newRecordData, oldRecordSize);
		result = fileHandle.writePage( rid.pageNum, pageData );
	}
	else{
		if (sizeDiff > (*freeSpace)[rid.pageNum])
		{
//			cout<<"reorg"<<endl;
			reorganizePage(fileHandle, recordDescriptor, rid.pageNum);
//			fileHandle.readPage(rid.pageNum,pageData);
		}

		if(newRecordSize < oldRecordSize||
				(newRecordSize > oldRecordSize && (sizeDiff < (freeSpace)->at(rid.pageNum) ) ) ){
//			short shiftDataBlockSize = dirInfo->freeSpaceOffset - slot->end;
//			cout<<"ShiftBlockSize "<<shiftDataBlockSize<<" block end "<<slot->end+sizeDiff+shiftDataBlockSize
//					<<"Page start"<<pageData<<" record end "<<slot->end<<endl;
			//memmove((char*)pageData + slot->end + sizeDiff, (char*)pageData + slot->end, shiftDataBlockSize);
//			shiftSlotInfo(pageData, sizeDiff,rid.slotNum);
//			dirInfo->freeSpaceOffset += sizeDiff;
//			slot->end += sizeDiff;
			memcpy((char *)pageData + slot->begin, newRecordData, newRecordSize);
			slot->end = slot->begin+newRecordSize;
//			(*freeSpace)[rid.pageNum] -=sizeDiff;
//			result = fileHandle.writePage( rid.pageNum, pageData );
		}
		else{// not enough space, set tombstone, add point to new record
			RID newRid;
			cout<<"not enough space"<<endl;
			result = insertRecord(fileHandle, recordDescriptor, data, newRid);
			fileHandle.readPage(rid.pageNum,pageData);
			setRecordTombStone((char*)pageData+slot->begin, newRid.pageNum, newRid.slotNum);
//			result = fileHandle.writePage( rid.pageNum, pageData );
		}

		result = fileHandle.writePage( rid.pageNum, pageData );
	}
	free(newRecordData);
	free(pageData);
	return result;
}

int RecordBasedFileManager::getEstimatedRecordDataSize(vector<Attribute> recordDesciptor){
	int res = 0;
	for(int iter1 = 0; iter1<recordDesciptor.size(); iter1++)
		res+=recordDesciptor.at(iter1).length;
	return res;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	RC result = -1;
	int estimateRecordLen = getEstimatedRecordDataSize(recordDescriptor);
	int fieldPointer = 0;
	void* recordData = malloc(estimateRecordLen);
	readRecord(fileHandle, recordDescriptor,rid,recordData);
	for(int iter1 = 0; iter1<recordDescriptor.size(); iter1++){
		if(recordDescriptor.at(iter1).name == attributeName){
			result = 0;
			if(recordDescriptor.at(iter1).type == TypeInt)
				memcpy(data, (char *)recordData+fieldPointer, sizeof(int));
			else if(recordDescriptor.at(iter1).type == TypeReal)
				memcpy(data, (char *)recordData+fieldPointer, sizeof(float));
			else{
				int varLen = *(int *)((char*)recordData + fieldPointer);
				memcpy(data, (char *)recordData+fieldPointer+sizeof(int), varLen);
			}
		}
		else{
			if(recordDescriptor.at(iter1).type == TypeInt)
				fieldPointer += sizeof(int);
			else if(recordDescriptor.at(iter1).type == TypeReal)
				fieldPointer +=sizeof(float);
			else{
				int varLen = *(int *)((char*)recordData + fieldPointer);
				fieldPointer +=(varLen + sizeof(int));
			}
		}
	}
	free(recordData);
	return result;
}


bool RecordBasedFileManager::checkTombStone(void* pageData, int pageId, int slotId){
	bool result = false;
	Slot* slot = goToSlot((char*)pageData+PAGE_SIZE, slotId);
	void* recordPos = (char*) pageData+slot->begin;
	if(*(short*)recordPos == -1)
		result = true;
	else
		result = false;
	return result;
}

RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
{
	RC result = -1;

	if( fileHandle.getFile() == NULL )
		return result;

	if( directoryOfSlots.find(fileHandle.getFileName()) == directoryOfSlots.end() )
		return result;

	unsigned pageN = pageNumber;
	void* page = malloc(PAGE_SIZE);
	void* reorganizedPage = malloc(PAGE_SIZE);

	result = fileHandle.readPage(pageN, page);

	memcpy( reorganizedPage, page, PAGE_SIZE );

	const char* endOfPage = (char *)page + PAGE_SIZE;
	const char* endOfReorganizedPage = (char*)reorganizedPage + PAGE_SIZE;

	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);
	Slot * slot = goToSlot(endOfPage, 1);

	short slotNum = dirInfo->numOfSlots;

	DirectoryOfSlotsInfo* reOrgPagedirInfo = goToDirectoryOfSlotsInfo(endOfReorganizedPage);
	Slot * reOrgSlot = goToSlot(endOfReorganizedPage, 1);

	short offset = 0;

	for( unsigned iter1 = 0; iter1 < slotNum; iter1++ ) {

		// data is there, let's move them
		if( slot->begin > 0  && slot->end > 0 )
		{
			short recordSize = slot->end - slot->begin;
			memcpy( (char*)reorganizedPage + offset, (char*)page + slot->begin, recordSize);
			reOrgSlot->begin = offset;
			reOrgSlot->end = offset + recordSize;
			offset += recordSize;
		}

		// deleted record
		if( slot->begin < 0 )
		{
			reOrgSlot->begin = slot->begin;
			reOrgSlot->end = slot->end;

			/*
			short trueStart = 0 - slot->begin-1;
			short recordSize = slot->end - trueStart;// as the slot begin is already -0
			short moveBlockSize = dirInfo->freeSpaceOffset - slot->end;

			memmove((char *)pageData + trueStart, (char *)pageData + slot->end, moveBlockSize);
			shiftSlotInfo(pageData, 0 - recordSize, iter1);
			dirInfo->freeSpaceOffset += recordSize;
			*/
		}

		slot--;
		reOrgSlot--;

		// tombstone
		/*
		if( checkTombStone(page, pageN, iter1) ){
			char* endOfPage = (char*) page + PAGE_SIZE;
			Slot* slot = goToSlot((char*)page+PAGE_SIZE, iter1);
			int tombStoneSize = 2*sizeof(unsigned)+sizeof(short);
			DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);
			short dataBlockBehindSize = dirInfo->freeSpaceOffset - slot->end;
			short recordSize = slot->end - slot->begin;
			short shiftOffset = recordSize - tombStoneSize;

			memmove((char*) page + slot->begin + tombStoneSize, (char*) page + slot->end,dataBlockBehindSize);
			dirInfo->freeSpaceOffset -= (recordSize - tombStoneSize);

			shiftSlotInfo(page, 0-shiftOffset, iter1);
			slot->end = slot->begin + tombStoneSize;

			vector<short> *freeSpace = directoryOfSlots[fileHandle.getFileName()];
			(*freeSpace)[pageN] +=shiftOffset;
		}
		*/
	}

//	result = fileHandle.writePage(pageN, page);
	result = fileHandle.writePage(pageN, reorganizedPage);

	reOrgPagedirInfo->numOfReorgSlots = 0;
	reOrgPagedirInfo->freeSpaceOffset = offset;

	vector<short> *freeSpace = directoryOfSlots[fileHandle.getFileName()];
	(*freeSpace)[pageN] = PAGE_SIZE - offset - sizeof(DirectoryOfSlotsInfo) - reOrgPagedirInfo->numOfSlots*sizeof(Slot);

	free(page);
	free(reoranizePage);
	return result;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator)
{
	return rbfm_ScanIterator.initialize(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
}

RC RecordBasedFileManager::reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor)
{
	RC result = -1;

	return result;
}

RBFM_ScanIterator::RBFM_ScanIterator()
{
	compOp = NO_OP;
	condition = NULL;
	this->conditionAttrType = TypeInt;
	this->conditionAttrNum = 0;

	pageNum = 0;
	slotNum = 0;

	pageData = NULL;
	endOfPage = NULL;

	constructAttrNum.clear();
	constructAttrType.clear();
	tombstoneMap.clear();
}

RBFM_ScanIterator::~RBFM_ScanIterator(){}

RC RBFM_ScanIterator::close()
{
	compOp = NO_OP;
	condition = NULL;
	this->conditionAttrType = TypeInt;
	this->conditionAttrNum = 0;

	pageNum = 0;
	slotNum = 0;

	free(pageData);
	pageData = NULL;
	endOfPage = NULL;

	constructAttrNum.clear();
	constructAttrType.clear();
	tombstoneMap.clear();

	return 0;
}


RC RBFM_ScanIterator::initialize(FileHandle &fileHandle,
		  const vector<Attribute> &recordDescriptor,
		  const string &conditionAttribute,
		  const CompOp compOp,
		  const void *value,
		  const vector<string> &attributeNames)
{

	RC result = -1;

	this->fileHandle = fileHandle;
	this->recordDescriptor = recordDescriptor;
	this->conditionAttrName = conditionAttribute;
	this->targetPointer = value;
	this->compOp = compOp;
	this->attributeNames = attributeNames;

	this->condition = value;

//	cout << " RBFM_ScanIterator::initialize: fileName=" << fileHandle.getFileName()
//			<< ", fileHandle.getNumberOfPages()="<< fileHandle.getNumberOfPages() << endl;

//	if( condition == NULL )
//		cout << " RBFM_ScanIterator::initialize:" << "condition is NULL"  << endl;

	pageNum = 0;
	slotNum = 1;
	totalPageNum = fileHandle.getNumberOfPages();

	pageData = (char*) malloc( PAGE_SIZE );
	memset( pageData, 0, PAGE_SIZE );
	fileHandle.readPage(0, pageData); // read the first page
	endOfPage = pageData + PAGE_SIZE;

	dirInfo = _rbfm->goToDirectoryOfSlotsInfo(endOfPage);
	totalSlotNum = dirInfo->numOfSlots;

//	cout << "RBFM_ScanIterator::initialize" << endl;

	return 0;
}
/*
bool RBFM_ScanIterator::checkConditionForAttribute(void* attribute, const void* condition, AttrType attrType, CompOp compOp){
	bool result = true;

	if( condition == NULL )
	{
		return result;
	}

	// Read field value and compare
	if(attrType == TypeInt){
		int attr = *(int *)attribute;
		int cond = *(int *)condition;

		cout << "checkConditionForAttribute: TypeInt" << "attr" << attr << ", cond=" << cond << endl;
		cout << "compOp:" << compOp << endl;

		switch(compOp){
			case EQ_OP :  result = (attr == cond); break;
			case LT_OP :  result = (attr < cond); break;    // <
			case GT_OP :  result = (attr > cond); break;   // >
			case LE_OP :  result = (attr <= cond); break;   // <=
			case GE_OP :  result = (attr >= cond); break;   // >=
			case NE_OP :  result = (attr != cond); break;   // !=
			case NO_OP :  result = true; break;
		}
	}
	else if (attrType == TypeReal){
		int attr = *(float *)attribute;
		int cond = *(float *)condition;

		int compareResult ;

		cout << "checkConditionForAttribute: TypeReal" << "attr" << attr << ", cond=" << cond << endl;
		cout << "compOp:" << compOp << endl;

		if( attr - cond > 0.00001 )
			compareResult = 1;
		else if( attr - cond < -0.00001 )
			compareResult = -1;
		else
			compareResult = 0;

		switch(compOp){
			case EQ_OP :  result = (compareResult == 0); break;
			case LT_OP :  result = (compareResult < 0); break;    // <
			case GT_OP :  result = (compareResult > 0); break;   // >
			case LE_OP :  result = (compareResult <= 0); break;   // <=
			case GE_OP :  result = (compareResult >= 0); break;   // >=
			case NE_OP :  result = (compareResult != 0); break;   // !=
			case NO_OP :  result = true; break;
		}
	}
	else{
		int attrLen = *(char*)attribute;
		string attr((char*)attribute + sizeof(int), attrLen);

		int condLen = *(char*)condition;
		string cond((char*)condition + sizeof(int), condLen);

		int strCmpRes = strcmp(attr.c_str(), cond.c_str());
		cout << "checkConditionForAttribute: strCmpRes" << strCmpRes << "attr" << attr << ", cond=" << cond << endl;
		cout << "compOp:" << compOp << endl;
		switch(compOp){
			case EQ_OP :  result = (strCmpRes == 0); break;
			case LT_OP :  result = (strCmpRes < 0 ); break;    // <
			case GT_OP :  result = (strCmpRes > 0); break;   // >
			case LE_OP :  result = (strCmpRes <= 0); break;   // <=
			case GE_OP :  result = (strCmpRes >= 0); break;   // >=
			case NE_OP :  result = (strCmpRes != 0); break;   // !=
			case NO_OP :  result = true; break;
		}
	}

	cout << "checkConditionForAttribute result: " << result << endl;
	return result;
}
*/
bool RBFM_ScanIterator::checkCondition(void* data, string &attrName, vector<Attribute> &recordDescriptor){
	if(targetPointer == NULL)
		return true;
	bool result = false;
	AttrType attrType;
	int flag = -1;
	short fieldOffset = 0;
	int varLen = 0;
	// Find the right type and position of compare field
	int tmp = recordDescriptor.size();
	for(vector<Attribute>::iterator iter1 = recordDescriptor.begin(); !result&&iter1!=recordDescriptor.end();iter1++){
		if(iter1->name == attrName){
			attrType = iter1->type;
			flag = 0;
			break;
		}
		else{
			if(iter1->type == TypeVarChar){
				varLen = *(int *)data;
				fieldOffset +=(sizeof(int)+varLen);
			}
			else if(iter1->type == TypeInt)
				fieldOffset += sizeof(int);
			else
				fieldOffset += sizeof(float);
		}
	}
	if(flag == -1) // if field not found, return false
		return false;
	// Read field value and compare
	if(attrType == TypeInt){
		int fieldValue = *(int *)((char *)data+fieldOffset);
		int targetValue = *(int *)targetPointer;
		switch(compOp){
			case EQ_OP :  result = (fieldValue == targetValue); break;
			case LT_OP :  result = (fieldValue < targetValue); break;    // <
			case GT_OP :  result = (fieldValue > targetValue); break;   // >
			case LE_OP :  result = (fieldValue <= targetValue); break;   // <=
			case GE_OP :  result = (fieldValue >= targetValue); break;   // >=
			case NE_OP :  result = (fieldValue != targetValue); break;   // !=
			case NO_OP :  result = true; break;
		}
	}
	else if (attrType == TypeReal){
		int fieldValue = *(int *)((char *)data+fieldOffset);
		float targetValue = *(float *)targetPointer;
		switch(compOp){
			case EQ_OP :  result = (fieldValue == targetValue); break;
			case LT_OP :  result = (fieldValue < targetValue); break;    // <
			case GT_OP :  result = (fieldValue > targetValue); break;   // >
			case LE_OP :  result = (fieldValue <= targetValue); break;   // <=
			case GE_OP :  result = (fieldValue >= targetValue); break;   // >=
			case NE_OP :  result = (fieldValue != targetValue); break;   // !=
			case NO_OP :  result = true; break;
		}
	}
	else{
		char* fieldValue;
		char* targetValue = (char *) targetPointer;
		varLen = *(int*)((char *)data+fieldOffset);
		fieldValue = (char *)malloc(varLen);
		memcpy(fieldValue, (char *)data+fieldOffset+sizeof(int), varLen);
		int strCmpRes = strcmp(fieldValue, targetValue);
		switch(compOp){
			case EQ_OP :  result = (strCmpRes == 0); break;
			case LT_OP :  result = (strCmpRes < 0 ); break;    // <
			case GT_OP :  result = (strCmpRes > 0); break;   // >
			case LE_OP :  result = (strCmpRes <= 0); break;   // <=
			case GE_OP :  result = (strCmpRes >= 0); break;   // >=
			case NE_OP :  result = (strCmpRes != 0); break;   // !=
			case NO_OP :  result = true; break;
		}
		free(fieldValue);
	}
	return result;
}

RC  RBFM_ScanIterator::inrecreaseIteratorPos(){
	slotNum += 1;
	if(slotNum>dirInfo->numOfSlots){
		pageNum +=1;
		slotNum = 1;
	}
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	Slot* slot;
	RC result= -1;
	RC tombStoneChk = -1;

	while( pageNum < totalPageNum && result ){

		if( slotNum > totalSlotNum )
		{
			slotNum = 1;
			pageNum++;

			if( pageNum >= fileHandle.getNumberOfPages() )
				return RBFM_EOF;

			fileHandle.readPage( pageNum, pageData );
			endOfPage = pageData + PAGE_SIZE;
		}

		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

		// read the record out
		slot = _rbfm->goToSlot(endOfPage, slotNum);
		short estimateRecordLen = slot->end - slot->begin;
		if( estimateRecordLen <= 0 )
			break;

		void* recordData = malloc(estimateRecordLen);
		tombStoneChk = _rbfm->readRecord(fileHandle, recordDescriptor, rid, recordData);

		inrecreaseIteratorPos();

		if(tombStoneChk == -1)
		{
			free(recordData);
			continue;
		}
		/*
		else
			puts("hi!");
			*/
		// see the condition
		if(checkCondition(recordData, conditionAttrName, recordDescriptor)){
			result = 0;
			int offSet = 0,outOffset= 0;
			//memcpy(data, recordData, estimateRecordLen);
			for(vector<string>::iterator iter1 = attributeNames.begin(); iter1!=attributeNames.end(); iter1++){
				for(vector<Attribute>::iterator iter2 = recordDescriptor.begin(); iter2!=recordDescriptor.end(); iter2++){
					if(*iter1 == iter2->name){
						if(iter2->type == TypeInt){
							memcpy((char*)data + outOffset, (char *)recordData+offSet, sizeof(int));
							outOffset +=sizeof(int);
						}
						else if(iter2->type == TypeReal){
							memcpy((char*)data + outOffset, (char *)recordData+offSet, sizeof(float));
							outOffset += sizeof(int);
						}
						else{
							int varLen = *(int *)((char*)recordData + offSet);
							memcpy((char*)data + outOffset, &varLen, sizeof(int));
							memcpy((char*)data + outOffset+sizeof(int), (char *)recordData+offSet+sizeof(int), varLen);
							outOffset +=(varLen + sizeof(int));
						}
						offSet = 0;
						break;
					}
					else{
						if(iter2->type == TypeInt)
							offSet += sizeof(int);
						else if(iter2->type == TypeReal)
							offSet +=sizeof(float);
						else{
							int varLen = *(int *)((char*)recordData + offSet);
							offSet +=(varLen + sizeof(int));
						}
					}
				}
			}
		}

		free(recordData);
	}

	return result;
}
