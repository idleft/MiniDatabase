
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

//	cout<<"##FileName##"+fileHandle.getFileName()<<endl;
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
	Slot* slot;

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
// Xikui
			if( slot->begin < 0 )
				return -1;
// Xikui

			char* beginOfRecord = page + slot->begin;
			isTombStone = isRecordTombStone(beginOfRecord, pageNum, slotNum);

/* Xikui
			if( (short*)beginOfRecord < 0 )
				return -1;
*/
			//	cout << "readRecord beginOfRecord" << endl;

			if( !isTombStone ) {

				short lengthOfRecord = slot->end - slot->begin;
				void* record = malloc(lengthOfRecord);

				//	cout << "readRecord lengthOfRecord=" << lengthOfRecord << endl;

				// read record
				memcpy( record, beginOfRecord, lengthOfRecord);

				//	cout << "readRecord memcpy=" << lengthOfRecord << endl;

				result = recordToData(record, recordDescriptor, data);

				//	cout << "readRecord recordToData=" << result << endl;

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


RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	RC result = -1;

	cout << "deleteRecords: fileHandle.getFileName()=" << fileHandle.getFileName() << endl;

	string fileName = fileHandle.getFileName();

	const char* cfileName = fileName.c_str();

	cout << "fileName:" << cfileName << endl;

	result = pfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

	cout << "deleteRecords: closeFile:" << result << endl;

	result = pfm->destroyFile( cfileName );
	if( result != 0 )
		return result;

	cout << "deleteRecords: destroyFile:" << result << endl;

	result = pfm->createFile( cfileName );
	if( result != 0 )
		return result;

	cout << "deleteRecords: createFile:" << result << endl;

	result = pfm->openFile( cfileName, fileHandle );
	if( result != 0 )
		return result;

	cout << "deleteRecords: openFile:" << result << endl;
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

		cout << "deleteRecord:writePage" << endl;

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
	fileHandle.readPage(rid.pageNum,pageData);
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
	if(PAGE_SIZE - dirInfo->freeSpaceOffset == freeSpace->at(rid.pageNum))
					cout<<"true"<<endl;
				else
					cout<<"false"<<endl;
	if(oldRecordSize==newRecordSize){
		cout<<"equal"<<endl;
		memcpy((char*) pageData + slot->begin,newRecordData,oldRecordSize);
		result = fileHandle.writePage( rid.pageNum, pageData );
	}
	else{
		if (sizeDiff > (*freeSpace)[rid.pageNum])
		{
			cout<<"reorg"<<endl;
			reorganizePage(fileHandle, recordDescriptor, rid.pageNum);
			fileHandle.readPage(rid.pageNum,pageData);
		}
		if(newRecordSize < oldRecordSize||
				(newRecordSize > oldRecordSize && (sizeDiff < (freeSpace)->at(rid.pageNum) ) ) ){
			short shiftDataBlockSize = dirInfo->freeSpaceOffset - slot->end;
			cout<<"ShiftBlockSize "<<shiftDataBlockSize<<" block end "<<slot->end+sizeDiff+shiftDataBlockSize
					<<"Page start"<<pageData<<" record end "<<slot->end<<endl;
			memmove((char*)pageData + slot->end + sizeDiff, (char*)pageData + slot->end, shiftDataBlockSize);
			shiftSlotInfo(pageData, sizeDiff,rid.slotNum);
			dirInfo->freeSpaceOffset += sizeDiff;
			slot->end += sizeDiff;
			memcpy((char *)pageData + slot->begin, newRecordData, newRecordSize);
			(*freeSpace)[rid.pageNum] -=sizeDiff;
			result = fileHandle.writePage( rid.pageNum, pageData );
		}
		else{// not enough space, set tombstone, add point to new record
			RID newRid;
			cout<<"no enough"<<endl;
			result = insertRecord(fileHandle, recordDescriptor, data, newRid);
			fileHandle.readPage(rid.pageNum,pageData);
			setRecordTombStone((char*)pageData+slot->begin, newRid.pageNum, newRid.slotNum);
			result = fileHandle.writePage( rid.pageNum, pageData );
		}
	}
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
	void* pageData = malloc(PAGE_SIZE);

	fileHandle.readPage(pageN, pageData);
	char* endOfPage = (char *)pageData + PAGE_SIZE;
	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);

	short slotNum = dirInfo->numOfSlots;

	for( unsigned iter1=1; iter1<= slotNum; iter1++){
		Slot * slot = goToSlot(endOfPage, iter1);
		if(slot->begin<0)
		{
			short trueStart = 0 - slot->begin-1;
			short recordSize = slot->end - trueStart;// as the slot begin is already -0
			short moveBlockSize = dirInfo->freeSpaceOffset - slot->end;
			memmove((char *)pageData + trueStart, (char *)pageData + slot->end, moveBlockSize);
			shiftSlotInfo(pageData, 0 - recordSize, iter1);
			dirInfo->freeSpaceOffset += recordSize;
		}
		if(checkTombStone(pageData, pageN, iter1)){
			char* endOfPage = (char*) pageData + PAGE_SIZE;
			Slot* slot = goToSlot((char*)pageData+PAGE_SIZE, iter1);
			int tombStoneSize = 2*sizeof(unsigned)+sizeof(short);
			DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);
			short dataBlockBehindSize = dirInfo->freeSpaceOffset - slot->end;
			short recordSize = slot->end - slot->begin;
			short shiftOffset = recordSize - tombStoneSize;
			memmove((char*) pageData + slot->begin + tombStoneSize, (char*) pageData + slot->end,dataBlockBehindSize);
			dirInfo->freeSpaceOffset -= (recordSize - tombStoneSize);
			shiftSlotInfo(pageData, 0-shiftOffset, iter1);
			slot->end = slot->begin + tombStoneSize;
			vector<short> *freeSpace = directoryOfSlots[fileHandle.getFileName()];
			(*freeSpace)[pageN] +=shiftOffset;
		}
	}
	fileHandle.writePage(pageN, pageData);
	free(pageData);

	return 0;
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

	this->condition = value;

	if( condition == NULL )
	cout << " RBFM_ScanIterator::initialize:" << "condition is NULL"  << endl;
	totalPageNum = fileHandle.getNumberOfPages();

	pageData = (char*) malloc( PAGE_SIZE );
	endOfPage = pageData + PAGE_SIZE;

	fileHandle.readPage( pageNum, pageData );

	cout << "RBFM_ScanIterator::initialize" << endl;

	for (unsigned i = 0, j = 0; i < recordDescriptor.size() && j < attributeNames.size(); i++) {
			Attribute attr = recordDescriptor[i];

			cout << "conditionAttribute=" << conditionAttribute <<
					" attr.name=" << attr.name <<
					" attributeNames[j]=" << attributeNames[j] << endl;

	 		if (attr.name.compare(conditionAttribute) == 0) {
	 			conditionAttrType = attr.type;
	 			conditionAttrNum = (short)i;
	 			cout << "conditionAttrNum=" << conditionAttrNum << endl;
	 		}

			if (attr.name.compare(attributeNames[j]) == 0) {
	 			j++;
	 			constructAttrNum.push_back((short)i);
	 			constructAttrType.push_back(attr.type);
	 		}
	 }

	cout << "conditionAttrNum=" << conditionAttrNum << endl;

	dirInfo = _rbfm->goToDirectoryOfSlotsInfo(endOfPage); // EXTREMLLY not safe

	cout << "RBFM_ScanIterator::dirInfo" << endl;

	totalSlotNum = dirInfo->numOfSlots;

	return 0;
}

bool RBFM_ScanIterator::checkConditionForAttribute(void* attribute, const void* condition, AttrType attrType, CompOp compOp){
	bool result = true;

	if( condition == NULL )
	{
		cout << "checkConditionForAttribute: condition is NULL" << endl;
		return result;
	}

	cout << "checkConditionForAttribute: attrType" << endl;

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

bool RBFM_ScanIterator::checkCondition(void* data, string &attrName, vector<Attribute> &recordDescriptor){
	bool result = false;
	AttrType attrType;
	int flag = -1;
	short fieldOffset = 0;
	int varLen = 0;
	// Find the right type and position of compare field
	for(vector<Attribute>::iterator iter1 = recordDescriptor.begin(); result&&iter1!=recordDescriptor.end();iter1++){
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
	cout << "RBFM_ScanIterator::getNextRecord 1" << endl;

	bool result = false;
	void *attribute = malloc(PAGE_SIZE);
	char *record;
	int attrLength = 0;
	Slot* slot;

	cout << "RBFM_ScanIterator::getNextRecord 2" << endl;
	do {
		slotNum++;

		cout << "RBFM_ScanIterator::getNextRecord totalSlotNum=" << totalSlotNum << endl;
		if( slotNum > totalSlotNum )
		{
			slotNum = 1;
			pageNum++;

			if( pageNum >= fileHandle.getNumberOfPages() )
				return RBFM_EOF;

			fileHandle.readPage( pageNum, pageData );
		}

		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

		slot = _rbfm->goToSlot(endOfPage, slotNum);
		if( slot->begin < 0 )
			continue;

		record = pageData + slot->begin;

		cout << "slot->begin" << slot->begin << endl;

		cout << "*(short*)record=" << *(short*)record << endl;

		if( *(short*)record == -1 )
		{
			RID newRID;
			newRID.pageNum = *(unsigned *)(record + sizeof(short));
			newRID.slotNum = *(unsigned *)(record + sizeof(short) + sizeof(unsigned));

			tombstoneMap[newRID.pageNum * 1000 + newRID.slotNum] = newRID.pageNum * 1000 + slotNum;
			continue;
		}

		if( tombstoneMap.find(rid.pageNum * 1000 + slotNum) != tombstoneMap.end() ) {
			unsigned newRID = tombstoneMap[rid.pageNum * 1000 + slotNum];
			tombstoneMap.erase(rid.pageNum * 1000 + slotNum);
			rid.pageNum = newRID / 1000;
			rid.slotNum = newRID % 1000;
		}

		readAttributeForScan( record, attribute, conditionAttrNum, conditionAttrType, attrLength );
//		result = checkCondition( attribute, conditionAttrName, recordDescriptor );
		result = checkConditionForAttribute( attribute, condition, conditionAttrType, compOp );

		cout << "checkConditionForAttribute result=" << result << endl;
	} while ( !result );

	free( attribute );

	cout << "attribute freed" << endl;

	cout << "getNextRecord result=" << result << endl;

	result = constructAttributeForScan( record, data, constructAttrType, constructAttrNum );
	return result;

/*
	RC result= -1;
	RC tombStoneChk = -1;

	while( pageNum < totalPageNum && result ){
		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

		// read the record out
		slot = _rbfm->goToSlot(endOfPage, slotNum);
		data = malloc(slot->end - slot->begin);
		tombStoneChk = _rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
		inrecreaseIteratorPos();
		if(tombStoneChk == -1)
			continue;
		// see the condition
		if(checkCondition(data, conditionAttrName, recordDescriptor)){
			result = 0;
		}
	}
	return result;
	*/
}

RC RBFM_ScanIterator::readAttributeForScan(char *record, void *attribute, short numOfAttribute, AttrType type, int &attrLength)
{
	short begin = *(short*)(record + sizeof(short) * (numOfAttribute + 1));
	short end = *(short*)(record + sizeof(short) * (numOfAttribute + 2));

	cout << "end= " << end << " begin=" << begin << " (end-begin)="<< (end-begin) << endl;

	attrLength = (int) (end - begin);
	cout << "numOfAttribute= " << numOfAttribute << " attrLength=" << attrLength << endl;

	cout << "type= " << type << endl;

	if( type == TypeInt )
	{
		memcpy( attribute, record + begin, sizeof(int));
	}
	else if ( type == TypeReal )
	{
		memcpy( attribute, record + begin, sizeof(float));
	}
	else if( type == TypeVarChar )
	{
		int offset = 0;

		memcpy( attribute, &attrLength, sizeof(int) );
		offset += sizeof(int);
		memcpy( (char*)attribute + offset, record + begin, attrLength );

		cout << (char*)attribute << endl;

		cout << "attribute=";
		for(int j = 0; j < attrLength; j++)
		{
			printf("%c", *((char*)attribute + offset));
			offset++;
		}
		printf("\n");

		cout << "record=";
		for(int j = 0; j < attrLength; j++)
		{
			printf("%c", *((char*)record + begin));
			offset++;
		}
		printf("\n");

		attrLength += sizeof(int);
		cout << "readAttributeForScan attrLength=" << attrLength << endl;
	}

	return 0;

}

RC RBFM_ScanIterator::constructAttributeForScan(char* record, void* data, vector<AttrType> attrType, vector<short> attrNum)
{
	int offset = 0;
	int attrLength = 0;

	void* attribute = malloc(PAGE_SIZE);

	cout << "constructAttributeForScan=" << attrNum.size() << endl;

	for(unsigned i = 0; i < attrNum.size(); i++ )
	{
		readAttributeForScan( record, attribute, attrNum[i], attrType.at(i), attrLength );
		cout << "attrNum[i]=" << attrNum[i] << ",attType[i]=" << attrType[i] <<
				" ,attrLength=" << attrLength << endl;
		memcpy( (char*)data+offset, attribute, attrLength );
		offset += attrLength;
	}

	free( attribute );
	return 0;
}
