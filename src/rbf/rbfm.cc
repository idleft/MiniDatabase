
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
	_pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
	map<string,vector<short>*>::iterator it;

	for (it=directoryOfSlots.begin(); it!=directoryOfSlots.end(); ++it)
		delete it->second;

	_pfm = NULL;
	_rbf_manager = NULL;
}
void printVector(vector<short> *vec){
	cout<<"****** Begin of print vector"<<endl;
	for(unsigned iter1 = 0; iter1<vec->size(); iter1++)
		cout<<vec->at(iter1)<<endl;
	cout<<"****** End of print vector"<<endl;
}

RC RecordBasedFileManager::debug(FileHandle fileHandle){
	printVector(directoryOfSlots[fileHandle.getFileName()]);
	return 0;
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
	int result = _pfm->createFile( &fileName[0] );

	if( result == 0 )
	{
		result = _pfm->createFile(  (fileName+"_desc").c_str() );
	}

	FileHandle descFileHandle;

	if( result == 0 )
	{
		result = _pfm->openFile( (fileName+"_desc").c_str(), descFileHandle );
	}


	if( result == 0 )
	{
		char * page = (char*)malloc(PAGE_SIZE);
		*(short*)page = 0;

		result = descFileHandle.appendPage( page );
		if( result == 0 )
			_pfm->closeFile ( descFileHandle );

		free( page );
	}

	// create file succeeded.
	return result;

}

int RecordBasedFileManager::keyCompare(void *key1, void *key2, Attribute attr){
	int res = -3;
	if(attr.type == TypeInt){
		if(*(int*)key1 == *(int*)key2)
			res = 0;
		else if(*(int*)key1 > *(int*)key2)
			res = 1;
		else
			res = -1;
	}
	else if (attr.type == TypeReal){
		if( *(float*)key1 == *(float*)key2)
			res =0;
		else if(*(float*)key1 > *(float*)key2)
			res = 1;
		else
			res = -1;
	}
	else{
		// int cmp;
		int len1, len2;
		len1 = *(int*)key1;
		len2 = *(int*)key2;
		if(len1<len2){
			res = memcmp((char*)key1+sizeof(int),(char*)key2+sizeof(int),len1);
			//printf("length1 compare --- %d ",res);
			if(res == 0)
				res = -1;
		}
		else if (len1>len2){
			res = memcmp((char*)key1+sizeof(int),(char*)key2+sizeof(int),len2);
				//		printf("length2 compare --- %d ",res);

			if(res == 0)
				res = 1;
		}
		else
			res = memcmp(key1,key2,len1+sizeof(int));
	}
	return res;
}

bool RecordBasedFileManager::getMatchCompareRes(CompOp op, int cmpRes){
	bool res = false;
	if(cmpRes==0 &&( op == EQ_OP || op == GE_OP || op == LE_OP))
		res = true;
	else if((cmpRes < 0||cmpRes ==0) && (op == LE_OP))
		res = true;
	else if((cmpRes >0 || cmpRes ==0) && (op == GE_OP))
		res = true;
	else if(cmpRes<0 && op == LT_OP)
		res = true;
	else if (cmpRes>0 && op==GT_OP)
		res = true;
	else if (op == NE_OP && cmpRes !=0)
		res = true;
	return res;
}

RC RecordBasedFileManager::selectAttribute(vector<Attribute> attrList, string attrName, Attribute &attr){
	int rc = -1;
	for(unsigned iter1 = 0; iter1 < attrList.size()&&rc==-1; iter1++)
		if(attrList.at(iter1).name == attrName){
			attr = attrList.at(iter1);
			rc = 0;
		}
	return rc;
}

RC RecordBasedFileManager::selectAttributes( vector<Attribute> attrList, vector<string> attrNames, 
		vector<Attribute> &selectedAttrs){
	for(string attrName : attrNames){
		for(Attribute attr :attrList){
			if(attr.name == attrName){
				selectedAttrs.push_back(attr);
				break;
			}
		}
	}
	return 0;
}


RC RecordBasedFileManager::destroyFile(const string &fileName) {
	RC result = _pfm->destroyFile( fileName.c_str() );

	if( result == 0 )
		result = _pfm->destroyFile( (fileName+"_desc").c_str() );

	if( result == 0 )
		directoryOfSlots.erase(fileName);

	return result;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	RC result = -1;

//	result = _pfm->openFile( &fileName[0], fileHandle );
	result = _pfm->openFile( fileName.c_str(), fileHandle );	// [EUNJEONG.SHIN] Bug fix while opening file with fileName
	if( result == 0 ) {
		if( directoryOfSlots.find(fileName) == directoryOfSlots.end() )	// fileName does not exist in directory of slots
		{
			// read description
			FileHandle descFileHandle;
			result =  _pfm->openFile( (fileName + "_desc").c_str() , descFileHandle );
			if( result != 0 )
				return -1;
			vector<short>* freeSpace = new vector<short>();
			for (unsigned pageNo = 0; pageNo < descFileHandle.getNumberOfPages(); pageNo++)
			{
				result = readHeader( descFileHandle, pageNo, freeSpace );
				if( result != 0 )
					return -1;
			}
			result = _pfm->closeFile( descFileHandle );
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
			(numOfPages < HEADER_PAGE_SIZE) ){
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
	if( directoryOfSlots.find( fileHandle.getFileName() ) == directoryOfSlots.end() )
		return result;

	vector<short>* freeSpace = directoryOfSlots[fileHandle.getFileName()];
	int numOfPages = (int)freeSpace->size();
	unsigned numOfHeaderPages = numOfPages / HEADER_PAGE_SIZE;
	if( numOfPages % HEADER_PAGE_SIZE != 0 )
		numOfHeaderPages++;

	FileHandle headerFileHandle;
	const char* desc = "_desc";

	string fileName = fileHandle.getFileName();
	string descFileName = string(fileName) + string(desc);

	_pfm->openFile( descFileName.c_str(), headerFileHandle );

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

	result = _pfm->closeFile( headerFileHandle );
	if( result != 0 )
		return result;

	result = _pfm->closeFile( fileHandle );

	return result;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	RC result = -1;
	PageNum pageNum = 0;

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
	memset(page, 0, PAGE_SIZE);

	// find first blank slot
	for(; pageNum < slotDirectory->size(); pageNum++ )
	{
		if( (sizeOfRecord +  sizeof(Slot)) <= (unsigned)(*slotDirectory)[pageNum] )
		{
			// add record to the page
			fileHandle.readPage( pageNum, page );
			const char* endOfPage = page + PAGE_SIZE;
			DirectoryOfSlotsInfo *info = getDirectoryOfSlotsInfo(endOfPage);
			unsigned slotNum = info->numOfSlots + 1;
			appendRecord(page, record, sizeOfRecord, slotNum);
			(*slotDirectory)[pageNum] -= (sizeOfRecord + sizeof(Slot));
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
	string filename = fileHandle.getFileName();
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
//	info->freeSpaceNum-=(sizeOfRecord + sizeof(Slot));
//	info->recordNumber++;

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
//	info->freeSpaceNum = PAGE_SIZE - sizeOfRecord - sizeof(directoryOfSlotsInfo) - sizeof(Slot)*2;
//	info->recordNumber = 1;

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

		cout<<attr.name;

		if( attr.type == TypeInt )
		{
			printf(" %d\n", *((int*)((char*)data + offset)));
			offset += attr.length;
		}
		else if( attr.type == TypeReal )
		{
			printf(" %.4f\n", *((float*)((char*)data + offset)));
			offset += attr.length;
		}
		else if( attr.type == TypeVarChar )
		{
			int varLength = *((int*)((char*)data + offset));
			offset += sizeof(int);
			printf(" %d ", varLength);
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

short RecordBasedFileManager::getSizeOfData(const vector<Attribute> &recordDescriptor, const void* data){
	// 12.7 xkwang measure the size of data before insert
	short length = 0;
	short offset = 0;
	int varLength = 0;
	for (unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		// cout<<"length "<<length<<endl;
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
			varLength = *(int*)((char*)data+offset);
			length += (varLength+sizeof(int));
			offset += (varLength + sizeof(int));
		}
	}
	return length;
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

	result = _pfm->closeFile( fileHandle );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: closeFile:" << result << endl;

	result = _pfm->destroyFile( cfileName );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: destroyFile:" << result << endl;

	result = _pfm->createFile( cfileName );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: createFile:" << result << endl;

	result = _pfm->openFile( cfileName, fileHandle );
	if( result != 0 )
		return result;

//	cout << "deleteRecords: openFile:" << result << endl;
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	RC result = -1;
//	DirectoryOfSlotsInfo *dirInfo;
	if( fileHandle.getFile() == NULL )
			return result;
	if( directoryOfSlots.find( fileHandle.getFileName() ) ==
			directoryOfSlots.end() )
		return result;

	unsigned pageNum = rid.pageNum;
	unsigned slotNum = rid.slotNum;
	char* page = (char*)malloc( PAGE_SIZE );

	bool isTombStone = true;
	while( isTombStone )
	{
		result = fileHandle.readPage( pageNum, page );
		if( result != 0 )
			return result;

		const char* endOfPage = page + PAGE_SIZE;

		Slot* slot = goToSlot( endOfPage, slotNum );
		if( slot->begin < 0 ) // delete a deleted record
		{
			result = -1;
//			result = 0;
			break;
		}

		char* data = page + slot->begin;
//		printf("Delete : %d %d\n", pageNum, slotNum);
		unsigned newPageNum,newSlotNum;
		newPageNum = pageNum;
		newSlotNum = slotNum;
		isTombStone = isRecordTombStone( data, newPageNum, newSlotNum );
		slot->begin = -1 - slot->begin;
		result = fileHandle.writePage( pageNum, page );
		if( result != 0 )
			break;
		pageNum = newPageNum;
		slotNum = newSlotNum;
	}

	free(page);
	return result;
}

//RC RecordBasedFileManager::shiftSlotInfo(void* pageData, short shiftOffset, short slotNum){
//	// shift all slot behind with offset, not include
//	char* endOfPage = (char *) pageData + PAGE_SIZE;
//	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);
//	Slot* slot;
//	short slotN = dirInfo->numOfSlots;
//	for(int iter1 = slotNum+1; iter1<=slotN; iter1++){
//		slot = goToSlot(endOfPage,iter1);
//		slot->begin+=shiftOffset;
//		slot->end+=shiftOffset;
//	}
//	return 0;
//}

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
	// calculate cfreespace = PAGE_SIZE - dirInfo.freeOffset - (dirinfo.slotnum+2)*sizeof(int)

	Slot* slot = goToSlot(endOfPage,rid.slotNum);
//	DirectoryOfSlotsInfo* dirInfo = goToDirectoryOfSlotsInfo(endOfPage);

//	cout<<2.3<<endl;
	short oldRecordSize = slot->end - slot->begin;
	short newRecordSize = getSizeOfRecord(recordDescriptor, data);
	void *newRecordData = malloc(newRecordSize);
	short sizeDiff = newRecordSize - oldRecordSize;
	dataToRecord(data, recordDescriptor, newRecordData);
	if(oldRecordSize==newRecordSize){
		memcpy((char*) pageData + slot->begin, newRecordData, oldRecordSize);
		result = fileHandle.writePage( rid.pageNum, pageData );
	}
	else{
		if (sizeDiff > (*freeSpace)[rid.pageNum])
		{
//			printf("request reorganize");
			reorganizePage(fileHandle, recordDescriptor, rid.pageNum);
		}
		if(newRecordSize < oldRecordSize){
			memcpy((char *)pageData + slot->begin, newRecordData, newRecordSize);
			slot->end = slot->begin+newRecordSize;
		}
		else{// not enough space, set tombstone, add point to new record
			RID newRid;
			DirectoryOfSlotsInfo *dirInfo;
//			cout<<"not enough space"<<endl;
			result = insertRecord(fileHandle, recordDescriptor, data, newRid);
			fileHandle.readPage(rid.pageNum,pageData);
			setRecordTombStone((char*)pageData+slot->begin, newRid.pageNum, newRid.slotNum);
			dirInfo = goToDirectoryOfSlotsInfo((char *)pageData + PAGE_SIZE);
//			dirInfo->recordNumber --;
		}

		result = fileHandle.writePage( rid.pageNum, pageData );
//		cout<<freeSpace->at(rid.pageNum)<<" "<<dirInfo->freeSpaceNum<<endl;
	}
	free(newRecordData);
	free(pageData);
	return result;
}

int RecordBasedFileManager::getEstimatedRecordDataSize(vector<Attribute> rescordDescriptor){
	int res = 0;
	for(unsigned iter1 = 0; iter1<rescordDescriptor.size(); iter1++)
	{
		if(rescordDescriptor.at(iter1).type==TypeVarChar)
			res+=sizeof(int);
		res+=rescordDescriptor.at(iter1).length;
	}
	return res;
}

RC RecordBasedFileManager::getAttrFromData(const vector<Attribute> &recordDescriptor, void* recordData, void* data, const string attributeName, short &attrSize){
	int fieldPointer = 0;
	RC result = -1;
	for(unsigned iter1 = 0; iter1<recordDescriptor.size()&&result == -1; iter1++){
		if(recordDescriptor.at(iter1).name == attributeName){
			result = 0;
			if(recordDescriptor.at(iter1).type == TypeInt){
				memcpy(data, (char *)recordData+fieldPointer, sizeof(int));
				attrSize = sizeof(int);
			}
			else if(recordDescriptor.at(iter1).type == TypeReal){
				memcpy(data, (char *)recordData+fieldPointer, sizeof(float));
				attrSize = sizeof(float);
			}
			else{
				int varLen = *(int *)((char*)recordData + fieldPointer);
				memcpy(data,&varLen, sizeof(int));
				memcpy((char*)data+sizeof(int), (char *)recordData+fieldPointer+sizeof(int), varLen);
				attrSize = sizeof(int) + varLen;
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
	return result;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
{
	RC result = -1;
	short attrSize;
	int estimateRecordLen = getEstimatedRecordDataSize(recordDescriptor);
	void *recordData = malloc(estimateRecordLen);
	readRecord(fileHandle, recordDescriptor,rid,recordData);
	result = getAttrFromData(recordDescriptor, recordData, data, attributeName, attrSize);
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

	for( short iter1 = 0; iter1 < slotNum; iter1++ ) {

		// data is there, let's move them
		if( slot->begin < 0 )
		{
			reOrgSlot->begin = slot->begin;
			reOrgSlot->end = slot->end;
		}
		else{
			short recordSize = slot->end - slot->begin;
			memcpy( (char*)reorganizedPage + offset, (char*)page + slot->begin, recordSize);
			reOrgSlot->begin = offset;
			reOrgSlot->end = offset + recordSize;
			offset += recordSize;
		}
		slot--;
		reOrgSlot--;
	}
	reOrgPagedirInfo->numOfSlots = dirInfo->numOfSlots;
	reOrgPagedirInfo->freeSpaceOffset = offset;
//	reOrgPagedirInfo->freeSpaceNum = PAGE_SIZE - offset - sizeof(DirectoryOfSlotsInfo) - reOrgPagedirInfo->numOfSlots*sizeof(Slot);

	result = fileHandle.writePage(pageN, reorganizedPage);

	vector<short> *freeSpace = directoryOfSlots[fileHandle.getFileName()];
	(*freeSpace)[pageN] = PAGE_SIZE - offset - sizeof(DirectoryOfSlotsInfo) - reOrgPagedirInfo->numOfSlots*sizeof(Slot);

	free(page);
	free(reorganizedPage);
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
	_rbfm = RecordBasedFileManager::instance();
	compOp = NO_OP;
	totalPageNum = 0;
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

	this->fileHandle = fileHandle;
	this->recordDescriptor = recordDescriptor;
	this->conditionAttrName = conditionAttribute;
	this->targetPointer = value;
	this->compOp = compOp;
	this->attributeNames = attributeNames;

	this->condition = value;

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

bool RBFM_ScanIterator::checkCondition(void* data, string &attrName, vector<Attribute> &recordDescriptor){
	if(targetPointer == NULL)
		return true;
	bool result = false;
	AttrType attrType;
	int flag = -1;
	short fieldOffset = 0;
	int varLen = 0;
	// Find the right type and position of compare field
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
		/*
		char* fieldValue;
		char* targetValue = (char *) targetPointer;
		varLen = *(int*)((char *)data+fieldOffset);
		fieldValue = (char *)malloc(varLen);
		memcpy(fieldValue, (char *)data+fieldOffset+sizeof(int), varLen);
		int strCmpRes = strcmp(fieldValue, targetValue);
		*/
		char* targetValue = (char *) targetPointer;
		// [EUNJEONG.SHIN] Bug fix while comparing string field value
		int fieldLeng = *(int *)((char *)data+fieldOffset);
		string field((char *)data + fieldOffset + sizeof(int), fieldLeng);
		int targetLeng = *(int *)targetValue;
		string target((char *)targetValue + sizeof(int), targetLeng);

		int strCmpRes = strcmp(field.c_str(), target.c_str());

		switch(compOp){
			case EQ_OP :  result = (strCmpRes == 0); break;
			case LT_OP :  result = (strCmpRes < 0 ); break;    // <
			case GT_OP :  result = (strCmpRes > 0); break;   // >
			case LE_OP :  result = (strCmpRes <= 0); break;   // <=
			case GE_OP :  result = (strCmpRes >= 0); break;   // >=
			case NE_OP :  result = (strCmpRes != 0); break;   // !=
			case NO_OP :  result = true; break;
		}
//		free(fieldValue);

	}
	return result;
}

RC  RBFM_ScanIterator::inrecreaseIteratorPos(){
	slotNum += 1;
	if((short)slotNum>dirInfo->numOfSlots){
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
	totalPageNum = this->fileHandle.getNumberOfPages();

	while( (pageNum < totalPageNum) && result ){		// [EUNJEONG.SHIN] Add parenthesis to make it clear


		if( (short)slotNum > dirInfo->numOfSlots )
		{
			slotNum = 1;
			pageNum++;
			if( pageNum >= fileHandle.getNumberOfPages() )
				break;
			fileHandle.readPage( pageNum, this->pageData );
			endOfPage = this->pageData + PAGE_SIZE;
		}

		rid.pageNum = pageNum;
		rid.slotNum = slotNum;

		slot = _rbfm->goToSlot(endOfPage, slotNum);
//		inrecreaseIteratorPos(); // let line1050 handle the scope
		slotNum++;

		if(slot->begin<0)
			continue;
		short estimateRecordLen = _rbfm->getEstimatedRecordDataSize(recordDescriptor);
		void* recordData = malloc(estimateRecordLen);
		tombStoneChk = _rbfm->readRecord(fileHandle, recordDescriptor, rid, recordData);
//		_rbfm->printRecord(recordDescriptor,recordData);
		if(tombStoneChk == -1)
		{
			free(recordData);
			continue;
		}
		if(checkCondition(recordData, conditionAttrName, recordDescriptor)){
			result = 0;
			int attrSetSize = 0;
			short attrSize = 0;
			void *attrBuffer = malloc(1000);//assume single feature maximum size 1000 ##DANGER##
			for(unsigned iter1 = 0; iter1<this->attributeNames.size(); iter1++)
			{
				string attrName = this->attributeNames.at(iter1);
				this->_rbfm->getAttrFromData(recordDescriptor, recordData, attrBuffer, attrName,attrSize);
//				cout<<"***getNextRecord*** Reading attribute "<<attrName<<" Length: "<<attrSize<<endl;
				memcpy((char*)data + attrSetSize, attrBuffer, attrSize);
				attrSetSize += attrSize;
			}
			free(attrBuffer);
		}
		free(recordData);
	}
	return result;
}

RC RBFM_ScanIterator::getAttrSizeByName(string attrName, vector<Attribute> attrSet){
	for(unsigned iter1 = 0; iter1<attrSet.size();iter1++){
		Attribute attr = attrSet.at(iter1);
		if(attr.name == attrName)
			return attr.length;
	}
	return -1;
}
