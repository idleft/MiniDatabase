
#include "ix.h"
#include "math.h"
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <functional>
#include <string>
#include <sstream>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	SIZE_OF_IDX_HEADER = sizeof(IdxRecordHeader);
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	//Xikui 11.15.2014
	RC rc1,rc2;
	string metaFileName, idxFileName;
	void *data;
	FILE *file;
	FileHandle idxFileHandle;
	metaFileName = fileName + METASUFFIX;
	idxFileName = fileName + BUCKETSUFFIX;
	rc1 = _pfm->createFile(&metaFileName[0]);
	rc2 = _pfm->createFile(&idxFileName[0]);

	if(rc1!=0||rc2!=0)
		return -1;

	// Prepare data
	_pfm->openFile(&idxFileName[0], idxFileHandle);
	for(int iter1 = 0; iter1<numberOfPages; iter1++)
		appendEmptyPage(idxFileHandle);
	_pfm->closeFile(idxFileHandle);

	// Deal with the first page of meta data
	data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	IdxMetaHeader *metaHeader = (IdxMetaHeader *)data;
	metaHeader->N = numberOfPages;
	metaHeader->level = 0;
	metaHeader->next = 0;
	metaHeader->primaryPgNum = numberOfPages;
	metaHeader->physicalPrimaryPgNum = numberOfPages;
	metaHeader->overFlowPgNum = 0;
	metaHeader->physicalOverflowPgNum = 0;
	file = fopen(&metaFileName[0],"rb+");
	fwrite(data, 1, PAGE_SIZE, file);
	fclose(file);
	free(data);

	return 0;
}

DirectoryOfIdxInfo* IndexManager::goToDirectoryOfIdx(void *pageData){
	char* res = (char*)pageData + PAGE_SIZE - sizeof(DirectoryOfIdxInfo);
	return (DirectoryOfIdxInfo*) res;
}

RC IndexManager::appendEmptyPage(FileHandle &fileHandle){

	RC res = -1;
	void *pageData;

	DirectoryOfIdxInfo *dirInfo;
	IdxSlot *idxSlot;
	pageData = malloc(PAGE_SIZE);
	memset(pageData, 0, PAGE_SIZE);

	dirInfo = goToDirectoryOfIdx(pageData);
	dirInfo->freeSpaceNum = PAGE_SIZE - sizeof(DirectoryOfIdxInfo) - MAX_INDEX_PAGE_SLOT_NUM*sizeof(IdxSlot);
	dirInfo->numOfIdx = 0;
	for(int iter1 = 0; iter1<MAX_INDEX_PAGE_SLOT_NUM; iter1++){
		idxSlot = goToIdxSlot(pageData, iter1+1);
		idxSlot->nextIdxOffset = -1;
	}

	res = fileHandle.appendPage(pageData);
	free(pageData);
	return res;
}

RC IndexManager::destroyFile(const string &fileName)
{
	// Xikui 11.15.2014
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
	// update 11/19/2014 decouple from rbfm Xikui
	RC rc1,rc2;
	string metaFileName, idxFilename;
	metaFileName = fileName + METASUFFIX;
	idxFilename = fileName + BUCKETSUFFIX;

	rc1 = _pfm->openFile(&metaFileName[0], ixFileHandle.metaFileHandle);
	rc2 = _pfm->openFile(&idxFilename[0],ixFileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	// update 11/19/2014 decouple from rbfm Xikui
	RC rc1,rc2;
	rc1 = _pfm->closeFile(ixfileHandle.metaFileHandle);
	rc2 = _pfm->closeFile(ixfileHandle.idxFileHandle);
	if(rc1==0&&rc2==0)
		return 0;
	else
		return -1;
}

unsigned IndexManager:: getIdxPgId(unsigned bucketId, IdxMetaHeader* idxMetaHeader)
{
	// Xikui 11.15.2014
	unsigned res = 0;
	unsigned currentListLength = idxMetaHeader->N*pow(2,idxMetaHeader->level);
	res = bucketId%currentListLength;
	if(res<idxMetaHeader->next){
		//idxMetaHeader->level++;
		currentListLength = idxMetaHeader->N*pow(2,idxMetaHeader->level+1);
		res = bucketId%currentListLength;
	}
	return res;
}

int IndexManager:: getKeyRecordSize(const Attribute &attr, const void *key)
{
	// Xikui 11/15/2014
	int res = 0;
	if(attr.type == TypeVarChar){
		int varLen = *((int *)key);
		res = sizeof(int) + varLen;
	}
	else if(attr.type == TypeInt)
		res = sizeof(int);
	else
		res = sizeof(float);
	// plus the rid.pageNum & rid.slotNum nextrid, idxlength
	res = res+ sizeof(IdxRecordHeader);
	return res;
}

RC keyToRecord(const void* key, const short recordLength, void* record, RID keyRID, short nextIdxOffset)
{
	IdxRecordHeader* idxSlot;
	idxSlot = (IdxRecordHeader*)record;
	idxSlot->idxRecordLength = recordLength;
	idxSlot->nextOffset = nextIdxOffset;
	idxSlot->recordPageId = keyRID.pageNum;
	idxSlot->recordSlotId = keyRID.slotNum;

	memcpy((char*)record+sizeof(IdxRecordHeader), key, recordLength - sizeof(IdxRecordHeader));

	return 0;
}

unsigned IndexManager::getSlotId(unsigned hashKey){
	return (hashKey%MAX_INDEX_PAGE_SLOT_NUM)+1;
}

IdxSlot* IndexManager::goToIdxSlot(void* pageData, unsigned slotId){
	// xikui 11.19
	// assume slot id start from 1
	char* idxSlot = (char*) pageData + PAGE_SIZE - sizeof(DirectoryOfIdxInfo) - slotId*sizeof(IdxSlot);
	return (IdxSlot*) idxSlot;
}

RC IndexManager::insertIdxToPage(FileHandle &fileHandle, const Attribute &keyAttribute,
		const void *key, const RID &keyRID, RID &idxRID, unsigned pageId, unsigned hashKey){
	// 11/19/2014 Xikui
	RC res = -1;
	unsigned idxRecordSize = -1, slotId;
	IdxSlot *idxSlot;
	DirectoryOfIdxInfo *idxDirInfo;
	void *pageData;
	if(fileHandle.getFile()== NULL)
		return res;

	// first load pageData
	pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(pageId, pageData);
	idxDirInfo = goToDirectoryOfIdx(pageData);

	slotId = getSlotId(hashKey);
	idxSlot = goToIdxSlot(pageData, slotId);

	idxRecordSize = getKeyRecordSize(keyAttribute, key);
	void *record = malloc( idxRecordSize );
	keyToRecord( key, idxRecordSize, record, keyRID, idxSlot->nextIdxOffset); // dont forget to initialize to -1

	// copy data to right place
	memcpy((char*) pageData + idxDirInfo->freeSpaceOffset, record, idxRecordSize);
	idxSlot->nextIdxOffset = idxDirInfo->freeSpaceOffset;
	// update page meta
	idxDirInfo->freeSpaceNum -= idxRecordSize;
	idxDirInfo->freeSpaceOffset+= idxRecordSize;
	idxDirInfo->numOfIdx++;

	res = fileHandle.writePage(pageId, pageData);

	free(record);
	free(pageData);
	return res;
}

RC IndexManager::flagInsertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid, bool splitFlag){
	// Xikui 11/16/2014
	RC res = -1;
	unsigned hashKey,idxPgId;
	void *idxMetaPage,*pageData;
	int addOverflowFlag = 0;
	IdxMetaHeader *idxMetaHeader;
	DirectoryOfIdxInfo *pageDirInfo;
	int keyRecordSize;
	RID idxRID;
	// Get the index meta data
	idxMetaPage = malloc(PAGE_SIZE);
	pageData = malloc(PAGE_SIZE);

	res = ixfileHandle.metaFileHandle.readPage(0,idxMetaPage);

	if(res<0)
		return -1;

	idxMetaHeader = (IdxMetaHeader*)idxMetaPage;
	keyRecordSize = getKeyRecordSize(attribute, key);

	// generate key record data
	hashKey = hash(attribute, key);

	idxPgId = getIdxPgId(hashKey, idxMetaHeader); // primary page id

	// get the page id see if over flow page needed
	ixfileHandle.idxFileHandle.readPage(idxPgId, pageData);
	pageDirInfo = goToDirectoryOfIdx(pageData);

	if(pageDirInfo->freeSpaceNum > keyRecordSize){
		// key RID is the input, idxRID is the output
		res = insertIdxToPage(ixfileHandle.idxFileHandle, attribute, key, rid, idxRID, idxPgId, hashKey);
	}
	else{
		// find a suitable over flow page
		// go over overflowgageN find a page with 0 record
		// remember to increase the existedOverFlowPageN
		unsigned overflowPgId = 0;
		overflowPgId = getOverflowPageId(ixfileHandle, pageDirInfo->nextPageId, keyRecordSize, addOverflowFlag);
		//pageDirInfo->nextPageId = overflowPgId;
		if(pageDirInfo->nextPageId != overflowPgId){
			pageDirInfo->nextPageId = overflowPgId;
			res = ixfileHandle.idxFileHandle.writePage(idxPgId, pageData); // if next page id change, rewrite all data
			idxMetaHeader->overFlowPgNum +=1;
			if(addOverflowFlag == 2){
				idxMetaHeader->physicalOverflowPgNum +=1;
			}
		}
		res = insertIdxToPage(ixfileHandle.metaFileHandle, attribute, key, rid, idxRID, overflowPgId, hashKey);
		if(addOverflowFlag&&splitFlag){
			// idea: read all record in primary & overflow page, insert it consecutively use insert entry
			unsigned curPgId, oriOverflowPgId;
			bool overflowFlag = false;
			unsigned splitBuckId = idxMetaHeader->next;
			DirectoryOfIdxInfo *idxDirInfo;
			// store the ori primary page in page data
			res = ixfileHandle.idxFileHandle.readPage(splitBuckId, pageData);
			idxDirInfo = goToDirectoryOfIdx(pageData);
			oriOverflowPgId = idxDirInfo->nextPageId;
			// empty primary page
			emptyPage(ixfileHandle.idxFileHandle, splitBuckId);
			// update meta info
			idxMetaHeader->next++;
			if(idxMetaHeader->next>=pow(2,idxMetaHeader->level)*idxMetaHeader->N){
				// if next larger than current level number
				idxMetaHeader->next = 0;
				idxMetaHeader->level+=1;
			}
			// append new primary page
			unsigned nextPgId = idxMetaHeader->next+pow(2,idxMetaHeader->level)*idxMetaHeader->N-1;
			if( nextPgId >= idxMetaHeader->physicalPrimaryPgNum){
				appendEmptyPage(ixfileHandle.idxFileHandle);
				idxMetaHeader->physicalPrimaryPgNum+=1;
				idxMetaHeader->primaryPgNum+=1;
			}
			// write meta to page
			res = ixfileHandle.metaFileHandle.writePage(0, idxMetaPage);
			curPgId = splitBuckId;
			while(overflowFlag == false || curPgId!=0){
				unsigned idxCnt = 0;
				unsigned curInpageOffset = 0;
				unsigned keyLength;
				RID recRID;
				IdxRecordHeader *idxRecordHeader;
				while(idxCnt<idxDirInfo->numOfIdx){
					idxRecordHeader = (IdxRecordHeader *)((char*) pageData + curInpageOffset);
					if(idxRecordHeader->idxRecordLength<0)
						continue;
					else{
						idxCnt++;
						keyLength = getKeySize(idxRecordHeader);
						void *inPageKey = malloc(keyLength);
						memcpy(inPageKey, (char*)pageData + curInpageOffset + SIZE_OF_IDX_HEADER, keyLength);
						recRID.pageNum = idxRecordHeader->recordPageId;
						recRID.slotNum = idxRecordHeader->recordSlotId;
						flagInsertEntry(ixfileHandle, attribute, inPageKey, recRID, false);
					}
					curInpageOffset += fabs(idxRecordHeader->idxRecordLength);
				}
				curPgId = idxDirInfo->nextPageId;
				overflowFlag = true;
				res = ixfileHandle.metaFileHandle.readPage(curPgId, pageData);
			}
			// after have to clean all ori overflow page
			unsigned curOverflowPgId;
			curOverflowPgId = oriOverflowPgId;
			ixfileHandle.metaFileHandle.readPage(0,idxMetaPage);
			while(curOverflowPgId!=0){
				unsigned nextOverflowPgId;
				res = ixfileHandle.metaFileHandle.readPage(curOverflowPgId, pageData);
				nextOverflowPgId = idxDirInfo->nextPageId;
				idxMetaHeader->overFlowPgNum -=1;
				emptyPage(ixfileHandle.metaFileHandle, curOverflowPgId);
				curOverflowPgId = nextOverflowPgId;
			}
			ixfileHandle.metaFileHandle.writePage(0,idxMetaPage);
		}
		else if(addOverflowFlag) // if not split then just update meta since overflow page changed
			ixfileHandle.metaFileHandle.writePage(0, idxMetaPage);
	}

	free(idxMetaPage);
	free(pageData);
	return res;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	return flagInsertEntry(ixfileHandle, attribute, key, rid, true);
}

unsigned IndexManager::getOverflowPageId(IXFileHandle ixfileHandle, unsigned nextPageId, int keyRecordSize, int &addOverflowFlag){
	// Xikui 11/18/2014
	// return page num, -1 means all overflowpage are fulled
	// 11.20 fix go over all pages
	unsigned overflowPageId = 0; // may have problem when insert to 0
	// see if current overflow page chain fit
	// trick: connect the page reversely, then only have to consider the
	// first page

	unsigned curPageId = nextPageId;
	if(curPageId != 0){
		// read the first overflow page to see if there is enough space
		DirectoryOfIdxInfo *dirInfo;
		void *pageData;
		pageData = malloc(PAGE_SIZE);
		dirInfo = goToDirectoryOfIdx(pageData);
		ixfileHandle.metaFileHandle.readPage(curPageId, pageData);
		if(dirInfo->freeSpaceNum>keyRecordSize)
			overflowPageId = curPageId;
//		curPageId = dirInfo->nextPageId;
		free(pageData);
	}
//	putchar('\n');
	if(overflowPageId == 0){
		// start to find one empty page
		bool emptyPageFoundFlag = false;
		void *metaIdxPageData = malloc(PAGE_SIZE);
		void *overflowPageData = malloc(PAGE_SIZE);
		DirectoryOfIdxInfo *idxDirInfo = goToDirectoryOfIdx(overflowPageData);
		ixfileHandle.metaFileHandle.readPage(0, metaIdxPageData);
		IdxMetaHeader *idxMetaHeader = (IdxMetaHeader *)metaIdxPageData;
		unsigned curOverFlowPgId = 1;
		while(curOverFlowPgId<= idxMetaHeader->overFlowPgNum && !emptyPageFoundFlag){
			ixfileHandle.metaFileHandle.readPage(curOverFlowPgId, overflowPageData);
			if(idxDirInfo->numOfIdx == 0){
				emptyPageFoundFlag = true;
			}
			else
				curOverFlowPgId+=1;
		}
		if(emptyPageFoundFlag == true){
			addOverflowFlag = 1; // 1 for reuse
//			cout<<"reuse old page 111111"<<endl;
			idxDirInfo->nextPageId = nextPageId;
			ixfileHandle.metaFileHandle.writePage(curOverFlowPgId, overflowPageData);
			overflowPageId = curOverFlowPgId;
		}
		else{
			// all flow page are full
			addOverflowFlag = 2; // 2 for new page
//			cout<<"new physical page 2222"<<endl;
			appendEmptyPage(ixfileHandle.metaFileHandle); // append empty page does change meta
//			ixfileHandle.metaFileHandle.readPage(0, metaIdxPageData);
			idxMetaHeader->overFlowPgNum+=1;
			void *newPageData = malloc(PAGE_SIZE);
			DirectoryOfIdxInfo *newPageDirInfo;
			ixfileHandle.metaFileHandle.readPage(idxMetaHeader->overFlowPgNum,newPageData);// newest page
			newPageDirInfo = goToDirectoryOfIdx(newPageData);
			newPageDirInfo->nextPageId = nextPageId; // link this page into the overflow chain
			ixfileHandle.metaFileHandle.writePage(idxMetaHeader->overFlowPgNum, newPageData);
			ixfileHandle.metaFileHandle.writePage(0,metaIdxPageData);
			overflowPageId = idxMetaHeader->overFlowPgNum;
			free(newPageData);
		}
		free(metaIdxPageData);
		free(overflowPageData);
	}
	return overflowPageId;
}

unsigned IndexManager::getKeySize(IdxRecordHeader *idxRecordHeader){
	return idxRecordHeader->idxRecordLength - sizeof(IdxRecordHeader);
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	// xikui 11/19/2014
	// merge only when last primary page is empty, one trick is
	// when last primary page is empty, but its overflow page is not
	// copy the last overflow page of it to the primary page.
	unsigned hashKey,idxPgId, slotId,prePgId;
	void *metaPageData, *pageData;
	short curOffset,preOffset,curPgId;
	short overFlowCount;
	IdxMetaHeader *idxMetaHeader;
	IdxRecordHeader *idxRecordHeader;
	DirectoryOfIdxInfo *idxDirInfo;
	IdxSlot *idxSlot;
	bool searchFlag = false;
	RC res;

	// load meta data
	metaPageData = malloc(PAGE_SIZE);
	idxMetaHeader = (IdxMetaHeader*)metaPageData;

	ixfileHandle.metaFileHandle.readPage(0, metaPageData);

	hashKey = hash(attribute, key);
	idxPgId = getIdxPgId(hashKey, idxMetaHeader);
	slotId = getSlotId(hashKey);
	preOffset = -1; // the offset of last idx, -1 for slot
	overFlowCount = 0;
	prePgId = 0; // 11.20 00:32 work here. The pre page id can be primary or overflow, need distinguish
	curPgId = idxPgId;
	// load page data
	pageData = malloc(PAGE_SIZE);
	ixfileHandle.idxFileHandle.readPage(idxPgId, pageData);
	idxDirInfo = goToDirectoryOfIdx(pageData);
	idxSlot = goToIdxSlot(pageData, slotId);
	curOffset = idxSlot->nextIdxOffset;
	while(curOffset != -1 && searchFlag == false){
		idxRecordHeader = (IdxRecordHeader*)((char*) pageData + curOffset);
		if(idxRecordHeader->recordPageId == rid.pageNum && idxRecordHeader->recordSlotId == rid.slotNum){
			unsigned keySize = getKeySize(idxRecordHeader);
			void *cmpKey = malloc(keySize);
			memcpy(cmpKey, (char*) pageData + curOffset + SIZE_OF_IDX_HEADER, keySize);
			if(checkEqualKey(attribute, key, cmpKey)){
				searchFlag = true;
			}
			else{
				preOffset = curOffset;
				curOffset = idxRecordHeader->nextOffset;
			}
		}
		if(curOffset == -1 && searchFlag == false){ // as long as not find in current page, it must in overflow page
			overFlowCount++;
			if(idxDirInfo->nextPageId!=0){
				prePgId = curPgId;
				curPgId = idxDirInfo->nextPageId;
				ixfileHandle.metaFileHandle.readPage(idxDirInfo->nextPageId, pageData);
				curOffset = idxSlot->nextIdxOffset;
			}
		}
	}

	if(searchFlag == false)
		res = -2;
	else{
		idxRecordHeader->idxRecordLength = 0 - idxRecordHeader->idxRecordLength; //mark delete
		if(preOffset == -1) // if it is the first record pointed by slot
			idxSlot->nextIdxOffset = idxRecordHeader->nextOffset;
		else{ // if it is an in page record
			IdxRecordHeader *preRecordHeader = (IdxRecordHeader*) ((char*)pageData + preOffset);
			preRecordHeader->nextOffset = idxRecordHeader->nextOffset;
		}
		idxDirInfo->numOfIdx --;// decrease idx number
		if(idxDirInfo->numOfIdx > 0){ // still have enough records to be deleted
			if(overFlowCount == 0)
				res = ixfileHandle.idxFileHandle.writePage(curPgId, pageData);
			else
				res = ixfileHandle.metaFileHandle.writePage(curPgId, pageData);
		}
		else {// idxDirInfo->numOfIdx == 0 merge pages
			if(overFlowCount ==0 ){
				// delete primary page
				// first need to see if there is over flow page
				if(idxDirInfo->nextPageId!=0){
					// have overflow page shift forward
					void *overflowPageData = malloc(PAGE_SIZE);
					unsigned overflowPgId = idxDirInfo->nextPageId;
					ixfileHandle.metaFileHandle.readPage(idxDirInfo->nextPageId, overflowPageData);
					memcpy(pageData, overflowPageData, PAGE_SIZE);
					emptyPage(ixfileHandle.metaFileHandle, overflowPgId);
					ixfileHandle.idxFileHandle.writePage(curPgId, pageData);
				}
				else{
				// if not simply remove page and change the meta data
					emptyPage(ixfileHandle.idxFileHandle, curPgId);
					if(idxMetaHeader->next == 0 && idxMetaHeader->level >0){
						idxMetaHeader->next = ceil(idxMetaHeader->primaryPgNum*1.0/2) -1;
						idxMetaHeader->level -=1;
					}
					else
						idxMetaHeader->next-=1;
					idxMetaHeader->primaryPgNum-=1;
				}
			}
			else if (overFlowCount == 1){
				// delete first overflow page
				removePage(ixfileHandle.idxFileHandle, idxDirInfo, prePgId);
				emptyPage(ixfileHandle.metaFileHandle, curPgId);
				idxMetaHeader->overFlowPgNum-=1;
			}
			else{
				// delete other overflow page
				removePage(ixfileHandle.metaFileHandle, idxDirInfo, prePgId);
				emptyPage(ixfileHandle.metaFileHandle, curPgId);
				idxMetaHeader->overFlowPgNum-=1;
			}
			res = ixfileHandle.metaFileHandle.writePage(0,metaPageData);
		}
	}
	free(metaPageData);
	free(pageData);
	return res;
}

RC IndexManager::emptyPage(FileHandle pageFileHandle, unsigned pgId){
	// Xikui 11.20 empty certain pg, no mater primary or overflow
	void *pageData = malloc(PAGE_SIZE);
	memset(pageData, 0, PAGE_SIZE);
	//pageFileHandle.readPage(pgId, pageData);
	DirectoryOfIdxInfo *idxDirInfo;
	IdxSlot *idxSlot;
	idxDirInfo = goToDirectoryOfIdx(pageData);
	idxDirInfo->freeSpaceNum = PAGE_SIZE - sizeof(DirectoryOfIdxInfo) - MAX_INDEX_PAGE_SLOT_NUM*sizeof(IdxSlot);
	idxDirInfo->freeSpaceOffset = 0;
	idxDirInfo->nextPageId = 0;
	idxDirInfo->numOfIdx = 0;
	for(int iter1 = 0; iter1<MAX_INDEX_PAGE_SLOT_NUM; iter1++){
			idxSlot = goToIdxSlot(pageData, iter1+1);
			idxSlot->nextIdxOffset = -1;
	}
	pageFileHandle.writePage(pgId, pageData);
	free(pageData);
	return 0;
}

RC IndexManager::removePage(FileHandle prePageFileHandle, DirectoryOfIdxInfo *curDirInfo, unsigned prePgId){
	// Xikui 11.20 remove page by given pre page id and cur page id
	// only consider the next pg id in pre page
	void *prePageData = malloc(PAGE_SIZE);
	prePageFileHandle.readPage(prePgId, prePageData);
	DirectoryOfIdxInfo *prePageDirInfo;
	prePageDirInfo = goToDirectoryOfIdx(prePageData);
	prePageDirInfo->nextPageId = curDirInfo->nextPageId;
	prePageFileHandle.writePage(prePgId, prePageData);
	free(prePageData);
	return 0;
}


bool IndexManager::checkEqualKey(Attribute attr, const void *key, void *cmpKey){
	bool res = false;

	if(attr.type == TypeReal){
		if(*((float*)key) == *((float*)cmpKey))
			res = true;
	}
	else if (attr.type == TypeInt){
		if(*((int*)key) == *((int*)cmpKey))
			res = true;
	}
	else{
		short varLen1 = *((short*)key);
		short varLen2 = *((short*)cmpKey);
		if(varLen1 == varLen2){
			if(memcmp(key, cmpKey, varLen1+sizeof(short)))
				res = true;
		}
	}
	return res;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key)
{
	AttrType attrType = attribute.type;

	std::hash<unsigned int> hash_uint;
	std::hash<float> hash_float;
	std::hash<std::string> hash_str;

	std::size_t hash;

	void* key_data;

	switch( attrType ) {
		case TypeInt:
		{
			size_t size = sizeof(int);
//			cout << "size = " << size << '\n';
			key_data = malloc(size);
			memcpy(key_data, key, size);
			int intKey = *(int *) key_data;

//			cout << "intKey = " << intKey << '\n';
			hash = hash_uint(static_cast<unsigned int>(intKey));
			free(key_data);
			break;
		}
		case TypeReal:
		{
			size_t size = sizeof(float);
//			cout << "size = " << size << '\n';
			key_data = malloc(size);
			memcpy(key_data, key, size);

			float floatKey = *(float *)key_data;
//			cout << "floatKey = " << floatKey << '\n';
			hash = hash_float(floatKey);
			free(key_data);
			break;
		}
		case TypeVarChar:
		{
			std::string strKey((char *)key);
//			cout << "strKey = " << strKey << '\n';
			hash = hash_str(strKey);
			break;
		}
		default:
			hash = 0;
			break;
	}

//	cout << "hash:" << hash <<endl;
	return hash;

}


/*	Xikui 11/18/2014
 * save for more efficient version
OverflowPageInfo* IndexManager::goToOverflowPageInfo(void *metaPageData, unsigned overflowPageId){
	char * overflowPageInfo = (char *)metaPageData + sizeof(IdxMetaHeader) + sizeof(OverflowPageInfo)(overflowPageId);
	return (OverflowPageInfo *)overflowPageInfo;
}*/

unsigned IndexManager::getOverFlowPageRecordNumber(IXFileHandle ixFileHandle, unsigned overflowPageId){
	// Xikui 11/18/2014
	// get all record in all overflow page
	unsigned overflowRecordNum = 0;
	unsigned curPgId = overflowPageId;
	if(curPgId == 0 )
		overflowRecordNum = 0;
	else{
		DirectoryOfIdxInfo *dirInfo;
		void *metaPageData = malloc(PAGE_SIZE);
		dirInfo = goToDirectoryOfIdx(metaPageData);
		while(curPgId!=0){
			ixFileHandle.metaFileHandle.readPage(curPgId, metaPageData);
			overflowRecordNum += dirInfo->numOfIdx;
			curPgId = dirInfo->nextPageId;
		}
		free(metaPageData);
	}
	return overflowRecordNum;
}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) 
{
	RC result = -1;

	IdxMetaHeader *idxMetaHeader;

	IX_ScanIterator ix_ScanIterator;

	void *pageData;
	pageData = malloc(PAGE_SIZE);

	result = ixfileHandle.idxFileHandle.readPage( primaryPageNumber, pageData );
	if( result != 0 )
		return result;

	DirectoryOfIdxInfo *idxDirInfo;
	idxDirInfo = goToDirectoryOfIdx( pageData );

	idxMetaHeader = (IdxMetaHeader*)malloc(PAGE_SIZE);
	result = ixfileHandle.metaFileHandle.readPage( primaryPageNumber, idxMetaHeader );

	DirectoryOfIdxInfo *idxMetaDirInfo;
	idxMetaDirInfo = goToDirectoryOfIdx( idxMetaHeader );

	cout << "Number of total entries in the page (+ overflow pages) : " <<  idxDirInfo->numOfIdx + idxMetaHeader->overFlowPgNum << endl;
	cout << "primary Page No." << primaryPageNumber << endl;

	short numOfIndices = idxDirInfo->numOfIdx;
	cout << "# of entries : " << numOfIndices << endl;

	if( numOfIndices == 0 )
	{
		cout << "numOfIndices is 0" << endl;
	}
	else {
		cout << "entries:";

		result = scan( ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator );
		if( result != 0 )
		{
			 closeFile( ixfileHandle );
			 return result;
		}

		RID rid;
		char key[PAGE_SIZE]={0,};

		while( ix_ScanIterator.getNextEntry( rid, &key )  == 0 )
		{
			cout << "[";

			if( attribute.type == TypeInt )
			{
				cout << (int)*key;
			}
			else if( attribute.type == TypeReal )
			{
				cout << (float)*key;
			}
			else if( attribute.type == TypeVarChar )
			{
				int varLength = *((int*)((char*)key));

				for(int j = 0; j < varLength; j++)
				{
					printf("%c", (char*)key);
				}
			}

			cout << "/" << rid.pageNum << "," << rid.slotNum << "] ";
		}
		cout << endl;
	}

	cout << "overflow Page No." << idxMetaHeader->overFlowPgNum << " linked to [primary | overflow] page" << endl;
	unsigned overflowRecordNum = getOverFlowPageRecordNumber( ixfileHandle, idxMetaHeader->overFlowPgNum );
	cout << "# of entries : " << overflowRecordNum << endl;

	IX_ScanIterator ix_oScanIterator;
	if( overflowRecordNum != 0 )
	{
		cout << "entries:";

		result = scan( ixfileHandle, attribute, NULL, NULL, true, true, ix_oScanIterator );
		if( result != 0 )
		{
			 closeFile( ixfileHandle );
			 return result;
		}

		RID rid;
		char key[PAGE_SIZE];

		while( ix_oScanIterator.getNextEntry( rid, &key )  == 0 )
		{
			cout << "[";

			if( attribute.type == TypeInt )
			{
				cout << (int)*key;
			}
			else if( attribute.type == TypeReal )
			{
				cout << (float)*key;
			}
			else if( attribute.type == TypeVarChar )
			{
				int varLength = *((int*)((char*)key));

				for(int j = 0; j < varLength; j++)
				{
					printf("%c", (char*)key);
				}
			}

			cout << "/" << rid.pageNum << "," << rid.slotNum << "] ";
		}

		cout << endl;
	}

	free(pageData);
	free(idxMetaHeader);

	cout << "9" << endl;


	return 0;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	// Xikui 11/15/2014
	// 11.20 update to use meta data
	void *metaPageData = malloc(PAGE_SIZE);
	ixfileHandle.metaFileHandle.readPage(0,metaPageData);
	IdxMetaHeader *idxMetaHeader = (IdxMetaHeader*)metaPageData;
	numberOfPrimaryPages = idxMetaHeader->primaryPgNum;
	free(metaPageData);
	return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	// Xikui 11/15/2014
	// 11.20 update to use meta data
	void *metaPageData = malloc(PAGE_SIZE);
	ixfileHandle.metaFileHandle.readPage(0,metaPageData);
	IdxMetaHeader *idxMetaHeader = (IdxMetaHeader *)metaPageData;
	numberOfAllPages = idxMetaHeader->primaryPgNum + idxMetaHeader->overFlowPgNum;
	free(metaPageData);
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
	// Xikui 11/16/2014
	return ix_ScanIterator.initialize(ixfileHandle, attribute, lowKey,
			highKey, lowKeyInclusive, highKeyInclusive);
}

void IndexManager::debug(IXFileHandle ixfileHandle){
	void* pageData = malloc(PAGE_SIZE);
	DirectoryOfIdxInfo *idxDirInfo = goToDirectoryOfIdx(pageData);
	ixfileHandle.idxFileHandle.readPage(0, pageData);
	cout<<"Overflow chain from 0 ";
	unsigned nextPgId=idxDirInfo->nextPageId;
	while(nextPgId != 0){
		ixfileHandle.metaFileHandle.readPage(nextPgId, pageData);
		printf("--> %d ", nextPgId);
		nextPgId = idxDirInfo->nextPageId;
	}
	putchar('\n');
}

IX_ScanIterator::IX_ScanIterator()
{
	_ixm = IndexManager::instance();
	SIZE_OF_IDX_HEADER = sizeof(IdxRecordHeader);
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::initialize(IXFileHandle &ixfileHandle,
	    const Attribute &attribute,
	    const void      *lowKey,
	    const void      *highKey,
	    bool			lowKeyInclusive,
	    bool        	highKeyInclusive){
	// Xikui 11/16/2014
	// 11/19 updated to fit idx page
	this->ixfileHandle = ixfileHandle;
	this->attribute = attribute;
	this->lowKey = lowKey;
	this->highKey = highKey;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;

	curBucketId = 0;
	curRecId = 1;
	curInPageOffset = 0;

	_ixm->getNumberOfPrimaryPages(ixfileHandle, totalBucketNum);

	keyAttri = attribute;
	pageData = malloc(PAGE_SIZE);
	dirInfo = _ixm->goToDirectoryOfIdx(pageData);
	ixfileHandle.idxFileHandle.readPage(0, pageData);
	dirInfo = _ixm->goToDirectoryOfIdx(pageData);
	// not finish yet
	return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	// Xikui 11/17/2014
	// 11/19 updated to fit idx page
	RC res = -1;
	IdxRecordHeader *idxRecordHeader;
	while(res == -1 && curBucketId < totalBucketNum){

		while(curRecId<=dirInfo->numOfIdx && res == -1){
			idxRecordHeader = (IdxRecordHeader*)((char*)pageData + curInPageOffset);
			if(idxRecordHeader->idxRecordLength < 0){
				curInPageOffset += (0-idxRecordHeader->idxRecordLength);
				continue;
			}
			else{
				curRecId++;
//				if(idxRecordHeader->idxRecordLength == 0)
//					printf("all data size %d key data size: %d\n",idxRecordHeader->idxRecordLength, idxRecordHeader->idxRecordLength - sizeof(IdxRecordHeader));
				memcpy(key, (char*)pageData+ curInPageOffset + SIZE_OF_IDX_HEADER, idxRecordHeader->idxRecordLength - sizeof(IdxRecordHeader));
//				printf("get value : %d \n",*(int*)key);
				curInPageOffset += idxRecordHeader->idxRecordLength;
				if(checkValueSpan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, key))
					res = 0;
				else
					continue;
				rid.pageNum = idxRecordHeader->recordPageId;
				rid.slotNum = idxRecordHeader->recordSlotId;
			}
		}
		if(curRecId > dirInfo->numOfIdx){
			// already read all record in current page
			curRecId = 1;
			curInPageOffset = 0;
			if(dirInfo->nextPageId == 0){ // run over overflow page
				curBucketId+=1;
				if(curBucketId < totalBucketNum)
					ixfileHandle.idxFileHandle.readPage(curBucketId, pageData);
			}
			else{
				ixfileHandle.metaFileHandle.readPage(dirInfo->nextPageId, pageData);
			}
		}
	}
	if(res==-1)
		return IX_EOF;
	else
		return 0;
}

RC IX_ScanIterator::getNextEntryForOverflowPage(RID &rid, void *key)
{
	// Xikui 11/17/2014
	// 11/19 updated to fit idx page
	RC res = -1;
	IdxRecordHeader *idxRecordHeader;
	while(res == -1 && curBucketId < totalBucketNum){

		while(curRecId<=dirInfo->numOfIdx && res == -1){
			idxRecordHeader = (IdxRecordHeader*)((char*)pageData + curInPageOffset);
			if(idxRecordHeader->idxRecordLength < 0){
				curInPageOffset += (0-idxRecordHeader->idxRecordLength);
				continue;
			}
			else{
				curRecId++;
				res = 0;
				curInPageOffset += idxRecordHeader->idxRecordLength;
				memcpy(key, (char*)pageData+ curInPageOffset, idxRecordHeader->idxRecordLength - sizeof(IdxRecordHeader));
				if(checkValueSpan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, key))
					res = 0;
				else
					continue;
				rid.pageNum = idxRecordHeader->recordPageId;
				rid.slotNum = idxRecordHeader->recordSlotId;
			}
		}
		if(curRecId > dirInfo->numOfIdx){
			if(dirInfo->nextPageId == 0){
				curBucketId+=1;
//				if(curBucketId < totalBucketNum)
//					ixfileHandle.idxFileHandle.readPage(curBucketId, pageData);
			}
			else{
				ixfileHandle.metaFileHandle.readPage(dirInfo->nextPageId, pageData);
			}
		}
	}
	if(res==-1)
		return IX_EOF;
	else
		return 0;
}

//int IX_ScanIterator::getKeyDataSize(Attribute attr, void *keyRecordData){
//	// Xikui 11/17/2014
//	int recordDataLen = 0;
//	if(attr.type == TypeInt)
//		recordDataLen = sizeof(int);
//	else if(attr.type == TypeReal)
//		recordDataLen = sizeof(float);
//	else{
//		int varLen = *((int *)keyRecordData);
//		recordDataLen = varLen + sizeof(int);
//	}
//	return recordDataLen;
//}

int compareRawDataWithAttr(Attribute attr, void *key, const void*toCompare, int cmpFlag){
	// 11.17 00:06 Xikui work here
	// make sure the comparison between varChar. change scan in rbfm correspondently.
	bool res = false;
	if(attr.type == TypeReal||attr.type == TypeInt){
		if(attr.type == TypeReal){
			float a = *((float *)toCompare);
			float b = *((float*)key);
			if(cmpFlag == -1)
				res = a<b;
			else if (cmpFlag == -2)
				res = a<=b;
			else if (cmpFlag == 1)
				res = a>b;
			else if(cmpFlag == 2)
				res = a>=b;
		}
		else{
			int a = *((int *) toCompare);
			int b = *((int *)key);
			if(cmpFlag == -1)
				res = a<b;
			else if (cmpFlag == -2)
				res = a<=b;
			else if (cmpFlag == 1)
				res = a>b;
			else if(cmpFlag == 2)
				res = a>=b;
		}
	}
	else{
		int tmpRes = 0;
		tmpRes = strcmp((char*)key+sizeof(int),(char*)toCompare+sizeof(int));
		if((cmpFlag == 2|| cmpFlag == -2)&& tmpRes == 0)
			res = true;
		else if((cmpFlag == 1 || cmpFlag ==2) && tmpRes >0)
			res = true;
		else if ((cmpFlag == -1 || cmpFlag ==-2) && tmpRes<0)
			res = true;
		res = false;
	}
	return res;
}

bool IX_ScanIterator::checkValueSpan(Attribute attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive, void *keyRecordData){
	// Xikui 11/17/2014
	bool res = true;
	bool tmpRes = 0;

	if(lowKey!=NULL){
		if(lowKeyInclusive==true)
			tmpRes = compareRawDataWithAttr(attribute, keyRecordData, lowKey, -2);
		else
			tmpRes = compareRawDataWithAttr(attribute, keyRecordData, lowKey, -1);
		res = res&&tmpRes;
	}
	if(highKey!=NULL){

		if(highKeyInclusive==true)
			tmpRes = compareRawDataWithAttr(attribute, keyRecordData, highKey, 2);
		else
			tmpRes = compareRawDataWithAttr(attribute, keyRecordData, highKey, 1);
		res = res&&tmpRes;
	}
	return res;
}

RC IX_ScanIterator::close()
{
	free(pageData);
	curBucketId = 0;
	curRecId = 1;
	return 0;
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
	 * Xikui 11/15/2014
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
