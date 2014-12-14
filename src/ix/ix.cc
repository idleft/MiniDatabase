
#include "ix.h"
#include "math.h"

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

	/* [EJSHIN] Bug fix, read file name change */
//	rc1 = _pfm->openFile(&metaFileName[0], ixFileHandle.metaFileHandle);
	rc1 = _pfm->openFile(metaFileName.c_str(), ixFileHandle.metaFileHandle);
//	cout << "openFile: metaFileName[" << metaFileName.c_str() << "]=" << rc1 << endl;
//	rc2 = _pfm->openFile(&idxFilename[0],ixFileHandle.idxFileHandle);
	rc2 = _pfm->openFile(idxFilename.c_str(),ixFileHandle.idxFileHandle);
//	cout << "openFile: idxFilename[" << idxFilename.c_str() << "]=" << rc2 << endl;	if(rc1==0&&rc2==0)
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
	// 11.23 updated
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
	// two problem. reinsert page don't have next page id
	// after first reinsert split not reinsert record
	res = ixfileHandle.metaFileHandle.readPage(0,idxMetaPage);
//	printf("insert %d %d\n",rid.pageNum, rid.slotNum);
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
//	printf("%d %d %d %d \n",splitFlag,pageDirInfo->freeSpaceNum, idxPgId,pageDirInfo->numOfIdx);
	if(pageDirInfo->freeSpaceNum > keyRecordSize){
		// key RID is the input, idxRID is the output
//		cout<<"To primary page"<<endl;
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
			// means overflow page added
//			cout<<" To new ";
			pageDirInfo->nextPageId = overflowPgId;
			res = ixfileHandle.idxFileHandle.writePage(idxPgId, pageData); // if next page id change, rewrite all data
			idxMetaHeader->overFlowPgNum +=1;
			if(addOverflowFlag == 2){
				idxMetaHeader->physicalOverflowPgNum +=1;
			}
		}
//		cout<<"Overflow page"<<endl;
		res = insertIdxToPage(ixfileHandle.metaFileHandle, attribute, key, rid, idxRID, overflowPgId, hashKey);
		if(addOverflowFlag&&splitFlag){
			// split page
			// idea: read all record in primary & overflow page, insert it consecutively use insert entry
//			cout<<"SPlit!!"<<endl;
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
					if(idxRecordHeader->idxRecordLength<0){
						curInpageOffset += fabs(idxRecordHeader->idxRecordLength);
						continue;
					}
					else{
						idxCnt++;
						keyLength = getKeySize(idxRecordHeader);
						void *inPageKey = malloc(keyLength);
						memcpy(inPageKey, (char*)pageData + curInpageOffset + SIZE_OF_IDX_HEADER, keyLength);
						recRID.pageNum = idxRecordHeader->recordPageId;
						recRID.slotNum = idxRecordHeader->recordSlotId;
//						printf("re insert %d %d\n",recRID.pageNum, recRID.slotNum);
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
//	debug(ixfileHandle);
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

void IndexManager::debugSlotChain(IXFileHandle ixfileHandle, unsigned pgId, unsigned slotId){

	void *pageData = malloc(PAGE_SIZE);
	IdxRecordHeader *idxHeader;
	IdxSlot *idxSlot = goToIdxSlot(pageData, slotId);
	DirectoryOfIdxInfo *idxDirInfo = goToDirectoryOfIdx(pageData);
	ixfileHandle.idxFileHandle.readPage(pgId, pageData);
	int curOffset = idxSlot->nextIdxOffset;
	while(curOffset!=-1){
		idxHeader = (IdxRecordHeader*)((char*)pageData + curOffset);
		printf("%d %d --> ", idxHeader->recordPageId, idxHeader->recordSlotId);
		curOffset = idxHeader->nextOffset;
		if(curOffset==-1&&idxDirInfo->nextPageId!=0){
			putchar('|');
			ixfileHandle.metaFileHandle.readPage(idxDirInfo->nextPageId, pageData);
			curOffset = idxSlot->nextIdxOffset;
		}
	}
	putchar('\n');
	return;
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
	metaPageData =malloc(PAGE_SIZE);
	idxMetaHeader = (IdxMetaHeader*)metaPageData;
	ixfileHandle.metaFileHandle.readPage(0, metaPageData);

	hashKey = hash(attribute, key);
	idxPgId = getIdxPgId(hashKey, idxMetaHeader);
	slotId = getSlotId(hashKey);
	preOffset = -1; // the offset of last idx, -1 for slot
	overFlowCount = 0;
	prePgId = 0; // 11.20 00:32 work here. The pre page id can be primary or overflow, need distinguish
	curPgId = idxPgId;
	// ### DEBUG INFO
//	if(rid.pageNum == 28663)
//		printIndexEntriesInAPage(ixfileHandle, attribute, idxPgId);
//	if(slotId == 45 && curPgId ==68){
//		debugSlotChain(ixfileHandle, curPgId, slotId);
//	}
	// ### DEBUG info
	// load page data
	pageData = malloc(PAGE_SIZE);
	ixfileHandle.idxFileHandle.readPage(idxPgId, pageData);
	idxDirInfo = goToDirectoryOfIdx(pageData);
	idxSlot = goToIdxSlot(pageData, slotId);
	curOffset = idxSlot->nextIdxOffset;
	while( (curPgId > 0 || (curPgId == 0 && overFlowCount==0)) && searchFlag == false){
//		cout<<"out loop "<<curPgId<<endl;
		if(idxDirInfo->numOfIdx == 0){
			overFlowCount+=1;
			prePgId = curPgId;
			curPgId = idxDirInfo->nextPageId;
			ixfileHandle.metaFileHandle.readPage(curPgId, pageData);
		}
		else{
			curOffset = idxSlot->nextIdxOffset;
			while(curOffset!=-1 && searchFlag == false){
//				cout<<"inloop"<<endl;
				idxRecordHeader = (IdxRecordHeader*)((char*) pageData + curOffset);
				if(idxRecordHeader->recordPageId == rid.pageNum && idxRecordHeader->recordSlotId == rid.slotNum){
					unsigned keySize = getKeySize(idxRecordHeader);
					void *cmpKey = malloc(keySize);
					memcpy(cmpKey, (char*) pageData + curOffset + SIZE_OF_IDX_HEADER, keySize);
					if(checkEqualKey(attribute, key, cmpKey)){
						searchFlag = true;
					}
					free(cmpKey);
				}
				if(searchFlag == false){
					preOffset = curOffset;
					curOffset = idxRecordHeader->nextOffset;
				}
			}
			if(curOffset == -1 && searchFlag == false){
				prePgId = curPgId;
				curPgId = idxDirInfo->nextPageId;
				preOffset = -1;
				ixfileHandle.metaFileHandle.readPage(idxDirInfo->nextPageId, pageData);
				overFlowCount+=1;
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
			if(overFlowCount ==0){
				//## logic here is not correct, for primary page record # == zero then do nothing
				// ## if only last primary and no overflow then merge
				// delete primary page
				// first need to see if there is over flow page
				emptyPage(ixfileHandle.idxFileHandle, curPgId);
				if(idxDirInfo->nextPageId!=0){
					unsigned nextPgId = idxDirInfo->nextPageId;
					ixfileHandle.idxFileHandle.readPage(curPgId, pageData);
					idxDirInfo->nextPageId = nextPgId;
					ixfileHandle.idxFileHandle.writePage(curPgId, pageData);
				}
				else if(idxMetaHeader->primaryPgNum > idxMetaHeader->N){
				// if not simply remove page and change the meta data
//					emptyPage(ixfileHandle.idxFileHandle, curPgId);
					if(idxMetaHeader->next == 0){
						idxMetaHeader->next = ceil(idxMetaHeader->primaryPgNum*1.0/2)-1;
						idxMetaHeader->level -=1;
					}
					else
						idxMetaHeader->next-=1;
					idxMetaHeader->primaryPgNum-=1;
				}
			}
			else if (overFlowCount == 1){
				// delete first overflow page
				// concatenate primary page with other overflow page
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
	// Joyce Shin
	AttrType attrType = attribute.type;

		std::hash<unsigned int> hash_uint;
		std::hash<float> hash_float;
		std::hash<std::string> hash_str;

		std::size_t hash;

		switch( attrType ) {
			case TypeInt:
			{
//				unsigned intKey = *(unsigned*)key;
//				cout << "intKey = " << intKey << '\n';
	//			cout << "static_cast<unsigned int>(key)=" << static_cast<unsigned int>(*key) << endl;
				hash = hash_uint(static_cast<unsigned int>(*(int*)key));
				break;
			}
			case TypeReal:
			{
//				float floatKey = *(float *)key;
//				printf("%3f\n", floatKey );
//				cout << "floatKey = " << floatKey << '\n';
				hash = hash_float(static_cast<float>(*(float *)key));
				break;
			}
			case TypeVarChar:
			{
				std::string strKey((char *)key);
//				cout << "strKey = " << strKey << '\n';
				hash = hash_str(strKey);
				break;
			}
			default:
				hash = 0;
				break;
		}

//		cout << "hash:" << hash <<endl;
		return (unsigned)hash;

	return 0;
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
	// Xikui 11.24 reconstruct
	RC result = -1;

	unsigned overflowPageRecordNum = getOverFlowPageRecordNumber(ixfileHandle, primaryPageNumber);
	unsigned nextPgId = 0;
	DirectoryOfIdxInfo *idxDirInfo;
	void *pageData;
	pageData = malloc(PAGE_SIZE);
	idxDirInfo = goToDirectoryOfIdx(pageData);
	ixfileHandle.idxFileHandle.readPage(primaryPageNumber, pageData);

	cout << "Number of total entries in the page (+ overflow pages) : " <<  idxDirInfo->numOfIdx + overflowPageRecordNum << endl;
	cout << "primary Page No." << primaryPageNumber << endl;

	short numOfIndices = idxDirInfo->numOfIdx;
	cout << "# of entries : " << idxDirInfo->numOfIdx << endl;

	if( idxDirInfo->numOfIdx == 0 )
	{
		cout << "numOfIndices is 0" << endl;
	}
	else {
		cout << "entries:";
		nextPgId = printGoOverPage(pageData, attribute);
		cout << endl;
	}

	bool firstOverflowFlag = false;
	while(nextPgId !=0){
		ixfileHandle.metaFileHandle.readPage(nextPgId, pageData);
		cout << "overflow Page No." << nextPgId;
		if(firstOverflowFlag == true)
			cout<<" linked to primary page" << endl;
		else
			cout<<" linked to overflow page" << endl;
		cout << "# of entries : " << idxDirInfo->numOfIdx << endl;
		cout << "entries:";
		nextPgId = printGoOverPage(pageData, attribute);
		cout<<endl;
	}
	free(pageData);
	return 0;
}

RC IndexManager::printGoOverPage( void *pageData, Attribute attribute){
	// Xikui 11.24.2014
	DirectoryOfIdxInfo *idxDirInfo;
	unsigned inpageOffset = 0;
	unsigned recordCnt = 0;
	idxDirInfo = goToDirectoryOfIdx(pageData);
	IdxRecordHeader *idxHeader;
	while(recordCnt < idxDirInfo->numOfIdx){
		idxHeader = (IdxRecordHeader *)((char*)pageData + inpageOffset);
		if(idxHeader->idxRecordLength<0){
			inpageOffset += (0-idxHeader->idxRecordLength);
			continue;
		}
		else{
			recordCnt+=1;
			if(attribute.type == TypeInt)
				printf(" [%d/%d,%d]",*(int*)((char*)pageData + inpageOffset + sizeof(IdxRecordHeader)),
						idxHeader->recordPageId, idxHeader->recordSlotId);
			else if(attribute.type == TypeInt)
				printf(" [%f/%d,%d]",*(float*)((char*)pageData + inpageOffset + sizeof(IdxRecordHeader)),
						idxHeader->recordPageId, idxHeader->recordSlotId);
			else
				printf(" [%s/%d,%d]",((char*)pageData + inpageOffset + sizeof(IdxRecordHeader)),
										idxHeader->recordPageId, idxHeader->recordSlotId);
			inpageOffset+=idxHeader->idxRecordLength;
//			printf("off: %d ", idxHeader->nextOffset);
		}
	}
	return idxDirInfo->nextPageId;
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
//	dirInfo = _ixm->goToDirectoryOfIdx(pageData);	// [EUNJEONG.SHIN] REDUNDANT CODE
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
//	bool overflowFlag = false;

//	cout << "[EJSHIN FOR DEBUG] curBucketId=" << curBucketId << " , totalBucketNum=" << totalBucketNum << endl;
	while(res == -1 && curBucketId < totalBucketNum){

//		cout << "[EJSHIN FOR DEBUG] curBucketId< totalBucketNum" << endl;

		while(curRecId<=dirInfo->numOfIdx && res == -1){
//			cout << "[EJSHIN FOR DEBUG] curRecId=" << curRecId << " , dirInfo->numOfIdx=" << dirInfo->numOfIdx << endl;
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
				if(checkValueSpan(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, key)){
					res = 0;
//					printf("get record at bucket %d  overflow %d\n",curBucketId, overflowFlag);
				}
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
			if(dirInfo->nextPageId == 0){
				curBucketId+=1;
				if(curBucketId < totalBucketNum)
					ixfileHandle.idxFileHandle.readPage(curBucketId, pageData);
			}
			else{
//				overflowFlag = true;
				ixfileHandle.metaFileHandle.readPage(dirInfo->nextPageId, pageData);
			}
		}
	}
//	printf("%d %d -- %u\n",rid.pageNum, rid.slotNum, *((unsigned*)key));
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
	if(rc == -2)
		cerr<<"Error! Trying to delete a non-existed record!"<<endl;
}
