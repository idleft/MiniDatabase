
#include "qe.h"

/// start of filter
Filter::Filter(Iterator* input, const Condition &condition) {
	/* Initialize variables */
	iterator = input;
	iterator->getAttributes(attrList);
	_rbfm->selectAttribute(attrList, condition.lhsAttr, selectAttr);
	this->condition = condition;
}

RC Filter::getNextTuple(void *data) {
	// 12.08 xkwang reconstruct
	RC rc = -1;
	bool qualifyFlag = false;
	int recordLen = _rbfm->getEstimatedRecordDataSize(attrList);
	void *recordData = malloc(recordLen);

	while(qualifyFlag ==false&&iterator->getNextTuple(recordData)!=-1){
		void* keyData = malloc(selectAttr.length);
		short keySize;
		_rbfm->getAttrFromData(attrList, recordData, keyData, condition.lhsAttr, keySize);
		// printf("The compared key: %d ",*(int*)keyData);
		int cmpRes;
		if(condition.bRhsIsAttr == true){
			// compare with attribute
			Attribute cmpAttr;
			short cmpKeySize;
			_rbfm->selectAttribute(attrList, condition.rhsAttr, cmpAttr);
			void* cmpKey = malloc(cmpAttr.length);
			_rbfm->getAttrFromData(attrList, recordData, cmpKey, condition.rhsAttr,cmpKeySize);
			cmpRes = _rbfm->keyCompare(keyData, cmpKey, selectAttr);
			free(cmpKey);
		}
		else
			cmpRes = _rbfm->keyCompare(keyData, condition.rhsValue.data, selectAttr);
		// printf(" cmp res :%d ",cmpRes);
		bool matchCmpRes = _rbfm->getMatchCompareRes(condition.op, cmpRes);
		if(matchCmpRes == true){
			qualifyFlag = true;
			// cout<<" Qulified";
		}
		// putchar('\n');
		free(keyData);
	}
	if(qualifyFlag == true){
		// copy the result
		memcpy(data, recordData, recordLen);
		rc =  0;
	}
	else
		rc = QE_EOF;
	free(recordData);
	return rc;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs = this->attrList;
}

/// end of filter

void moveToValueByAttrType(char* value, AttrType type) {

//	cout << "Filter moveToValueByAttrType type=" << type << endl;

	int length = 0;
	switch( type )
	{
		case TypeInt:
			value += sizeof(int);
			break;
		case TypeReal:
			value += sizeof(float);
			break;
		case TypeVarChar:
			length = *((int *)value);
			value += length + sizeof(int);
			break;
	}

//	cout << "Filter moveToValueByAttrType End=" << type << endl;
}


// Project
Project::Project(Iterator *input,                    // Iterator of input R
        const vector<string> &attrNames){
	// 12.08 xkwang reconstruct
	this->iterator = input;
	this->attrNames = attrNames;
	iterator->getAttributes(attrList);
	_rbfm->selectAttributes(attrList, attrNames, selectedAttrList);
}

RC Project::getNextTuple(void *data) {
	// 12.08 xkwang reconstruct
	RC rc = -1;
	int fullRecordLen = _rbfm->getEstimatedRecordDataSize(attrList);
	void *fullRecord = malloc(fullRecordLen);
	int offSet = 0;

	rc = iterator->getNextTuple( fullRecord );

	if(rc!=-1){
		for(Attribute attr: selectedAttrList){
			void* fieldData = malloc(attr.length);
			short fieldSize;
			_rbfm->getAttrFromData(attrList, fullRecord, fieldData, attr.name, fieldSize);
			memcpy((char*)data + offSet, fieldData, fieldSize);
			offSet+=fieldSize;
			free(fieldData);
		}
		// cout<<"----------------Print selected record--------------"<<endl;
		// _rbfm->printRecord(selectedAttrList, data);
		// cout<<"----------------END selected record--------------"<<endl;
	}
	free(fullRecord);
	return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs = this->selectedAttrList;
}
//end of project

// starts of BNLJoin
// xikui dec 14
BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        )
{
	this->condition = condition;
	this->numRecords = numRecords;


	leftIn->getAttributes(leftAttrList);
	rightIn->getAttributes(rightAttrList);
	leftRecordSize = _rbfm->getEstimatedRecordDataSize(leftAttrList);
	rightRecordSize = _rbfm->getEstimatedRecordDataSize(rightAttrList);

	// endMark = loadBlockRecords();
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	curInblockP = 0;
	curBlockListSize = 0;

	mergeAttrList.clear();
	for(Attribute iter1:leftAttrList){
		mergeAttrList.push_back(iter1);
	}
	for(Attribute iter1:rightAttrList){
		mergeAttrList.push_back(iter1);
	}

	_rbfm->selectAttribute(rightAttrList, condition.rhsAttr, comAttr);

}

RC BNLJoin::loadBlockRecords(){
	emptyBlockList();
	int blockIter = 0;
	curBlockListSize = 0;
	curInblockP = 0;
	void *recordData = malloc(leftRecordSize);
	while(blockIter < numRecords && leftIn->getNextTuple(recordData)!=-1){
		blockRecordList.push_back(recordData);
		recordData = malloc(leftRecordSize);
		blockIter++;
		curBlockListSize++;
	}
	free(recordData);
	if(blockRecordList.size() == 0)
		return -1;
	else
		return 0;
}

void BNLJoin::emptyBlockList(){
	for(unsigned iter1 = 0; iter1<blockRecordList.size(); iter1++){
		void *dataRecord = blockRecordList.at(iter1);
		free(dataRecord);
	}
	blockRecordList.clear();
}

RC BNLJoin::getNextTuple(void *data){
	RC rc = -1;
	void *leftRecord;
	bool foundFlag = false;
	void *rightRecord = malloc(rightRecordSize);
	void *leftKey = malloc(comAttr.length+sizeof(int));
	void *rightKey = malloc(comAttr.length+sizeof(int));

	while(foundFlag == false){
		if(curInblockP == curBlockListSize){
			rc = loadBlockRecords();
			if(rc == -1)
				break;
		}
		leftRecord = blockRecordList.at(curInblockP);
		RC getRightFlag = rightIn->getNextTuple(rightRecord);
		if(getRightFlag == -1){
			rightIn->setIterator();
			curInblockP ++;
		}
		else{
			short leftSize, rightSize;
			_rbfm->getAttrFromData(leftAttrList, leftRecord, leftKey, condition.lhsAttr, leftSize);
			_rbfm->getAttrFromData(rightAttrList, rightRecord, rightKey, condition.rhsAttr, rightSize);
			if(_rbfm->keyCompare(leftKey, rightKey, comAttr)==0){
				// cout<<"Key match : "<<comAttr.name<<endl;
				// cout<<"----------Record 2---------------"<<endl;
				// _rbfm->printRecord(rightAttrList, recordData);
				// cout<<"------------------"<<endl;
				int actLeftRecordSize, actRightRecordSize;
				actLeftRecordSize = _rbfm->getSizeOfData(leftAttrList, leftRecord);
				actRightRecordSize = _rbfm->getSizeOfData(rightAttrList,rightRecord);
				// printf("Diagnostic value: %d %d %u %u %u\n",actLeftRecordSize,actRightRecordSize,leftAttrList.size(),
					// rightAttrList.size(),mergeAttrList.size());
				void *mergeData = malloc(actLeftRecordSize+actRightRecordSize);
				memcpy(mergeData, leftRecord, actLeftRecordSize);
				memcpy((char*)mergeData+actLeftRecordSize, rightRecord, actRightRecordSize);
				foundFlag = true;
				memcpy(data, mergeData, actLeftRecordSize+actRightRecordSize);
				// cout<<"merged Record: "<<endl;
				// _rbfm->printRecord(mergeAttrList, mergeData);
				free(mergeData);
			}
		}
	}
	free(rightRecord);
	if(foundFlag == false)
		return -1;
	else
		return 0;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = this->mergeAttrList;
}

BNLJoin::~BNLJoin(){
	emptyBlockList();
}

// end of BNLJoin

// Index nested-loop join operator
INLJoin::INLJoin(Iterator *leftIn,       // Iterator of input R
        IndexScan *rightIn,          	// IndexScan Iterator of input S
        const Condition &condition   	// Join condition
)
{
	// xikui dec/14/2014 reconstruct
	this->leftIn = leftIn;
	this->rightIn = rightIn;

	this->condition = condition;

	leftIn->getAttributes(leftAttrList);
	rightIn->getAttributes(rightAttrList);

	mergeAttrList.clear();
	for(Attribute iter1:leftAttrList){
		mergeAttrList.push_back(iter1);
	}
	for(Attribute iter1:rightAttrList){
		mergeAttrList.push_back(iter1);
	}

	leftRecordSize = _rbfm->getEstimatedRecordDataSize(leftAttrList);
	rightRecordSize = _rbfm->getEstimatedRecordDataSize(rightAttrList);

	leftRecord = malloc(leftRecordSize);
	rightRecord = malloc(rightRecordSize);

	_rbfm->selectAttribute(rightAttrList, condition.rhsAttr, comAttr);
	leftKey = malloc(comAttr.length+sizeof(int));

	leftRC = leftIn->getNextTuple(leftRecord);
	_rbfm->getAttrFromData(leftAttrList, leftRecord, leftKey, condition.lhsAttr, leftSize);
	rightIn->setIterator(leftKey,leftKey,true,true);
}

RC INLJoin::getNextTuple(void *data){

	bool foundFlag = false;
	void *rightKey = malloc(comAttr.length+sizeof(int));

	while(foundFlag == false){
		if(leftRC == -1)
			break;

		rightRC = rightIn->getNextTuple(rightRecord);
		if(rightRC == -1){
			leftRC = leftIn->getNextTuple(leftRecord);
			_rbfm->getAttrFromData(leftAttrList, leftRecord, leftKey, condition.lhsAttr, leftSize);
			rightIn->setIterator(leftKey,leftKey,true,true);
		}
		else{
			short leftSize, rightSize;
			_rbfm->getAttrFromData(rightAttrList, rightRecord, rightKey, condition.rhsAttr, rightSize);
			if(_rbfm->keyCompare(leftKey, rightKey, comAttr)==0){
				int actLeftRecordSize, actRightRecordSize;
				actLeftRecordSize = _rbfm->getSizeOfData(leftAttrList, leftRecord);
				actRightRecordSize = _rbfm->getSizeOfData(rightAttrList,rightRecord);
				// printf("Diagnostic value: %d %d %u %u %u\n",actLeftRecordSize,actRightRecordSize,leftAttrList.size(),
					// rightAttrList.size(),mergeAttrList.size());
				void *mergeData = malloc(actLeftRecordSize+actRightRecordSize);
				memcpy(mergeData, leftRecord, actLeftRecordSize);
				memcpy((char*)mergeData+actLeftRecordSize, rightRecord, actRightRecordSize);
				foundFlag = true;
				memcpy(data, mergeData, actLeftRecordSize+actRightRecordSize);
				// cout<<"merged Record: "<<endl;
				// _rbfm->printRecord(mergeAttrList, mergeData);
				free(mergeData);
			}
		}
	}

	if(foundFlag == false)
		return -1;
	else
		return 0;

}

void copyValue( void* dest, const void* src, AttrType attrType )
{
//	cout << "copyValue Start type=" << attrType << endl;

	switch( attrType )
	{
		case TypeInt:
			memcpy( dest, src, sizeof(int));
			break;
		case TypeReal:
			memcpy( dest, src, sizeof(float));
			break;
		case TypeVarChar:
			int length = *((int*)src);
			memcpy( dest, src, sizeof(int) + length);
			break;
	}

//	cout << "copyValue End " << endl;
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->mergeAttrList;
};

// Mandatory for graduate teams only
// Basic aggregation
Aggregate:: Aggregate(Iterator *input,          // Iterator of input R
        Attribute aggAttr,        				// The attribute over which we are computing an aggregate
        AggregateOp op            				// Aggregate operation
){

	if( aggAttr.type == TypeVarChar )
	{
		status = false;
		cout << "VarChar Type not supported! " << " Exit with error code= " <<QE_ATTRIBUTE_NOT_SUPPORTED << endl;
	}

	typeOfAggregation =  AGGREGATION_BASIC;

	iterator = input;

	iterator->getAttributes(attributeVector);

	this->aggAttr = aggAttr;

	this->op = op;

	status = true;

};

// Optional for everyone. 5 extra-credit points
// Group-based hash aggregation
Aggregate:: Aggregate(Iterator *input,             // Iterator of input R
        Attribute aggAttr,           				// The attribute over which we are computing an aggregate
        Attribute groupAttr,         				// The attribute over which we are grouping the tuples
        AggregateOp op,              				// Aggregate operation
        const unsigned numPartitions 				// Number of partitions for input (decided by the optimizer)
){

	if( aggAttr.type == TypeVarChar ){
		cout << "VarChar Type not supported! " << " Exit with error code= " <<QE_ATTRIBUTE_NOT_SUPPORTED << endl;
		return;
	}

	typeOfAggregation =  AGGREGATION_GROUP;

	iterator = input;

	iterator->getAttributes(attributeVector);

	this->aggAttr = aggAttr;

	this->groupAttr = groupAttr;

	this->op = op;

	this->numPartitions = numPartitions;

	status = true;

	switch( op ) {
		case MIN:
			calculateMinForGroup();
			break;
		case MAX:
			calculateMaxForGroup();
			break;
		case SUM:
			calculateSumForGroup();
			break;
		case AVG:
			calculateAvgForGroup();
			break;
		case COUNT:
			calculateCountForGroup();
			break;
		default:
			break;
	}

};

RC Aggregate::getNextTuple(void *data){
	if( status == false )
		return QE_EOF;

	RC rc = 0;
	switch( typeOfAggregation )
	{
		case AGGREGATION_BASIC:
		{
			getNextTuple_basic(data);
			status = false;
			return rc;
			break;
		}
		case AGGREGATION_GROUP:
		{
			RC rc;
			switch(op) {
				case MIN:
				case MAX:
				case SUM:
					rc = getNextTuple_groupMaxMinSum(data);
					break;
				case AVG:
					rc = getNextTuple_groupAvg(data);
					break;
				case COUNT:
					rc = getNextTuple_groupCount(data);
			}
			return rc;
		}
	}
};

void Aggregate::getNextTuple_basic(void *data){

	switch( this->op )
	{
		case MIN:
			getMin_basic(data);
			break;
		case MAX:
			getMax_basic(data);
			break;
		case SUM:
			getSum_basic(data);
			break;
		case AVG:
			getAvg_basic(data);
			break;
		case COUNT:
			getCount_basic(data);
			break;
	}
}

void Aggregate::getMin_basic(void *data) {
	int minInt = INT_MAX;
	float minFloat = FLT_MAX;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char value[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
			{
				void* fieldData = malloc(attr.length);
				short fieldSize;
				_rbfm->getAttrFromData(attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy(value, fieldData, fieldSize);
				free(fieldData);

				break;
			}
		}

//		 cout<<"----------------Print selected record--------------"<<endl;
//		 _rbfm->printRecord(attributeVector, data);
//		 cout<<"----------------END selected record--------------"<<endl;

		switch( aggAttr.type )
		{
			case TypeInt:
				if( *((int *)value) < minInt )
					minInt = *((int *)value);
				break;
			case TypeReal:
				if( *((float *)value) < minFloat )
					minFloat = *((float *)value);
				break;
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
			memcpy( data , &minInt, sizeof(int));
			break;
		case TypeReal:
			memcpy( data , &minFloat, sizeof(float));
			break;
		default:
			break;

	}

	free( returnValue );
}

void Aggregate::getMax_basic(void *data) {
	int maxInt = INT_MIN;
	float maxFloat = -FLT_MAX;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

//	cout << "getMax_basic start" << endl;
	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char value[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
			{
				void* fieldData = malloc(attr.length);
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)value, fieldData, fieldSize);
				free(fieldData);
				break;

			}
		}

		switch( aggAttr.type )
		{
			case TypeInt:
//				printf("%d\n", *((int*)((char*)value)));
				if( *((int *)value) > maxInt )
					maxInt = *((int *)value);
				break;
			case TypeReal:
//				printf("%.4f\n", *((float*)((char*)value)));
				if (maxFloat < *((float *)value)) {
					maxFloat = *((float *)value);
				}
				break;
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
			memcpy( data , &maxInt, sizeof(int));
			break;
		case TypeReal:
			memcpy( data , &maxFloat, sizeof(float));
			break;
		default:
			break;

	}

	free( returnValue );
}

void Aggregate::getSum_basic(void *data) {
	int sumInt = 0;
	float sumFloat = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char value[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
			{
				void* fieldData = malloc(attr.length);
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)value, fieldData, fieldSize);
				free(fieldData);
				break;
			}
		}

		switch( aggAttr.type )
		{
			case TypeInt:
				sumInt += *((int *) value );
				break;
			case TypeReal:
				sumFloat += *((float *) value );
				break;
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
			memcpy( data , &sumInt, sizeof(int));
			break;
		case TypeReal:
			memcpy( data , &sumFloat, sizeof(float));
			break;
		default:
			break;

	}

	free( returnValue );
}

void Aggregate::getAvg_basic(void *data) {
	int sumInt = 0;
	float sumFloat = 0;

	float count = 0.;

	float avg = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char value[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
			{
				void* fieldData = malloc(attr.length);
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy(value, fieldData, fieldSize);
				free(fieldData);
				break;
			}
		}

		switch( aggAttr.type )
		{
			case TypeInt:
			{
				sumInt += *((int *) value );
				// cout<<"get value: "<<*(int*)value<<endl;
				break;
			}
			case TypeReal:
			{
				sumFloat += *((float *) value );
				// cout<<"get value: "<<*(float*)value<<endl;
				break;
			}
			default:
				cout << "Type not supported" << endl;
		}

		count +=  1.0;
	}

	switch( aggAttr.type )
	{
		case TypeInt:
		{
			// cout << sumInt << endl;
			// cout << count << endl;
			avg = (float)sumInt / count;
			// cout << avg << endl;
			break;
		}
		case TypeReal:
		{
			// cout << sumFloat << endl;
			// cout << count << endl;
			avg = sumFloat / count;
			// cout << avg << endl;
			break;
		}
		default:
			break;
	}

	memcpy( data , &avg, sizeof(float));

	free(returnValue);
}

void Aggregate::getCount_basic(void *data) {
	unordered_set<float> countFloat;
	unordered_set<int> countInt;

	int countSize = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char value[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
			{
				void* fieldData = malloc(attr.length);
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy(value, fieldData, fieldSize);
				free(fieldData);
				break;
			}
		}

		switch( aggAttr.type )
		{
			case TypeInt:
				countInt.insert(*((int *)value));
				break;
			case TypeReal:
				countFloat.insert(*((float *)value));
				break;
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
			countSize = countInt.size();
			// cout << countSize << endl;
			break;
		case TypeReal:
			countSize = countFloat.size();
			// cout << countSize << endl;
			break;
		default:
			break;

	}
	// int resSize = (int)countSize;

	memcpy(data, &countSize, sizeof(int));

	free( returnValue );
}

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	Attribute attribute = aggAttr;

	attrs.clear();

	switch( this->op )
	{
		case MIN:
			attribute.name = "MIN(" + aggAttr.name + ")";
			break;
		case MAX:
			attribute.name = "MAX(" + aggAttr.name + ")";
			break;
		case SUM:
			attribute.name = "SUM(" + aggAttr.name + ")";
			break;
		case AVG:
			attribute.name = "AVG(" + aggAttr.name + ")";
			attribute.type = TypeReal;
			// attribute.length = 4;
			break;
		case COUNT:
			attribute.name = "COUNT(" + aggAttr.name + ")";
			attribute.type = TypeInt;
			break;
	}

	attrs.push_back(attribute);

};

void Aggregate::calculateMinForGroup()
{
	int matchCount = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{

		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( matchCount >= 2 )
				return;

			if( aggAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}
		}

		switch( aggAttr.type )
		{
			case TypeInt:
				 if( groupAttr.type == TypeInt )
				 {
					 cout << "groupAttr.type=int" << endl;
					 groupMin( groupmap_int_int, *((int*)groupAttrValue), *((int*)aggrValue) );
				 }
				break;

			case TypeReal:
				 if( groupAttr.type == TypeReal )
					 groupMin( groupmap_int_float, *((int*)groupAttrValue), *((float*)aggrValue) );
				break;

			case TypeVarChar:
				if( groupAttr.type == TypeVarChar )
				{
					 cout << "groupAttr.type=varchar" << endl;
					int length = *((int*)groupAttrValue);
//					substr(sizeof(int), length-sizeof(int), groupAttrValue);
					string gString = string( groupAttrValue, length );
					cout << "gString=" << gString << endl;
					groupMin( groupmap_int_string, *((int*)groupAttrValue),  gString);
				}
				break;

			default:
				cout << "Type Not Supported" << endl;
				break;
		}

	}

	free( returnValue );
}

void Aggregate::calculateMaxForGroup()
{
	int matchCount = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);


	while (iterator->getNextTuple(returnValue) != QE_EOF) {

		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( matchCount >= 2 )
				return;

			if( aggAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupMax(groupmap_int_int, gVal, aggVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupMax(groupmap_float_int, gVal, aggVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMax(groupmap_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupMax(groupmap_int_float, gVal, aggVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupMax(groupmap_float_float, gVal, aggVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupMax(groupmap_string_float, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}

void Aggregate::calculateSumForGroup()
{
	int matchCount = 0;

	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while (iterator->getNextTuple(returnValue) != QE_EOF) {
		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		for( Attribute attr: attributeVector )
		{
			if( matchCount >= 2 )
				return;

			if( aggAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
				free(fieldData);
				matchCount += 1;
			}
		}


		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupSum(groupmap_int_int, gVal, aggVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupSum(groupmap_float_int, gVal, aggVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupSum(groupmap_string_int, gVal, aggVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupSum(groupmap_int_float, gVal, aggVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupSum(groupmap_float_float, gVal, aggVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupSum(groupmap_string_float, gVal, aggVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}

void Aggregate::calculateAvgForGroup()
{
	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{

		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		int matchCount = 0;

		for( Attribute attr: attributeVector )
		{
//			cout << "matchCount=" << matchCount << endl;
			if( matchCount >= 2 )
				break;

			if( aggAttr.name == attr.name )
			{
//				cout << "aggAttr.name=" << aggAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
//				cout << "aggrValue" << aggrValue << endl;
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
//				cout << "groupAttr.name=" << groupAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
//				cout << "groupAttrValue" << groupAttrValue << endl;
				free(fieldData);
				matchCount += 1;
			}
		}

//		cout << "aggrValue=" << aggrValue << ", groupAttrValue" << groupAttrValue << endl;

//			float aggVal = *((float*)aggrValue);
//		double aggrVal = atof(aggrValue);
//		float aggrF = (float)aggrVal;

//		cout << "aggrF=" << aggrF << endl;

//		cout << "aggAttr.type =" << aggAttr.type << endl;
//		cout << "groupAttr.type =" << groupAttr.type << endl;

		if( aggAttr.type == TypeInt )
		{
			float aggrF = (float)*((int *)aggrValue);
			 if( groupAttr.type == TypeInt )
			 {
				 int gVal = *((int *)groupAttrValue);
//				 cout << "gVal=" << gVal << endl;
				 groupAvg( groupmap_int_float, groupmap_int_int, gVal, aggrF );
			 }
			 else if( groupAttr.type == TypeReal )
			 {
				 float gVal = *((float *)groupAttrValue);
//				 cout << "gVal=" << gVal << endl;
				 groupAvg( groupmap_float_float, groupmap_float_int, gVal, aggrF );
			 }
			 else if( groupAttr.type == TypeVarChar )
			 {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);

//				cout << "gVal=" << gVal << endl;
				groupAvg( groupmap_string_float, groupmap_string_int, gVal, aggrF );
			 }
		}
		else if (aggAttr.type == TypeReal) {
			float aggrF = *((float *)aggrValue);
			if( groupAttr.type == TypeInt )
			 {
				 int gVal = *((int *)groupAttrValue);
//				 cout << "gVal=" << gVal << endl;
				 groupAvg( groupmap_int_float, groupmap_int_int, gVal, aggrF );
			 }
			 else if( groupAttr.type == TypeReal )
			 {
				 float gVal = *((float *)groupAttrValue);
//				 cout << "gVal=" << gVal << endl;
				 groupAvg( groupmap_float_float, groupmap_float_int, gVal, aggrF );
			 }
			 else if( groupAttr.type == TypeVarChar )
			 {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);

//				cout << "gVal=" << gVal << endl;
				groupAvg( groupmap_string_float, groupmap_string_int, gVal, aggrF );
			 }
		}

	}

	free( returnValue );

}

void Aggregate::calculateCountForGroup()
{
	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	while (iterator->getNextTuple(returnValue) != QE_EOF) {

		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		int matchCount = 0;

		for( Attribute attr: attributeVector )
		{
//			cout << "matchCount=" << matchCount << endl;
			if( matchCount >= 2 )
				break;

			if( aggAttr.name == attr.name )
			{
//				cout << "aggAttr.name=" << aggAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
//				cout << "aggrValue" << aggrValue << endl;
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
//				cout << "groupAttr.name=" << groupAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
//				cout << "groupAttrValue" << groupAttrValue << endl;
				free(fieldData);
				matchCount += 1;
			}
		}

		if (aggAttr.type == TypeInt) {
			int aggVal = *((int *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupCount(groupmap_int_int, gVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupCount(groupmap_float_int, gVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupCount(groupmap_string_int, gVal);
			}
		} else if (aggAttr.type == TypeReal) {
			float aggVal = *((float *)aggrValue);
			if (groupAttr.type == TypeInt) {
				int gVal = *((int *)groupAttrValue);
				groupCount(groupmap_int_int, gVal);
			} else if (groupAttr.type == TypeReal) {
				float gVal = *((float *)groupAttrValue);
				groupCount(groupmap_float_int, gVal);
			} else {
				int len = *((int *)groupAttrValue);
				memcpy(str, groupAttrValue + sizeof(int), len);
				str[len] = '\0';
				string gVal(str);
				groupCount(groupmap_string_int, gVal);
			}
		} else {
			cerr << "Try to aggregate a char that we don't support" << endl;
		}
	}
}

RC Aggregate::getNextTuple_groupAvg(void *data) {
	char *returnData = (char *)data;
	if (groupAttr.type == TypeInt) {
		if (groupmap_int_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = groupmap_int_int.begin();
			int id = itr_1->first;
			int cnt = itr_1->second;
			auto itr_2 = groupmap_int_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			copyValue(returnData, &id, groupAttr.type);
			copyValue(returnData+sizeof(int), &avg, aggAttr.type);
			groupmap_int_int.erase(itr_1);
			groupmap_int_float.erase(itr_2);
		}
	} else if (groupAttr.type == TypeReal) {
		if (groupmap_float_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = groupmap_float_int.begin();
			int id = itr_1->first;
			int cnt = itr_1->second;
			auto itr_2 = groupmap_float_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			copyValue(returnData, &id, groupAttr.type);
			copyValue(returnData+sizeof(int), &avg, aggAttr.type);
			groupmap_float_int.erase(itr_1);
			groupmap_float_float.erase(itr_2);
		}
	} else {
		if (groupmap_string_int.empty()) {
			return QE_EOF;
		} else {
			auto itr_1 = groupmap_string_int.begin();
			string id = itr_1->first;
			int id_len = id.size();
			int cnt = itr_1->second;
			auto itr_2 = groupmap_string_float.begin();
			float sum =  itr_2->second;
			float avg = sum / (float)cnt;
			memcpy(returnData, &id_len, sizeof(int));
			memcpy(returnData+sizeof(int), id.c_str(), id_len);
			copyValue(returnData+sizeof(int)+id_len, &avg, aggAttr.type);
			groupmap_string_int.erase(itr_1);
			groupmap_string_float.erase(itr_2);
		}
	}
	return 0;
}

RC Aggregate::getNextTuple_groupMaxMinSum(void *data) {
	char *returnData = (char *)data;
	if (aggAttr.type == TypeInt) {
		if (groupAttr.type == TypeInt) {
			if (groupmap_int_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_int_int.begin();
				int id = itr->first;
				int val = itr->second;
				copyValue(returnData, &id, groupAttr.type);
				copyValue(returnData+sizeof(int), &val, aggAttr.type);
				groupmap_int_int.erase(itr);
			}
		} else if (groupAttr.type == TypeReal) {
			if (groupmap_float_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_float_int.begin();
				float id = itr->first;
				int val = itr->second;
				copyValue(returnData, &id, groupAttr.type);
				copyValue(returnData+sizeof(float), &val, aggAttr.type);
				groupmap_float_int.erase(itr);
			}
		} else {
			if (groupmap_string_int.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_string_int.begin();
				string id = itr->first;
				int id_len = id.size();
				int val = itr->second;
				memcpy(returnData, &id_len, sizeof(int));
				memcpy(returnData+sizeof(int), id.c_str(), id_len);
				copyValue(returnData+sizeof(int)+id_len, &val, aggAttr.type);
				groupmap_string_int.erase(itr);
			}
		}
	} else if (aggAttr.type == TypeReal) {
		if (groupAttr.type == TypeInt) {
			if (groupmap_int_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_int_float.begin();
				int id = itr->first;
				float val = itr->second;
				copyValue(returnData, &id, groupAttr.type);
				copyValue(returnData+sizeof(int), &val, aggAttr.type);
				groupmap_int_float.erase(itr);
			}
		} else if (groupAttr.type == TypeReal) {
			if (groupmap_float_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_float_float.begin();
				float id = itr->first;
				float val = itr->second;
				copyValue(returnData, &id, groupAttr.type);
				copyValue(returnData+sizeof(float), &val, aggAttr.type);
				groupmap_float_float.erase(itr);
			}
		} else {
			if (groupmap_string_float.empty()) {
				return QE_EOF;
			} else {
				auto itr = groupmap_string_float.begin();
				string id = itr->first;
				int id_len = id.size();
				float val = itr->second;
				memcpy(returnData, &id_len, sizeof(int));
				memcpy(returnData+sizeof(int), id.c_str(), id_len);
				copyValue(returnData+sizeof(int)+id_len, &val, aggAttr.type);
				groupmap_string_float.erase(itr);
			}
		}
	}

	return 0;
}

RC Aggregate::getNextTuple_groupCount(void *data) {
	char *returnData = (char *)data;
	if (groupAttr.type == TypeInt) {
		if (groupmap_int_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = groupmap_int_int.begin();
			int id = itr->first;
			int val = itr->second;
			copyValue(returnData, &id, groupAttr.type);
			copyValue(returnData+sizeof(int), &val, aggAttr.type);
			groupmap_int_int.erase(itr);
		}
	} else if (groupAttr.type == TypeReal) {
		if (groupmap_float_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = groupmap_float_int.begin();
			float id = itr->first;
			int val = itr->second;
			copyValue(returnData, &id, groupAttr.type);
			copyValue(returnData+sizeof(float), &val, aggAttr.type);
			groupmap_float_int.erase(itr);
		}
	} else {
		if (groupmap_string_int.empty()) {
			return QE_EOF;
		} else {
			auto itr = groupmap_string_int.begin();
			string id = itr->first;
			int id_len = id.size();
			int val = itr->second;
			memcpy(returnData, &id_len, sizeof(int));
			memcpy(returnData+sizeof(int), id.c_str(), id_len);
			copyValue(returnData+sizeof(int)+id_len, &val, aggAttr.type);
			groupmap_string_int.erase(itr);
		}
	}
	return 0;
}

// implement of GHJoin
GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
        Iterator *rightIn,               // Iterator of input S
        const Condition &condition,      // Join condition (CompOp is always EQ)
        const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
  	  	){
	// 12.07 finalized xkwang
	leftIn->getAttributes(leftAttrList);
	rightIn->getAttributes(rightAttrList);

	this->condition = condition;

	// partition
	identityName = condition.lhsAttr+condition.rhsAttr;
	// cout<<"Create partition "<<identityName<<endl;
	partitionOperator(leftIn, "left"+identityName, numPartitions,condition.lhsAttr);
	partitionOperator(rightIn, "right"+identityName, numPartitions, condition.rhsAttr);
	_rbfm->createFile("res"+identityName);

	// merge attribute
	// cout<<"Merge attribute"<<endl;
	mergeAttrList.clear();
	for(Attribute iter1:leftAttrList){
		mergeAttrList.push_back(iter1);
	}
	for(Attribute iter1:rightAttrList){
		mergeAttrList.push_back(iter1);
	}

	// merge partition
	// cout<<"Merge partition"<<endl;
	for(unsigned iter1 = 0; iter1<numPartitions; iter1++)
		mergePartition(iter1, identityName, leftAttrList, rightAttrList, condition, mergeAttrList);

	// initialize iterator on mergeResult
	vector<string> mergeAttrName;
	_rbfm->openFile("res"+identityName, resFileHandle);
	getAllAttrNames(mergeAttrList, mergeAttrName);
	resScaner.initialize(resFileHandle, mergeAttrList, "", NO_OP, NULL, mergeAttrName);
}

GHJoin::~GHJoin(){
	// cout<<"destory"<<endl;
	// for(unsigned iter1 = 0; iter1<numPartitions; iter1++){
	// 	string partitionName = identityName+to_string(iter1);
	// 	_rbfm->destroyFile("left"+partitionName);
	// 	_rbfm->destroyFile("right"+partitionName);
	// 	cout<<"destory "<<" left"+partitionName<<endl;
	// }
	// _rbfm->destroyFile("res"+identityName);
}

RC GHJoin::getAllAttrNames(vector<Attribute> attrList, vector<string> &attrNames){
	for(unsigned iter1 = 0; iter1<attrList.size(); iter1++)
		attrNames.push_back(attrList.at(iter1).name);
	return 0;
}


RC GHJoin::mergePartition(int iter1, string identityName, vector<Attribute> leftAttrList,
							vector<Attribute> rightAttrList, Condition condition, vector<Attribute>mergeAttrList){

	FileHandle leftFileHandle, rightFileHandle, resFileHandle;
	vector<void *> inMemorySet;
	RBFM_ScanIterator leftRbfmScanner,rightRbfmScanner;
	vector<string> leftAttrNames,rightAttrNames;
	Attribute comAttr;
	RID rid;
	RC rc = -1;
	void *recordData;
	unsigned estRecordSize;

	// cout<<"Open partition "<<iter1<<" File"<<endl;
	_rbfm->openFile("left"+identityName+to_string(iter1), leftFileHandle);
	_rbfm->openFile("right"+identityName+to_string(iter1), rightFileHandle);
	_rbfm->openFile("res"+identityName, resFileHandle);

	if(leftFileHandle.getNumberOfPages()>rightFileHandle.getNumberOfPages()){
		//swap left and right
	}

	// load left partition
	getAllAttrNames(leftAttrList, leftAttrNames);
	getAllAttrNames(rightAttrList, rightAttrNames);
	_rbfm->selectAttribute(rightAttrList, condition.rhsAttr, comAttr);

	leftRbfmScanner.initialize(leftFileHandle, leftAttrList, "", NO_OP, NULL, leftAttrNames);
	estRecordSize = _rbfm->getEstimatedRecordDataSize(leftAttrList);
	recordData = malloc(estRecordSize);
	// cout<<"Loading inMemorySet"<<endl;
	while(leftRbfmScanner.getNextRecord(rid, recordData)!=-1){
		inMemorySet.push_back(recordData);
		recordData = malloc(estRecordSize);
	}
	free(recordData);
	// compare to another partition & write to file
	rightRbfmScanner.initialize(rightFileHandle, rightAttrList, "", NO_OP, NULL, rightAttrNames);
	estRecordSize = _rbfm->getEstimatedRecordDataSize(rightAttrList);
	recordData = malloc(estRecordSize);
	int keyLength = comAttr.length+sizeof(int);
	void *leftKey = malloc(keyLength);
	void *rightKey = malloc(keyLength);

	// cout<<"Start finding matching"<<endl;
	while(rightRbfmScanner.getNextRecord(rid, recordData)!=-1){
		for(unsigned iter1= 0; iter1<inMemorySet.size();iter1++){
			short leftSize, rightSize; // in case of long str
			void *leftRecord = inMemorySet.at(iter1);
			_rbfm->getAttrFromData(leftAttrList, leftRecord, leftKey, condition.lhsAttr, leftSize);
			_rbfm->getAttrFromData(rightAttrList, recordData, rightKey, condition.rhsAttr, rightSize);
			// cout<<"----------Record 1---------------"<<comAttr.name<<endl;
			// _rbfm->printRecord(leftAttrList, leftRecord);
			if(_rbfm->keyCompare(leftKey, rightKey, comAttr)==0){
				// cout<<"Key match : "<<comAttr.name<<endl;
				// cout<<"----------Record 2---------------"<<endl;
				// _rbfm->printRecord(rightAttrList, recordData);
				// cout<<"------------------"<<endl;
				int actLeftRecordSize, actRightRecordSize;
				actLeftRecordSize = _rbfm->getSizeOfData(leftAttrList, leftRecord);
				actRightRecordSize = _rbfm->getSizeOfData(rightAttrList,recordData);
				// printf("Diagnostic value: %d %d %u %u %u\n",actLeftRecordSize,actRightRecordSize,leftAttrList.size(),
					// rightAttrList.size(),mergeAttrList.size());
				void *mergeData = malloc(actLeftRecordSize+actRightRecordSize);
				memcpy(mergeData, leftRecord, actLeftRecordSize);
				memcpy((char*)mergeData+actLeftRecordSize, recordData, actRightRecordSize);
				rc = _rbfm->insertRecord(resFileHandle, mergeAttrList, mergeData, rid);
				// cout<<"merged Record: "<<endl;
				// _rbfm->printRecord(mergeAttrList, mergeData);
				free(mergeData);
			}
		}
	}

	_rbfm->closeFile(leftFileHandle);
	_rbfm->closeFile(rightFileHandle);
	_rbfm->closeFile(resFileHandle);

	_rbfm->destroyFile("left"+identityName+to_string(iter1));
	_rbfm->destroyFile("right"+identityName+to_string(iter1));
	// _rbfm->destroyFile("res"+identityName);
	free(leftKey);
	free(rightKey);
	return rc;
}




void GHJoin::partitionOperator(Iterator *iter, string identityName, const unsigned numPartitions, string attrName){

	for(unsigned iter1 = 0; iter1<numPartitions; iter1++)
		_rbfm->createFile(identityName+to_string(iter1));

	vector<Attribute> attrList;
	Attribute keyAttr;
	iter->getAttributes(attrList);
	_rbfm->selectAttribute(attrList, attrName,keyAttr);

	void *recordData = malloc(_rbfm->getEstimatedRecordDataSize(attrList));
	void *keyData = malloc(_rbfm->getEstimatedRecordDataSize(attrList));
	short keySize;
	unsigned hashKey,bucketId;
	vector<FileHandle> fileHandleList;
	RID rid;

	for(unsigned iter1=0;iter1<numPartitions;iter1++){
		FileHandle fileHandle;
		_rbfm->openFile(identityName+to_string(iter1), fileHandle);
		fileHandleList.push_back(fileHandle);
	}

	while(iter->getNextTuple(recordData)!=QE_EOF){
		_rbfm->getAttrFromData(attrList, recordData, keyData, attrName,keySize);
		hashKey = _ix->hash(keyAttr,keyData);
		bucketId = hashKey%numPartitions;
		_rbfm->insertRecord(fileHandleList.at(bucketId), attrList, recordData, rid);
	}

	for(unsigned iter1=0;iter1<numPartitions;iter1++)
		_rbfm->closeFile(fileHandleList.at(iter1));
	fileHandleList.clear();
}

void GHJoin::getAttributes(vector<Attribute> &attrList)const{
	attrList = this->mergeAttrList;
}

RC GHJoin::getNextTuple(void *data){
	RID rid;
	RC rc = resScaner.getNextRecord(rid, data);
	if(rc == -1 && nonNull == true)
		_rbfm->destroyFile("res"+identityName);
	else
		nonNull = true;
	return rc;
}

//end of implement of GHJoin

template <typename T>
bool compareValueByAttrType( T const lhs_value, T const rhs_value, CompOp compOp) {

	switch( compOp ) {
		case EQ_OP:
			return lhs_value == rhs_value;
			break;
		case LT_OP:
			return lhs_value < rhs_value;
			break;
		case GT_OP:
			return lhs_value > rhs_value;
			break;
		case LE_OP:
			return lhs_value <= rhs_value;
			break;
		case GE_OP:
			return lhs_value >= rhs_value;
			break;
		case NE_OP:
			return lhs_value != rhs_value;
			break;
		default:
			return true;
	}
	return true;
}
