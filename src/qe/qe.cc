
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

	if( condition.bRhsIsAttr == false )
		return;

	iterator = leftIn;
	iterator->getAttributes( leftAttributeVector );

	for( Attribute attr : leftAttributeVector )
	{
		if( attr.name == condition.lhsAttr )
		{
			cout << "attr.name=" << attr.name << endl;
			cout << "condition.lhsAttr=" << condition.lhsAttr << endl;
			attrType = attr.type;
			break;
		}
	}

	_rbfm->selectAttribute(leftAttributeVector, condition.lhsAttr, selectAttr);

	totalAttributes = leftAttributeVector;

	indexScan = rightIn;
	indexScan->getAttributes( rightAttributeVector );

	for( Attribute rAttr: rightAttributeVector )
	{
		cout << "right attribute name=" << rAttr.name << endl;
		totalAttributes.push_back ( rAttr );
	}

	this->condition = condition;

	init = true;
	retrieveNextLeftValue = true;
}

RC INLJoin::getNextTuple(void *data){
	if( init == false )
		return init;

	int rRecordLen = _rbfm->getEstimatedRecordDataSize( rightAttributeVector );
	void *rightValue = malloc( rRecordLen );

	int lRecordLen = _rbfm->getEstimatedRecordDataSize( leftAttributeVector );
	void *leftValue = malloc( lRecordLen );

	void* rDataAll = malloc(PAGE_SIZE);
	void* lDataAll = malloc(PAGE_SIZE);

	RC rc;

	do {

		// Get next tuple from right iterator
		rc = indexScan->getNextTuple( rightValue );
		cout << "indexScan->getNextTuple=" << rc << endl;
		if( rc != 0 )
			retrieveNextLeftValue = true;

//		cout<<"----------------Print right selected record--------------"<<endl;
//		_rbfm->printRecord(rightAttributeVector, rightValue);
//		cout<<"----------------END right selected record--------------"<<endl;

		if( retrieveNextLeftValue )
		{
			do{
				// Try loading one more time
				rc = iterator->getNextTuple( leftValue );
				if( rc != 0 )
				{
					cout << "Failed to load left input value " << endl;
					return QE_EOF;
//					break;
				}

//				cout<<"----------------Print left next selected record--------------"<<endl;
//				_rbfm->printRecord(leftAttributeVector, leftValue);
//				cout<<"----------------END left next selected record--------------"<<endl;

				int offSet = 0;
				vector<Attribute> lList;

				for( Attribute attr: leftAttributeVector ){
//					cout << "left attribute name=" << attr.name << endl;
					if( attr.name == condition.lhsAttr )
					{
						Attribute cmpAttr;
						cout << "left condition.lhsAttr=" << condition.lhsAttr << endl;
						_rbfm->selectAttribute(leftAttributeVector, condition.lhsAttr, cmpAttr);
						void* lData = malloc(cmpAttr.length);
						short lfieldSize;
						_rbfm->getAttrFromData(leftAttributeVector, leftValue, lData, condition.lhsAttr, lfieldSize);
						memcpy((char*)lDataAll + offSet, lData, lfieldSize);
//						memcpy( leftCondition, lData, lfieldSize );
						offSet+=lfieldSize;
						lList.push_back(cmpAttr);
						free(lData);
					}
				}

				// Select attribute only and print
				cout<<"----------------Print left selected attribute--------------"<<endl;
				_rbfm->printRecord(lList, lDataAll);
				cout<<"----------------END left selected attribute--------------"<<endl;

				// Tuple retrieval failed
				retrieveNextLeftValue = false;

				// Reset the iterator
				setRightIterator( (char*)lDataAll );

			} while( indexScan->getNextTuple( rightValue ) == QE_EOF );
		}

		int roffSet = 0;
		vector<Attribute> rList;

		for( Attribute attr: rightAttributeVector ){
			if( attr.name == condition.rhsAttr )
			{
//				cout << "attribute name=" << attr.name << " condition.rhsAttr=" << condition.rhsAttr << endl;
				void* rData = malloc(attr.length);
				short rfieldSize;
				_rbfm->getAttrFromData(rightAttributeVector, rightValue, rData, attr.name, rfieldSize);
				memcpy((char*)rDataAll + roffSet, rData, rfieldSize);
//				memcpy(rightCondition, rData, rfieldSize);
				roffSet+=rfieldSize;
				rList.push_back(attr);
				free(rData);
			}

		}

		cout<<"----------------Print right selected record--------------"<<endl;
		_rbfm->printRecord(rList, rDataAll);
		cout<<"----------------END right selected record--------------"<<endl;

//	} while( !compareValue( leftCondition, rightCondition, condition.op,  attrType) );
//	} while( !compareValue( (char*)lDataAll, (char*)rDataAll, condition.op,  attrType) );

		cout<<"Before Key match : "<< endl;
		rc = _rbfm->keyCompare( lDataAll, rDataAll, selectAttr );
		cout<<"Key match : "<< selectAttr.name << " rc=" << rc << endl;
		// cout<<"----------Record 2---------------"<<endl;
		// _rbfm->printRecord(rightAttrList, recordData);
		// cout<<"------------------"<<endl;

	} while( rc != 0 );

	int sizeOfLeftRecord = _rbfm->getSizeOfRecord( leftAttributeVector, leftValue );
	int sizeOfRightRecord = _rbfm->getSizeOfRecord( rightAttributeVector, rightValue );

	memcpy( (char*)data, leftValue, sizeOfLeftRecord );
	memcpy( (char*)data + sizeOfLeftRecord , rightValue, sizeOfRightRecord );

	/* free memory allocated for each record for each relation */
	free( rightValue );
	free( leftValue );

	/* free memory allocated for each indexed attribute for each relation */
	free( lDataAll );
	free( rDataAll );

	return 0;

}

void INLJoin::setRightIterator(char* value) {

	switch( condition.op ) {
		case EQ_OP:
			cout << "setIterator to" << "equal w/ value:" << value << endl;
			indexScan->setIterator(value, value, true, true);
			break;
		case LT_OP:
			indexScan->setIterator(value, NULL, false, true);
			break;
		case GT_OP:
			indexScan->setIterator(NULL, value, true, false);
			break;
		case LE_OP:
			indexScan->setIterator(value, NULL, true, true);
			break;
		case GE_OP:
			indexScan->setIterator(NULL, value, true, true);
			break;
		case NE_OP:
			indexScan->setIterator(NULL, NULL, true, true);
			break;
		default:
			indexScan->setIterator(NULL, NULL, true, true);
			break;
	}
}

RC INLJoin::getAttributeValue( char* value, char* condition, vector<Attribute> attributeVector, string strCondition )
{
	RC rc = 0;
	for( Attribute attr: attributeVector )
	{
		if( attr.name == strCondition )
		{
			copyValue( value, condition, attr.type );
			return rc;
		}

		moveToValueByAttrType( value, attr.type );
	}

	return QE_FAIL_TO_FIND_ATTRIBUTE;
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

bool INLJoin::compareValue( const char* lhs_value, const char* rhs_value, CompOp compOp, AttrType attrType )
{
	// prevent cross initialization error
	int lhs_int, rhs_int;
	float lhs_float, rhs_float;
	bool result;

	cout << "compareValue:" << attrType << endl;

	switch( attrType )
	{
		case TypeInt:
			lhs_int = *((int *)lhs_value);
			rhs_int = *((int *)rhs_value);

			result = compareValueByAttrType( lhs_int, rhs_int, compOp );
			cout << "lhs_int=" << lhs_int << " ,rhs_int=" << rhs_int << " ,result=" << result << endl;
			return result;
			break;
		case TypeReal:
			lhs_float = *((float *)lhs_value);
			rhs_float = *((float *)rhs_value);

			result =  compareValueByAttrType( lhs_float, rhs_float, compOp );
			cout << "lhs_float=" << lhs_float << " ,rhs_float=" << rhs_float << " ,result=" << result << endl;
			return result;
			break;
		case TypeVarChar:
			int length = *((int*)lhs_value);
			std::string lhs_string(lhs_value, length);
			std::string rhs_string(rhs_value, length);

			result = compareValueByAttrType( lhs_string, rhs_string, compOp );
			cout << "lhs_string=" << lhs_string << " ,rhs_string=" << rhs_string << " ,result=" << result << endl;
			return result;
			break;
	}

	return true;

}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->totalAttributes;
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

	cout << "op=" << op << endl;
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
			RC rc = 0;
			switch( this->op )
			{
				case MIN:
					break;
				case MAX:
					break;
				case SUM:
					break;
				case AVG:
					break;
				case COUNT:
					break;
			}
			return rc;
	}
	return rc;
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
				break;
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

		switch( aggAttr. type )
		{
			case TypeInt:
				 if( groupAttr.type == TypeInt )
					 groupMin( groupmap_int_int, *((int*)groupAttrValue), *((int*)aggrValue) );
				break;

			case TypeReal:
				 if( groupAttr.type == TypeReal )
					 groupMin( groupmap_int_float, *((int*)groupAttrValue), *((float*)aggrValue) );
				break;

			case TypeVarChar:
				if( groupAttr.type == TypeVarChar )
				{
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

}

void Aggregate::calculateSumForGroup()
{

}

void Aggregate::calculateAvgForGroup()
{


	int fullRecordLen = _rbfm->getEstimatedRecordDataSize( attributeVector );
	void* returnValue = malloc(fullRecordLen);

	cout << "calculateAvgForGroup" << endl;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{

		char aggrValue[PAGE_SIZE];
		char groupAttrValue[PAGE_SIZE];

		int matchCount = 0;

		for( Attribute attr: attributeVector )
		{
			cout << "matchCount=" << matchCount << endl;
			if( matchCount >= 2 )
				break;

			if( aggAttr.name == attr.name )
			{
				cout << "aggAttr.name=" << aggAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)aggrValue, fieldData, fieldSize);
				cout << "aggrValue" << aggrValue << endl;
				free(fieldData);
				matchCount += 1;
			}

			if( groupAttr.name == attr.name )
			{
				cout << "groupAttr.name=" << groupAttr.name << endl;
				void* fieldData = malloc( attr.length );
				short fieldSize;
				_rbfm->getAttrFromData( attributeVector, returnValue, fieldData, attr.name, fieldSize);
				memcpy((char*)groupAttrValue, fieldData, fieldSize);
				cout << "groupAttrValue" << groupAttrValue << endl;
				free(fieldData);
				matchCount += 1;
			}
		}

		cout << "aggrValue=" << aggrValue << ", groupAttrValue" << groupAttrValue << endl;

//			float aggVal = *((float*)aggrValue);
		double aggrVal = atof(aggrValue);
		float aggrF = (float)aggrVal;

		cout << "aggrF=" << aggrF << endl;
		cout << "aggAttr.type =" << aggAttr.type << endl;

		switch( aggAttr.type )
		{
			case TypeInt:
				 if( groupAttr.type == TypeInt )
				 {
					 int gVal = *((int *)groupAttrValue);
					 cout << "gVal=" << gVal << endl;
					 groupAvg( groupmap_int_float, groupmap_int_int, gVal, aggrF );
				 }
				break;

			case TypeReal:
				 if( groupAttr.type == TypeReal )
				 {
					 float gVal = *((float *)groupAttrValue);
					 groupAvg( groupmap_float_float, groupmap_float_int, gVal, aggrF );
				 }
				break;

			case TypeVarChar:
				if( groupAttr.type == TypeVarChar )
				{
					int length = *((int*)groupAttrValue);
					cout << "length=" << length << endl;
//					substr(sizeof(int), length-sizeof(int), groupAttrValue);
					string gVal = string( groupAttrValue, length );
					cout << "gVal=" << gVal << endl;
					groupAvg( groupmap_string_float, groupmap_string_int, gVal, aggrF );
				}
				break;

			default:
				cout << "Type Not Supported" << endl;
				break;
		}

	}

	free( returnValue );

}

void Aggregate::calculateCountForGroup()
{

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
