
#include "qe.h"

/* Filter constructor */
Filter::Filter(Iterator* input, const Condition &condition) {
	/* Initialize variables */
	iterator = input;
	input->getAttributes(attrList);
}

RC Filter::getNextTuple(void *data) {
	// 12.08 xkwang reconstruct
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
//	cout << "Filter getAttributes Start" << endl;
	iterator->getAttributes(attrs);
//	cout << "Filter getAttributes End" << endl;
}

void Filter::setValue(Value rhsValue) {

//	cout << "Filter setValue Start" << endl;
	switch( type ) {
		case TypeInt:
			memcpy( rhs_value, rhsValue.data, sizeof(int) );
			break;
		case TypeReal:
			memcpy( rhs_value, rhsValue.data, sizeof(float) );
			break;
		case TypeVarChar:
			int length = *((int*)rhsValue.data);
			memcpy( rhs_value, rhsValue.data, sizeof(int) + length );
			break;
	}

//	cout << "Filter setValue End" << endl;
}

bool Filter::valueCompare(void *data) {

//	cout << "Filter valueCompare Start" << endl;
	char *lhs_value = (char *)data;

	for ( Attribute attr : attrList ) {
		if ( attr.name == lhsAttr ) {
			break;
		}

		moveToValueByAttrType( lhs_value, attr.type );
	}

//	cout << "Filter moveToValueByAttrType Pass" << endl;

	// prevent cross initialization error
	int lhs_int, rhs_int;
	float lhs_float, rhs_float;

	switch( type )
	{
		case TypeInt:
			lhs_int = *((int *)lhs_value);
			rhs_int = *((int *)rhs_value);

			compareValueByAttrType( lhs_int, rhs_int, compOp );
			break;
		case TypeReal:
			lhs_float = *((float *)lhs_value);
			rhs_float = *((float *)rhs_value);

			compareValueByAttrType(lhs_float, rhs_float, compOp );
			break;
		case TypeVarChar:
			int length = *((int*)lhs_value);
			std::string lhs_string(lhs_value, length);
			std::string rhs_string(rhs_value, length);

			compareValueByAttrType(lhs_string, rhs_string, compOp);
			break;
	}

	return true;
}

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

/*
BNLJoin::BNLJoin(Iterator *leftIn,
        TableScan *rightIn,
        const Condition &condition,
        const unsigned numRecords) {

}

RC BNLJoin::getNextTuple(void *data){
};

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{

};
*/

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
			attrType = attr.type;
			break;
		}
	}

	totalAttributes = leftAttributeVector;

	indexScan = rightIn;
	indexScan->getAttributes( rightAttributeVector );

	for( Attribute lAttr: rightAttributeVector )
	{
		totalAttributes.push_back ( lAttr );
	}

	this->condition = condition;
}

RC INLJoin::getNextTuple(void *data){
	RC rc;

	do {
		// Get next tuple from right iterator
		rc = indexScan->getNextTuple( rightValue );
		if( rc != 0 )
		{
			do{
				// Try loading one more time
				rc = indexScan->getNextTuple( leftValue );
				if( rc != 0 )
				{
					cout << "Failed to load left input value " << endl;
					return QE_EOF;
				}

				rc = getAttributeValue( leftValue, leftCondition, leftAttributeVector, condition.lhsAttr );
				if( rc != 0 )
				{
					cout << "Failed to read attribute value from left iterator" << endl;
					return QE_EOF;
				}

				// Tuple retrieval failed
				retrieveNextLeftValue = false;

				// Reset the iterator
				setRightIterator( leftValue );
//				indexScan->setIterator( condition.rhsValue.data, condition.rhsValue.data, true, true );

			} while( indexScan->getNextTuple( rightValue ) != QE_EOF );
		}


		// Get right condition value
		rc = getAttributeValue( rightValue, rightCondition, rightAttributeVector, condition.rhsAttr );
		if( rc != 0 )
		{
			cout << "Failed to read attribute value from right condition value" << endl;
			return rc;
		}

	} while( !compareValue( leftCondition, rightCondition, condition.op,  attrType) );

	int sizeOfLeftRecord = _rbfm->getSizeOfRecord( leftAttributeVector, leftValue );
	int sizeOfRightRecord = _rbfm->getSizeOfRecord( rightAttributeVector, rightValue );

	memcpy( (char*)data, leftValue, sizeOfLeftRecord );
	memcpy( (char*)data + sizeOfLeftRecord , rightValue, sizeOfRightRecord );

	return 0;

}

void INLJoin::setRightIterator(char* value) {

	switch( condition.op ) {
		case EQ_OP:
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

	switch( attrType )
	{
		case TypeInt:
			lhs_int = *((int *)lhs_value);
			rhs_int = *((int *)rhs_value);

			compareValueByAttrType( lhs_int, rhs_int, compOp );
			break;
		case TypeReal:
			lhs_float = *((float *)lhs_value);
			rhs_float = *((float *)rhs_value);

			compareValueByAttrType( lhs_float, rhs_float, compOp );
			break;
		case TypeVarChar:
			int length = *((int*)lhs_value);
			std::string lhs_string(lhs_value, length);
			std::string rhs_string(rhs_value, length);

			compareValueByAttrType( lhs_string, rhs_string, compOp );
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
		cout << "VarChar Type not supported! " << " Exit with error code= " <<QE_ATTRIBUTE_NOT_SUPPORTED << endl;

	typeOfAggregation =  AGGREGATION_BASIC;

	iterator = input;

	iterator->getAttributes(attributeVector);

	this->aggAttr = aggAttr;

	this->op = op;

};

// Optional for everyone. 5 extra-credit points
// Group-based hash aggregation
Aggregate:: Aggregate(Iterator *input,             // Iterator of input R
        Attribute aggAttr,           				// The attribute over which we are computing an aggregate
        Attribute groupAttr,         				// The attribute over which we are grouping the tuples
        AggregateOp op,              				// Aggregate operation
        const unsigned numPartitions 				// Number of partitions for input (decided by the optimizer)
){

	if( aggAttr.type == TypeVarChar )
		cout << "VarChar Type not supported! " << " Exit with error code= " <<QE_ATTRIBUTE_NOT_SUPPORTED << endl;

	typeOfAggregation =  AGGREGATION_GROUP;

	iterator = input;

	iterator->getAttributes(attributeVector);

	this->aggAttr = aggAttr;

	this->groupAttr = groupAttr;

	this->op = op;

	this->numPartitions = numPartitions;

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

	RC rc = 0;
	switch( typeOfAggregation )
	{
		case AGGREGATION_BASIC:
		{
			getNextTuple_basic(data);
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
	void* returnValue = malloc(PAGE_SIZE);
	int minInt = INT_MAX;
	float minFloat = FLT_MAX;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char *value = (char *)returnValue;

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
				break;

			moveToValueByAttrType( value, attr.type );
		}

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
	void* returnValue = malloc(PAGE_SIZE);
	int maxInt = INT_MIN;
	float maxFloat = FLT_MIN;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char *value = (char *)returnValue;

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
				break;

			moveToValueByAttrType( value, attr.type );
		}

		switch( aggAttr.type )
		{
			case TypeInt:
				if( *((int *)value) > maxInt )
					maxInt = *((int *)value);
				break;
			case TypeReal:
				if( *((float *)value) > maxFloat )
					maxFloat = *((float *)value);
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
	void* returnValue = malloc(PAGE_SIZE);
	int sumInt = 0;
	float sumFloat = 0;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char *value = (char *)returnValue;

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
				break;

			moveToValueByAttrType( value, attr.type );
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
	void* returnValue = malloc(PAGE_SIZE);
	int sumInt = 0;
	float sumFloat = 0;

	int countInt = 0;
	int countFloat  = 0;

	float avgInt = 0;
	float avgFloat = 0;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char *value = (char *)returnValue;

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
				break;

			moveToValueByAttrType( value, attr.type );
		}

		switch( aggAttr.type )
		{
			case TypeInt:
			{
				sumInt += *((int *) value );
				countInt++;
				break;
			}
			case TypeReal:
			{
				sumFloat += *((float *) value );
				countFloat++;
				break;
			}
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
		{
			avgInt = (float)sumInt / countInt;
			memcpy( data , &avgInt, sizeof(float));
			break;
		}
		case TypeReal:
		{
			avgFloat = sumFloat / countFloat;
			memcpy( data , &avgFloat, sizeof(float));
			break;
		}
		default:
			break;

	}

	free( returnValue );
}

void Aggregate::getCount_basic(void *data) {
	void* returnValue = malloc(PAGE_SIZE);
	int countInt = 0;
	int countFloat = 0;

	while( iterator->getNextTuple( returnValue ) != QE_EOF )
	{
		char *value = (char *)returnValue;

		for( Attribute attr: attributeVector )
		{
			if( attr.name == aggAttr.name )
				break;

			moveToValueByAttrType( value, attr.type );
		}

		switch( aggAttr.type )
		{
			case TypeInt:
				countInt ++;
				break;
			case TypeReal:
				countFloat ++;
				break;
			default:
				cout << "Type not supported" << endl;
		}
	}

	switch( aggAttr.type )
	{
		case TypeInt:
			memcpy( data , &countInt, sizeof(int));
			break;
		case TypeReal:
			memcpy( data , &countFloat, sizeof(int));
			break;
		default:
			break;

	}

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
			break;
		case COUNT:
			attribute.name = "COUNT(" + aggAttr.name + ")";
			break;
	}

	attrs.push_back(attribute);

};

void Aggregate::calculateMinForGroup()
{

}

void Aggregate::calculateMaxForGroup()
{

}

void Aggregate::calculateSumForGroup()
{

}

void Aggregate::calculateAvgForGroup()
{

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
	cout<<"Create partition "<<identityName<<endl;
	partitionOperator(leftIn, "left"+identityName, numPartitions,condition.lhsAttr);
	partitionOperator(rightIn, "right"+identityName, numPartitions, condition.rhsAttr);
	_rbfm->createFile("res"+identityName);

	// merge attribute
	cout<<"Merge attribute"<<endl;
	mergeAttrList.clear();
	for(Attribute iter1:leftAttrList){
		mergeAttrList.push_back(iter1);
	}
	for(Attribute iter1:rightAttrList){
		mergeAttrList.push_back(iter1);
	}

	// merge partition
	cout<<"Merge partition"<<endl;
	for(unsigned iter1 = 0; iter1<numPartitions; iter1++)
		mergePartition(iter1, identityName, leftAttrList, rightAttrList, condition, mergeAttrList);

	// initialize iterator on mergeResult
	vector<string> mergeAttrName;
	_rbfm->openFile("res"+identityName, resFileHandle);
	getAllAttrNames(mergeAttrList, mergeAttrName);
	resScaner.initialize(resFileHandle, mergeAttrList, "", NO_OP, NULL, mergeAttrName);
}

GHJoin::~GHJoin(){
	cout<<"destory"<<endl;
	for(unsigned iter1 = 0; iter1<numPartitions; iter1++){
		string partitionName = identityName+to_string(iter1);
		_rbfm->destroyFile("left"+partitionName);
		_rbfm->destroyFile("right"+partitionName);
		cout<<"destory "<<" left"+partitionName<<endl;
	}
	_rbfm->destroyFile("res"+identityName);
}

RC GHJoin::getAllAttrNames(vector<Attribute> attrList, vector<string> &attrNames){
	for(unsigned iter1 = 0; iter1<attrList.size(); iter1++)
		attrNames.push_back(attrList.at(iter1).name);
	return 0;
}

bool GHJoin::keyCompare(void *key1, void *key2, Attribute attr){
	bool res = true;
	if(attr.type == TypeInt)
		res = *(int*)key1 == *(int*)key2;
	else if (attr.type == TypeReal)
		res = *(float*)key1 == *(float*)key2;
	else{
		int cmp;
		int len1, len2;
		len1 = *(int*)key1;
		len2 = *(int*)key2;
		if(len1!=len2)
			res = false;
		else
			res = (memcmp(key1,key2,len1+sizeof(int)) == 0);
	}
	return res;
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
	selectAttribute(rightAttrList, condition.rhsAttr, comAttr);

	leftRbfmScanner.initialize(leftFileHandle, leftAttrList, "", NO_OP, NULL, leftAttrNames);
	estRecordSize = _rbfm->getEstimatedRecordDataSize(leftAttrList);
	recordData = malloc(estRecordSize);
	cout<<"Loading inMemorySet"<<endl;
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
			_rbfm->printRecord(leftAttrList, leftRecord);
			if(keyCompare(leftKey, rightKey, comAttr)){
				// cout<<"Key match : "<<comAttr.name<<endl;
				// cout<<"----------Record 2---------------"<<endl;
				_rbfm->printRecord(rightAttrList, recordData);
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
				_rbfm->printRecord(mergeAttrList, mergeData);
				free(mergeData);
			}
		}
	}

	_rbfm->closeFile(leftFileHandle);
	_rbfm->closeFile(rightFileHandle);
	_rbfm->closeFile(resFileHandle);

	free(leftKey);
	free(rightKey);
	return rc;
}


RC GHJoin:: selectAttribute(vector<Attribute> attrList, string attrName, Attribute &attr){
	int rc = -1;
	for(unsigned iter1 = 0; iter1 < attrList.size()&&rc==-1; iter1++)
		if(attrList.at(iter1).name == attrName){
			attr = attrList.at(iter1);
			rc = 0;
		}
	return rc;
}

void GHJoin::partitionOperator(Iterator *iter, string identityName, const unsigned numPartitions, string attrName){

	for(unsigned iter1 = 0; iter1<numPartitions; iter1++)
		_rbfm->createFile(identityName+to_string(iter1));

	vector<Attribute> attrList;
	Attribute keyAttr;
	iter->getAttributes(attrList);
	selectAttribute(attrList, attrName,keyAttr);

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
	return resScaner.getNextRecord(rid, data);
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
