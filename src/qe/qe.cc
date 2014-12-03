
#include "qe.h"

/* Filter constructor */
Filter::Filter(Iterator* input, const Condition &condition) {
	/* Initialize variables */
	iterator = input;

	compOp = condition.op;

	type = condition.rhsValue.type;

	setValue(condition.rhsValue);

	iterator->getAttributes(attributeVector);

//	cout << "Filter initialization complete" << endl;

}

RC Filter::getNextTuple(void *data) {

//	cout << "Filter getNextTuple Start " << endl;
	RC rc = -1;

	do {
		rc = iterator->getNextTuple(data);
		if( rc != 0 )
			return QE_EOF;
	} while(!valueCompare(data));

//	cout << "Filter getNextTuple End" << endl;
	return 0;
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

	for ( Attribute attr : attributeVector ) {
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

Project::Project(Iterator *input,                    // Iterator of input R
        const vector<string> &attrNames)
{
	if( attrNames.empty() )
		cout << "Attribute name vector is empty" << endl;

	iterator = input;

	iterator->getAttributes( attributeVector );

	/* construct unordered set to save only unique attribute name */
	for( string name: attrNames )
	{
		attributeSet.emplace(name);
	}

	for( Attribute attr: attributeVector )
	{
		if( attributeSet.count(attr.name) > 0 )
			attributeVectorForProjection.push_back( attr );
	}
}

RC Project::getNextTuple(void *data) {

	RC rc = -1;

	rc = iterator->getNextTuple( retrievedData );
	if( rc != 0 )
		return rc;

	char* dest = (char*) retrievedData;
	for( Attribute attr: attributeVectorForProjection )
	{
		if( attributeSet.count(attr.name) > 0 )
		{
			copyValue( data, dest, attr.type );		// update data with returned result

			moveToValueByAttrType( (char*)data, attr.type );	// go to value
		}

		moveToValueByAttrType( dest, attr.type );		// go to value
	}

	return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = attributeVectorForProjection;
}

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
			RC rc;
			switch( this->op )
			{
				case MIN:
					return rc;
					break;
				case MAX:
					return rc;
					break;
				case SUM:
					return rc;
					break;
				case AVG:
					return rc;
					break;
				case COUNT:
					return rc;
					break;
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
