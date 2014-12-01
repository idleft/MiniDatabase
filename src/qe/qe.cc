
#include "qe.h"

/* Filter constructor */
Filter::Filter(Iterator* input, const Condition &condition) {
	/* Initialize variables */
	iterator = input;

	compOp = condition.op;

	type = condition.rhsValue.type;

	rhs_value = setValue(condition.rhsValue);

	iterator->getAttributes(attributeVector);

}


/* Filter destructor */
Filter::~Filter(){
}

RC Filter::getNextTuple(void *data) {
	RC rc = -1;

	do {
		rc = iterator->getNextTuple(data);
		if( rc != 0 )
			return QE_EOF;
	} while(!valueCompare(data));

	return 0;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
	iterator->getAttributes(attrs);
}

void Filter::setValue(Value rhsValue) {

	switch( type ) {
		case TypeInt:
			memcpy( (void*)rhs_value, rhsValue.data, sizeof(int) );
			break;
		case TypeReal:
			memcpy( (void*)rhs_value, rhsValue.data, sizeof(float) );
			break;
		case TypeVarChar:
			int length = *((int*)rhsValue.data);
			memcpy( (void*)rhs_value, rhsValue.data, sizeof(int) + length );
			break;
	}
}

bool Filter::valueCompare(void *data) {
	char *lhs_value = (char *)data;

	for ( Attribute attr : attributeVector ) {
		if ( attr.name == lhsAttr ) {
			break;
		}

		moveToValueByAttrType( lhs_value, attr.type );
	}

	switch( type )
	{
		case TypeInt:
			int lhs_int = *((int *)lhs_value);
			int rhs_int = *((int *)rhs_value);
			compareValueByAttrType( lhs_int, rhs_int, compOp );
			break;
		case TypeReal:
			float lhs_float = *((float *)lhs_value);
			float rhs_float = *((float *)rhs_value);
			compareValueByAttrType(lhs_float, rhs_float,compOp);
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

void Filter::moveToValueByAttrType(char* value, AttrType type) {

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
}

BNLJoin::BNLJoin(Iterator *leftIn,
        TableScan *rightIn,
        const Condition &condition,
        const unsigned numRecords) {

}

BNLJoin::~BNLJoin(){

};

RC BNLJoin::getNextTuple(void *data){
	return QE_EOF;
};

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{

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

Aggregate:: ~Aggregate(){

};

RC Aggregate::getNextTuple(void *data){
	return QE_EOF;
};

// Please name the output attribute as aggregateOp(aggAttr)
// E.g. Relation=rel, attribute=attr, aggregateOp=MAX
// output attrname = "MAX(rel.attr)"
void Aggregate::getAttributes(vector<Attribute> &attrs) const{


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
