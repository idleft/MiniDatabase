
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

Value Filter::setValue(Value rhsValue) {

	switch( type ) {
		case TypeInt:
			memcpy( rhs_value, rhsValue, sizeof(int) );
			break;
		case TypeReal:
			memcpy( rhs_value, rhsValue, sizeof(float) );
			break;
		case TypeVarChar:
			int length = *((int*)rhsValue);
			memcpy( rhs_value, rhsValue, sizeof(int) + length );
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
