#ifndef _qe_h_
#define _qe_h_

#include <vector>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#include <climits>
#include <float.h>
#include <unordered_set>
#include <unordered_map>

# define QE_EOF (-1)  // end of the index scan

using namespace std;

#define QE_ATTRIBUTE_NOT_SUPPORTED 20
#define QE_NOT_FOUND 21
#define QE_FAIL_TO_FIND_ATTRIBUTE 22

#define AGGREGATION_BASIC 40
#define AGGREGATION_GROUP 41

typedef enum{ MIN = 0, MAX, SUM, AVG, COUNT } AggregateOp;


// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};

void moveToValueByAttrType(char* value, AttrType type);
template <typename T>
bool compareValueByAttrType( T const lhs_value, T const rhs_value, CompOp compOp);
void copyValue( void* dest, const void* src, AttrType attrType );

class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs[i].name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator()
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
//            cout << "iter->getNextEntry:" << rc << endl;
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
//                cout << "rm.readTuple[" << tableName.c_str()<< "]=" << rc << endl;
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs[i].name;
                attrs[i].name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

        bool valueCompare(void *data);
        void setValue(Value rhsValue);

    private:
        Attribute selectAttr;
        Iterator *iterator;
        vector<Attribute> attrList;
        Condition condition;
        RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();

};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator* iterator;
        vector<string> attrNames;

        vector<Attribute> attrList;
        vector<Attribute> selectedAttrList;

        RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
};

class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      );
      ~GHJoin();

      RC getNextTuple(void *data);
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const;
      void partitionOperator(Iterator *iter, string identityName, const unsigned numPartitions, string attrName);
      RC mergePartition(Iterator *iter1, string identityName);
      RC getAllAttrNames(vector<Attribute> attrList, vector<string> &attrNames);
      RC mergePartition(int iter1, string identityName, vector<Attribute> leftAttrList,
      							vector<Attribute> rightAttrList, Condition condition, vector<Attribute>mergeAttrList);
      vector<Attribute> mergeAttrList;
      vector<Attribute> leftAttrList;
      vector<Attribute> rightAttrList;

      private:
      	  RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
      	  IndexManager *_ix = IndexManager::instance();
          unsigned numPartitions;
          string identityName;
      	  Condition condition;
      	  RBFM_ScanIterator resScaner;
      	  FileHandle resFileHandle;
          bool nonNull;
};


class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin();

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

        RC loadBlockRecords();
        void emptyBlockList();

        Condition condition;
        Iterator *leftIn;
        TableScan *rightIn;
        unsigned numRecords, curInblockP,curBlockListSize;
        vector<void *> blockRecordList;
        RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
        int leftRecordSize, rightRecordSize;
        vector<Attribute> mergeAttrList;
        vector<Attribute> leftAttrList;
        vector<Attribute> rightAttrList;
        Attribute comAttr;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

    private:
        Iterator *leftIn;
        IndexScan *rightIn;

        vector<Attribute> leftAttrList;
        vector<Attribute> rightAttrList;
        vector<Attribute> mergeAttrList;

        Attribute comAttr;
        Condition condition;
        int rightRecordSize;
        short leftSize;
        int leftRecordSize;
        RC leftRC,rightRC;

        void* leftRecord;
        void* rightRecord;
        void *leftKey;

        RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();

};


class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op,              // Aggregate operation
                  const unsigned numPartitions // Number of partitions for input (decided by the optimizer)
        );
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

        // Group-based hash aggregation
        void calculateMinForGroup();
        void calculateMaxForGroup();
        void calculateSumForGroup();
        void calculateAvgForGroup();
        void calculateCountForGroup();

        // getters
        void getNextTuple_basic(void *data);
        void getMin_basic(void *data);
        void getMax_basic(void *data);
        void getSum_basic(void *data);
        void getAvg_basic(void *data);
        void getCount_basic(void *data);

        // caculate group value
        RC getNextTuple_groupAvg(void *data);
        RC getNextTuple_groupCount(void *data);
        RC getNextTuple_groupMaxMinSum(void *data);

        /* value type, aggregate value type */
        unordered_map<int, int> groupmap_int_int;
        unordered_map<int, float> groupmap_int_float;
        unordered_map<int, string> groupmap_int_string;

        unordered_map<float, float> groupmap_float_float;
        unordered_map<float, int> groupmap_float_int;

        unordered_map<string, float> groupmap_string_float;
        unordered_map<string, int> groupmap_string_int;

        char str[PAGE_SIZE];

        template <typename GR, typename AGG>
		void groupMin(unordered_map<GR, AGG> &map, const GR &gr, const AGG& agg) {
			if (map.count(gr) == 0) {
				map[gr] = agg;
			} else if (map[gr] > agg){
				map[gr] = agg;
			}
		}

        template <typename GR, typename AGG>
		void groupMax(unordered_map<GR, AGG> &map, const GR &gr, const AGG& agg) {
			if (map.count(gr) == 0) {
				map[gr] = agg;
			} else if (map[gr] < agg){
				map[gr] = agg;
			}
		}

        template <typename GR>
		void groupAvg(unordered_map<GR, float> &map_sum,
				unordered_map<GR, int> &map_count,
				const GR &gr, float &agg) {
			if (map_count.count(gr) == 0) {
				map_count[gr] = 1;
			} else {
				map_count[gr] = map_count[gr] + 1;
			}
			if (map_sum.count(gr) == 0) {
				map_sum[gr] = (float)agg;
			} else {
				map_sum[gr] = map_sum[gr] + (float)agg;
			}

//			cout << "map_sum[gr]" << map_sum[gr] << endl;
		}

     	template <typename GR, typename AGG>
		void groupSum(unordered_map<GR, AGG> &map, const GR &gr, const AGG& agg) {
			if (map.count(gr) == 0) {
				map[gr] = agg;
			} else {
				map[gr] = map[gr] + agg;
			}
		}

     	template <typename GR>
		void groupCount(unordered_map<GR, int> &map, const GR &gr) {
			if (map.count(gr) == 0) {
				map[gr] = 1;
			} else {
				map[gr] = map[gr] + 1;
			}
		}

    private:
        Iterator	*iterator;
        vector<Attribute> attributeVector;
        Attribute aggAttr;
        Attribute groupAttr;
        AggregateOp op;
        int typeOfAggregation;
        unsigned numPartitions;
        bool status;
//        char returnValue[PAGE_SIZE];

        RecordBasedFileManager *_rbfm = RecordBasedFileManager::instance();
};

#endif
