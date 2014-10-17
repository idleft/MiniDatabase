#include "test_util.h"
int main()
{
    cout << endl << "Test Insert/Read Tuple .." << endl;

    // Insert/Read Tuple
    TEST_RM_1("tbl_employee", 6, "Peters", 24, 170.1, 5000);

    return 0;
}

int main()
{
    cout << endl << "Test Delete Tuple .." << endl;

    // Delete Tuple
    TEST_RM_2("tbl_employee", 6, "Victor", 22, 180.2, 6000);

    return 0;
}
int main()
{
    cout << endl << "Test Update Tuple .." << endl;

    // Update Tuple
    TEST_RM_3("tbl_employee", 6, "Thomas", 28, 187.3, 4000);

    return 0;
}
