/* compile with 
 * g++ -shared -o spatialite_functions.so -fPIC spatialite_functions.cpp -Wall -O2 -lsqlite3
 *
 * load with
 * SELECT load_extension('./spatialite_functions.so','wtavg_init');
 */

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1;

#include <iostream>
#include <cassert>

namespace vs 
{

struct Graph
{
    Graph(){
        std::cout << __PRETTY_FUNCTION__ << "\n";
    }
    ~Graph(){
        std::cout << __PRETTY_FUNCTION__ << "\n";
    }
};

void leaf( sqlite3_context *ctx, int num_values, sqlite3_value **values )
{
    std::cout << "value " << sqlite3_value_int( values[0] ) <<" "
       << ( SQLITE_NULL == sqlite3_value_type( values[1] ) ? "null" : "not null") << "\n";

    sqlite3 * h = sqlite3_context_db_handle( ctx );

    int sqlite3_exec( h,
            "SELECT * FROM "
  const char *sql,                           /* SQL to be evaluated */
  int (*callback)(void*,int,char**,char**),  /* Callback function */
  void *,                                    /* 1st argument to callback */
  char **errmsg                              /* Error msg written here */
);
    sqlite3_result_int( ctx, 1 );
}

}

extern "C" {
int vs_init( sqlite3 *db, char **error, const sqlite3_api_routines *api )
{
    SQLITE_EXTENSION_INIT2(api);

    sqlite3_create_function( db, "vsleaf", 2, SQLITE_UTF8,
            NULL, vs::leaf, NULL, NULL );

    return SQLITE_OK;
}
}

