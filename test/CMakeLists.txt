file(GLOB tests ${CMAKE_CURRENT_SOURCE_DIR}/*.py)

foreach(test ${tests})
    add_test( ${test} ${test})
endforeach()
