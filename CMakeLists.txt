cmake_minimum_required( VERSION 2.8 )
project( versioning )
enable_testing()

file(GLOB Files 
    ${CMAKE_CURRENT_SOURCE_DIR}/metadata.txt
    ${CMAKE_CURRENT_SOURCE_DIR}/*.py 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.qrc 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.svg 
    ${CMAKE_CURRENT_SOURCE_DIR}/*.ui 
    )

foreach(File ${Files})
    get_filename_component(BaseName ${File} NAME)
    configure_file(${File} ${CMAKE_BINARY_DIR}/${BaseName} COPYONLY)
endforeach()

add_custom_command( OUTPUT commit_msg_ui.py 
    COMMAND pyuic4 -o commit_msg_ui.py commit_msg.ui
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/commit_msg.ui )

add_custom_target(qgis_plugin ALL DEPENDS commit_msg_ui.py ${Files})

add_test(issue287_test ../test_data/issue287_test.py)
add_test(issue358_test ../test_data/issue358_test.py)
add_test(issue357_test ../test_data/issue357_test.py)
add_test(issue437_test ../test_data/issue437_test.py)
add_test(history_creation_test ../test_data/history_creation_test.py)
add_test(postgres_working_copy_test ../test_data/posgres_working_copy_test.py)
add_test(posgres_working_copy_bug_in_view_test ../test_data/posgres_working_copy_bug_in_view_test.py)
add_test(posgres_working_copy_bug_in_conflict_test ../test_data/posgres_working_copy_bug_in_conflict_test.py)
add_test(history_creation_big_epanet_test ../test_data/history_creation_big_epanet_test.py)
