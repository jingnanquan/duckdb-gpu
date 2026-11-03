set(TEST_MICROBENCH_OBJECTS
    join_microbench.cpp
    test_join_microbench.cpp)

add_library(test_microbench OBJECT ${TEST_MICROBENCH_OBJECTS})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_microbench>
    PARENT_SCOPE)
