add_library_unity(test_catalog OBJECT test_catalog_version.cpp
                  test_bitmap_join_meta.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_catalog>
    PARENT_SCOPE)
