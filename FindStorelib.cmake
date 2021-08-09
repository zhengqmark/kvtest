#
# find the storelib library and set up an imported target for it
#

#
# inputs:
#   - STORELIB_INCLUDE_DIR: hint for finding store_lib_exp.h
#   - STORELIB_LIBRARY_DIR: hint for finding the storelib lib
#
# output:
#   - "storelib" library target
#   - STORELIB_FOUND  (set if found)
#

include (FindPackageHandleStandardArgs)

find_path (STORELIB_INCLUDE store_lib_exp.h HINTS ${STORELIB_INCLUDE_DIR})
find_library (STORELIB_LIBRARY storelib HINTS ${STORELIB_LIBRARY_DIR})

find_package_handle_standard_args (Storelib DEFAULT_MSG
        STORELIB_INCLUDE STORELIB_LIBRARY)

mark_as_advanced (STORELIB_INCLUDE STORELIB_LIBRARY)

if (STORELIB_FOUND AND NOT TARGET storelib)
    add_library (storelib UNKNOWN IMPORTED)
    set_target_properties (storelib PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${STORELIB_INCLUDE}")
    set_property (TARGET storelib APPEND PROPERTY
            IMPORTED_LOCATION "${STORELIB_LIBRARY}")
endif ()
