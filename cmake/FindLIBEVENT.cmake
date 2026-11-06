# FindLIBEVENT.cmake — locate libevent and optionally its OpenSSL TLS
# extension. libevent provides no upstream CMake package, so this module
# is also installed next to bgraftConfig.cmake to keep find_package(bgraft)
# self-contained for consumers.
#
# Components:
#   OpenSSL   also locate libevent_openssl
#
# Result variables:
#   LIBEVENT_FOUND, LIBEVENT_INCLUDE_DIRS, LIBEVENT_LIBRARIES
# Imported targets:
#   LIBEVENT::LIBEVENT   always (when found)
#   LIBEVENT::OPENSSL    with the OpenSSL component
#
# Assumes shared libraries; for a static-only libevent build, set
# LIBEVENT_LIBRARY / LIBEVENT_OPENSSL_LIBRARY cache entries explicitly.

find_path(LIBEVENT_INCLUDE_DIR NAMES event2/event.h)
find_library(LIBEVENT_LIBRARY NAMES event libevent)

if(OpenSSL IN_LIST LIBEVENT_FIND_COMPONENTS)
  find_package(OpenSSL QUIET)
  find_library(LIBEVENT_OPENSSL_LIBRARY NAMES event_openssl libevent_openssl)
  set(LIBEVENT_OpenSSL_FOUND FALSE CACHE BOOL "libevent OpenSSL extension found"
                                             FORCE)
  if(LIBEVENT_OPENSSL_LIBRARY AND OpenSSL_FOUND)
    set(LIBEVENT_OpenSSL_FOUND TRUE CACHE BOOL "" FORCE)
  endif()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  LIBEVENT
  REQUIRED_VARS LIBEVENT_LIBRARY LIBEVENT_INCLUDE_DIR
  HANDLE_COMPONENTS)

mark_as_advanced(LIBEVENT_INCLUDE_DIR LIBEVENT_LIBRARY
                 LIBEVENT_OPENSSL_LIBRARY)

if(LIBEVENT_FOUND)
  set(LIBEVENT_INCLUDE_DIRS "${LIBEVENT_INCLUDE_DIR}")

  if(NOT TARGET LIBEVENT::LIBEVENT)
    add_library(LIBEVENT::LIBEVENT SHARED IMPORTED)
    set_target_properties(
      LIBEVENT::LIBEVENT
      PROPERTIES IMPORTED_LOCATION "${LIBEVENT_LIBRARY}"
                 INTERFACE_INCLUDE_DIRECTORIES "${LIBEVENT_INCLUDE_DIR}")
  endif()
  list(APPEND LIBEVENT_LIBRARIES LIBEVENT::LIBEVENT)

  if(LIBEVENT_OpenSSL_FOUND AND NOT TARGET LIBEVENT::OPENSSL)
    add_library(LIBEVENT::OPENSSL SHARED IMPORTED)
    set_target_properties(
      LIBEVENT::OPENSSL
      PROPERTIES
        IMPORTED_LOCATION "${LIBEVENT_OPENSSL_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${LIBEVENT_INCLUDE_DIR}"
        INTERFACE_LINK_LIBRARIES
          "LIBEVENT::LIBEVENT;OpenSSL::SSL;OpenSSL::Crypto")
  endif()
  if(LIBEVENT_OpenSSL_FOUND)
    list(APPEND LIBEVENT_LIBRARIES LIBEVENT::OPENSSL)
  endif()
endif()
