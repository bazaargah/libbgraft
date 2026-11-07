add_library(bgraft)

target_sources(
  bgraft
  PRIVATE loop.cc
          log.cc
          node.cc
          protocol.cc
          state.cc
          transport.cc)

target_include_directories(
  bgraft PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../include>
               $<INSTALL_INTERFACE:include>)

target_link_libraries(
  bgraft
  PUBLIC $<BUILD_INTERFACE:bg::wire>
         $<BUILD_INTERFACE:LIBEVENT::LIBEVENT>
         $<BUILD_INTERFACE:LIBEVENT::OPENSSL>
         $<INSTALL_INTERFACE:LIBEVENT::LIBEVENT>
         $<INSTALL_INTERFACE:LIBEVENT::OPENSSL>)

target_compile_features(bgraft PUBLIC cxx_std_23)
target_compile_options(bgraft PRIVATE -Wall -Wextra -Wpedantic)

set_target_properties(
  bgraft
  PROPERTIES OUTPUT_NAME bgraft
             VERSION ${PROJECT_VERSION}
             SOVERSION ${PROJECT_VERSION_MAJOR}
             CXX_VISIBILITY_PRESET hidden
             VISIBILITY_INLINES_HIDDEN ON)

add_library(bg::raft ALIAS bgraft)

include(GNUInstallDirs)
install(TARGETS bgraft EXPORT bgraftTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../include/bg
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT bgraftTargets
        NAMESPACE BG::
        FILE bgraftTargets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/bgraft)

include(CMakePackageConfigHelpers)
configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/bgraftConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/bgraftConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/bgraft)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/bgraftConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/bgraftConfig.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/bgraftConfigVersion.cmake
              ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/FindLIBEVENT.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/bgraft)
