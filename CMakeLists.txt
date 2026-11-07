cmake_minimum_required(VERSION 3.30)

project(libbgraft VERSION 0.1.1 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(BG_RAFT_BUILD_TESTS "Build libbgraft tests" ON)
option(BG_RAFT_BUILD_EXAMPLES "Build libbgraft examples" ON)
option(BG_RAFT_SANITIZERS "Enable address and undefined behavior sanitizers" OFF)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(LIBEVENT REQUIRED COMPONENTS OpenSSL)

enable_testing()

# libbgwire — ecosystem wire primitives (CRC-32, big-endian helpers,
# bounds-checked Cursor), consumed exclusively through FetchContent at a
# pinned tag (the repo is public, so CI runners need no credentials).
# For offline/co-development use CMake's standard override:
# -DFETCHCONTENT_SOURCE_DIR_BGWIRE=/path/to/libbgwire
include(FetchContent)
FetchContent_Declare(bgwire
  GIT_REPOSITORY https://github.com/bazaargah/libbgwire.git
  GIT_TAG v0.1.1
  GIT_SHALLOW TRUE
  SYSTEM)
FetchContent_MakeAvailable(bgwire)

if(BG_RAFT_SANITIZERS)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

add_subdirectory(src)

if(BG_RAFT_BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BG_RAFT_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
