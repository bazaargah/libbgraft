add_executable(demo demo.cc)
target_link_libraries(demo PRIVATE bgraft)
target_compile_options(demo PRIVATE -Wall -Wextra)
set_target_properties(demo PROPERTIES FOLDER examples)
