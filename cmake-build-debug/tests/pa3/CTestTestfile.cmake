# CMake generated Testfile for 
# Source directory: /Users/chen/Desktop/cs660-pa/tests/pa3
# Build directory: /Users/chen/Desktop/cs660-pa/cmake-build-debug/tests/pa3
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[IntegerAggregatorTest.test]=] "/Users/chen/Desktop/cs660-pa/cmake-build-debug/tests/pa3/pa3_test" "--gtest_filter=IntegerAggregatorTest.test")
set_tests_properties([=[IntegerAggregatorTest.test]=] PROPERTIES  SKIP_REGULAR_EXPRESSION "\\[  SKIPPED \\]" TIMEOUT "5" _BACKTRACE_TRIPLES "/Applications/CLion.app/Contents/bin/cmake/mac/share/cmake-3.26/Modules/GoogleTest.cmake;402;add_test;/Users/chen/Desktop/cs660-pa/tests/pa3/CMakeLists.txt;13;gtest_add_tests;/Users/chen/Desktop/cs660-pa/tests/pa3/CMakeLists.txt;0;")
subdirs("../../_deps/googletest-build")
