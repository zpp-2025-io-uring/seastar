add_custom_target (test_unit_asymmetric_backend
  COMMAND ${CMAKE_COMMAND} -E env ${Seastar_TEST_ENVIRONMENT}
    pytest ${CMAKE_CURRENT_SOURCE_DIR}/asymmetric_backend_test.py
    --seastar-path $<TARGET_FILE:shard_reporter>
    --strace-path strace
    --grammar-path ${CMAKE_CURRENT_SOURCE_DIR}/grammar.lark
  USES_TERMINAL)

add_executable (shard_reporter
  shard_reporter.cc)
target_link_libraries (shard_reporter
  PRIVATE
    seastar_private
    yaml-cpp::yaml-cpp)
add_dependencies (test_unit_asymmetric_backend shard_reporter)

add_test (
  NAME Seastar.unit.asymmetric_backend
  COMMAND ${CMAKE_COMMAND} --build ${Seastar_BINARY_DIR} --target test_unit_asymmetric_backend)
set_tests_properties (Seastar.unit.asymmetric_backend
  PROPERTIES
    TIMEOUT ${Seastar_TEST_TIMEOUT})
