pulselog_add_test(test_base SOURCES test_status.cc test_crc32c.cc test_buffer.cc test_config.cc
                  DEPS pulselog::base)
pulselog_add_test(test_concurrency SOURCES test_queues.cc DEPS pulselog::concurrency)
