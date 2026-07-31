pulselog_add_test(test_base SOURCES test_status.cc test_crc32c.cc test_buffer.cc test_config.cc
                  DEPS pulselog::base)
pulselog_add_test(test_concurrency SOURCES test_queues.cc DEPS pulselog::concurrency)
pulselog_add_test(test_protocol SOURCES test_frame.cc test_record.cc test_messages.cc
                  DEPS pulselog::protocol)
