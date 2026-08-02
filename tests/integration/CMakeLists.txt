pulselog_add_test(test_integration_single_broker SOURCES test_single_broker.cc
                  DEPS pulselog::broker pulselog::client)
pulselog_add_test(test_integration_replication SOURCES test_replication.cc
                  DEPS pulselog::broker pulselog::client)
