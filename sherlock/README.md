# Demo calling Shelock via Kafka

sherlock_test.py is a test client that reads n (default 10) alerts from the ZTF test topic. For each alert it decodes the AVRO, throws away the images and republishes the reduced alert on the dev_sherlock_test_input topic. It then listens for output on the dev_sherlock_test_output topic.

sherlock_wrapper.py is a test server that listens on the dev_sherlock_test_input topic and for each message queries sherlock, adds any attributes in the response to the alert and republishes the alert on dev_sherlock_test_output.

mock_sherlock_driver.py is based on the interface in lasair/src/sherlock_web_wrapper/server/sherlock_driver.py but simply adds dec and ra and returns them as the attribute 'sum'. It also allows using named arguments to pass input and output as python dictionaries rather than json in order to avoid pointless serialisation/deserialisation.

