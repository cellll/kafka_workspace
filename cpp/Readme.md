### Install
https://github.com/edenhill/librdkafka


<br>
On Mac OSX, install librdkafka with homebrew:

$ brew install librdkafka
On Debian and Ubuntu, install librdkafka from the Confluent APT repositories, see instructions here and then install librdkafka:

$ apt install librdkafka-dev
On RedHat, CentOS, Fedora, install librdkafka from the Confluent YUM repositories, instructions here and then install librdkafka:

$ yum install librdkafka-devel



<br>


### Run
<br>
g++ KafkaProducerRunTest.cpp KafkaProducer.cpp -lrdkafka++ -o producer && ./producer