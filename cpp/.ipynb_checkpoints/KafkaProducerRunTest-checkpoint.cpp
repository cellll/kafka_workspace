#include "KafkaProducer.h"

static volatile sig_atomic_t run = 1;
static void sigterm(int sig) {
    run = 0;
}

int main(){

    std::string brokers = "127.0.0.1:39092";
    std::string topic = "ctest";
    XProducer xp = XProducer(brokers, topic);
    xp.print();

    for (std::string message; run && std::getline(std::cin, message);){
        if (message.empty()){
            continue;
        }
        xp.produce(message);
    }

    return 0;

}