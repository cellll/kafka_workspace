/*
Develop environment
    .NET 6.0
    OpenCvSharp4 --version 4.6.0.20220608
    Confluent.Kafka --version 1.9.3
    OpenCvSharp4.runtime.osx.10.15-x64 --version 4.6.0.20220608     osx -> ubuntu/centos/win/...  https://www.nuget.org/packages?q=opencvsharp4
    kafkacs.dll
*/

using System;
using System.Text.Json;
using OpenCvSharp;
using KafkaCSProducer;

namespace IncludeTest{
    class Program{
        static void Main(String[] args){
            /**
             * Kafka producer 메시지 전송 샘플 코드 
             * OpenCv Mat을 base64 string 으로 변환 후 메시지에 포함시켜 Produce
             * @version 0.1
             */
             
            String kafka_addr = "127.0.0.1:39092";
            String topic = "t2";

            /*
             * public Producer(string kafka_addr, string topic, bool createIfNotExist = false, int numPartition = 4, short replicationFactor = 1);
             *
             * @param string kafka_addr : Kafka broker address
             * @param string topic : kafka topic name
             * @param bool createIfNotExist : 토픽이 존재하지 않을 경우 생성
             * @param int numPartition : 토픽이 존재하지 않고 createIfNotExist가 true인 경우 생성하는 토픽의 partition 수
             * @param short replicationFactor : 토픽이 존재하지 않고 createIfNotExist가 true인 경우 생성하는 토픽의 replicationFactor
             *
             */
            Producer p = new Producer(kafka_addr, topic, true);

            /*
             * public (bool status, string message) GetStatus();
             *
             * @return bool status : Kafka producer 정상 생성시 true
             * @return string msg : Kafka producer status가 false인 경우 reason
             *
             */
            (bool status, string msg) = p.GetStatus();
            Console.WriteLine($"Producer status : {status}");
            Console.WriteLine($"{msg}");

            if (status){
                string filepath = "/Users/ce/Desktop/Data/sample/arsenal.jpg";
                /*
                 * OpenCv Mat 
                 */
                Mat img = Cv2.ImRead(filepath, ImreadModes.Color);
                string now = DateTime.Now.ToString("yyyy-MM-dd %H:%m:%s");

                /*
                 * public string MatToB64String(Mat mat, int jpegQuality=70);
                 *
                 * @param Mat mat
                 * @param int jpegQuality : JPEG 압축 quality
                 * @return string imgb64 : mat을 변환한 base64 string
                 */
                string imgb64 = p.MatToB64String(img);

                /*
                 *  headers : 전송할 메시지의 헤더가 필요한 경우 Dictionary<string, string>으로 생성
                 */
                var headers = new Dictionary<string, string>(){
                    {"created", now},
                    {"owner", "me"}
                };

                /*
                 * message : Dictionary<string, string> 형태의 메시지를 JSON 직렬화
                 */
                var message = JsonSerializer.Serialize(
                    new Dictionary<string, string>(){
                        {"image", imgb64},
                        {"filepath", filepath},
                        {"width", img.Rows.ToString()},
                        {"height", img.Cols.ToString()},
                    }
                );

                /*
                 * public void Produce(string msg, string? key, Dictionary<string, string> headersDict = null);
                 *
                 * @param string msg : 전송할 메시지
                 * @param string? key : 전송할 메시지의 key값이 필요한 경우 입력, 생략 시 null
                 * @param headersDict : headers Dictionary 생성한 경우 전달, 생략 시 null
                 *
                 */
                p.Produce(message, null, headers);
            }
        }
    }
}
