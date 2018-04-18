using MQTTnet;
using MQTTnet.Server;
using System;
using System.Threading.Tasks;

namespace TestServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Server");

            // Start a MQTT server.
            var mqttServer = new MqttFactory().CreateMqttServer();
            await mqttServer.StartAsync(new MqttServerOptions() { });
            Console.WriteLine("Press any key to exit.");
            Console.ReadLine();
            await mqttServer.StopAsync();
        }
    }
}
