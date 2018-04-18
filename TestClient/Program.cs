using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Diagnostics;
using MQTTnet.ManagedClient;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TestClient
{
    class Program
    {
        static async Task<IMqttClient> NewClient(string clientId)
        {
            // Create a new MQTT client.
            var factory = new MqttFactory();
            var mqttClient = factory.CreateManagedMqttClient();



            // Create TCP based options using the builder.
            var clientOptions = new MqttClientOptionsBuilder()
                .WithClientId(clientId)
                .WithTcpServer("localhost")
                .WithCredentials("test", "test")
                .WithCleanSession()
                .Build();

            var managedClientOptions = new ManagedMqttClientOptionsBuilder()
                .WithClientOptions(clientOptions)
                .WithAutoReconnectDelay(TimeSpan.FromSeconds(5))
                .Build();

            //await mqttClient.StartAsync(managedClientOptions);

            var rawMqttClient = factory.CreateMqttClient();
            var r = await rawMqttClient.ConnectAsync(clientOptions);

           

            return rawMqttClient;
        }

        static long messageSentCount = 0;
        static long messageReceivedCount = 0;
        static long messageProcessedCount = 0;
        static double totalTime = 0;


        static object syncObject = new object();

        struct TestMessage
        {
            long Ticks { get; set; }

        }

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Client");
            Console.ReadKey();

            //MqttNetGlobalLogger.LogMessagePublished += (s, e) => { Debug.WriteLine($"{e.TraceMessage.Level}: {e.TraceMessage.Message}"); };

            var client1 = await NewClient("client1");

            await client1.SubscribeAsync("a/b/c");

            client1.ApplicationMessageReceived += MqttClient_ApplicationMessageReceived;

            ObjectPool<MqttApplicationMessage> messagePool = new ObjectPool<MqttApplicationMessage>(() => new MqttApplicationMessage()
            {
                QualityOfServiceLevel = MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
                Retain = false,
                Topic = "a/b/c"
            });

            var range = Enumerable.Range(0, 1);

            var delay = 0;

            foreach (var c in range)
            {
                Task.Run(async () =>
                {
                    var _client = await NewClient(Guid.NewGuid().ToString());

                    //_client.ApplicationMessageProcessed += (s, e) => {
                    //    messagePool.PutObject(e.ApplicationMessage);
                    //    Interlocked.Increment(ref messageProcessedCount);
                    //};

                    var overload = false;                    

                    while (true)
                    {
                        var message = messagePool.GetObject();
                        message.Payload = BitConverter.GetBytes(DateTimeOffset.UtcNow.Ticks);
                        await _client.PublishAsync(message);
                        Interlocked.Increment(ref messageSentCount);

                        if (messageSentCount - messageReceivedCount > 1000)
                        {
                            if (!overload)
                            {
                                overload = true;

                                delay += 1;
                                delay = delay > 0 ? delay : 0;
                            }
                            SpinWait.SpinUntil(() => messageSentCount - messageReceivedCount < 200, delay);
                        }
                        else
                        {
                            if (overload)
                            {
                                overload = false;
                                //not overloading anymore
                                delay -= 1;
                            }
                        }
                    }

                }).ConfigureAwait(false);
                
            }

         

            Stopwatch sw = Stopwatch.StartNew();
            while (true)
            {

                sw.Restart();

                await Task.Delay(5000);
                var localTotalTime = Interlocked.Exchange(ref totalTime, 0);
                var localMessageReceivedCount = Interlocked.Exchange(ref messageReceivedCount, 0);
                var localMessageSentCount = Interlocked.Exchange(ref messageSentCount, messageSentCount- localMessageReceivedCount);
                
                var elapsedTime = sw.Elapsed.TotalMilliseconds;

                Console.WriteLine($"{ localMessageReceivedCount / 1000.0:f2} K requests , {elapsedTime:f2} ms");
                Console.WriteLine($"{ (double)localMessageReceivedCount / 1000.0 / elapsedTime * 1000:f2} K req/s");
                Console.WriteLine($"{ localTotalTime / (double)localMessageReceivedCount:f2} ms/rq");
                Console.WriteLine($"Backlog : {localMessageSentCount - localMessageReceivedCount} reqs");
                Console.WriteLine($"Delay : {delay} ms");


                Console.WriteLine();

            }
            
        }

        private static void MqttClient_ApplicationMessageReceived(object sender, MqttApplicationMessageReceivedEventArgs e)
        {
            var ticks = BitConverter.ToInt64(e.ApplicationMessage.Payload, 0);

            var diff = DateTimeOffset.UtcNow.Ticks - ticks;

            Interlocked.Increment(ref messageReceivedCount);

            lock (syncObject)
            {
                totalTime += TimeSpan.FromTicks(diff).TotalMilliseconds;
            }
        }
    }

    public class ObjectPool<T>
    {
        private ConcurrentBag<T> _objects;
        private Func<T> _objectGenerator;

        public ObjectPool(Func<T> objectGenerator)
        {
            if (objectGenerator == null) throw new ArgumentNullException("objectGenerator");
            _objects = new ConcurrentBag<T>();
            _objectGenerator = objectGenerator;
        }

        public T GetObject()
        {
            T item;
            if (_objects.TryTake(out item)) return item;
            return _objectGenerator();
        }

        public void PutObject(T item)
        {
            _objects.Add(item);
        }
    }
}
