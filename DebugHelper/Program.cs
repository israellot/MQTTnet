using MQTTnet.Serializer;
using System;

namespace DebugHelper
{
    class Program
    {
        static void Main(string[] args)
        {
            var serializer = new MqttPacketSerializer();

            var stateMachine = new MqttPacketStateMachine(serializer);

            stateMachine.OnPacket(p => { });

            var packets = new[]
            {
                "UAIAew==",
                "Ow4ABUEvQi9DAHtIRUxMTw=="
            };

            foreach (var p in packets)
            {
                foreach (var b in Convert.FromBase64String(p))
                {
                    stateMachine.Push(new[] { b });
                }

                var a = 1;
            }

            foreach (var p in packets)
            {
                stateMachine.Push(Convert.FromBase64String(p));
            }

           
        }
    }
}
