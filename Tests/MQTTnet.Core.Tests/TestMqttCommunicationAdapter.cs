﻿using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using MQTTnet.Adapter;
using MQTTnet.Packets;
using MQTTnet.Serializer;

namespace MQTTnet.Core.Tests
{
    public class TestMqttCommunicationAdapter : IMqttChannelAdapter
    {
        private readonly BlockingCollection<MqttBasePacket> _incomingPackets = new BlockingCollection<MqttBasePacket>();

        public TestMqttCommunicationAdapter Partner { get; set; }

        public IMqttPacketSerializer PacketSerializer { get; } = new MqttPacketSerializer();

        public Func<MqttBasePacket, Task> OnPacketHandler { get; set; }

        public void Dispose()
        {
        }

        public Task ConnectAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        public Task DisconnectAsync(TimeSpan timeout)
        {
            return Task.FromResult(0);
        }

        public Task SendPacketsAsync(TimeSpan timeout, CancellationToken cancellationToken, MqttBasePacket[] packets)
        {
            ThrowIfPartnerIsNull();

            foreach (var packet in packets)
            {
                SendPacketAsync(timeout, cancellationToken, packet);
            }

            return Task.FromResult(0);
        }

        public Task SendPacketAsync(TimeSpan timeout, CancellationToken cancellationToken, MqttBasePacket packet)
        {
            ThrowIfPartnerIsNull();

            Partner.EnqueuePacketInternal(packet);

            return Task.FromResult(0);
        }

        public Task<MqttBasePacket> ReceivePacketAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            ThrowIfPartnerIsNull();

            return Task.Run(() =>
            {
                try
                {
                    return _incomingPackets.Take(cancellationToken);
                }
                catch
                {
                    return null;
                }
            }, cancellationToken);
        }

        private void EnqueuePacketInternal(MqttBasePacket packet)
        {
            if (packet == null) throw new ArgumentNullException(nameof(packet));

            if (OnPacketHandler != null)
            {
                OnPacketHandler(packet);
            }
            else
            {
                _incomingPackets.Add(packet);
            }
        }

        private void ThrowIfPartnerIsNull()
        {
            if (Partner == null)
            {
                throw new InvalidOperationException("Partner is not set.");
            }
        }
    }
}
