using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MQTTnet.Adapter;
using MQTTnet.Diagnostics;
using MQTTnet.Exceptions;
using MQTTnet.Packets;
using MQTTnet.Protocol;

namespace MQTTnet.Server
{
    public sealed class MqttClientPendingMessagesQueue : IDisposable
    {
        private readonly BufferBlock<MqttBasePacket> _queue = new BufferBlock<MqttBasePacket>();
        private readonly IMqttServerOptions _options;
        private readonly MqttClientSession _clientSession;
        private readonly IMqttNetLogger _logger;

        private Task _workerTask;

        public MqttClientPendingMessagesQueue(IMqttServerOptions options, MqttClientSession clientSession, IMqttNetLogger logger)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _clientSession = clientSession ?? throw new ArgumentNullException(nameof(clientSession));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public int Count => _queue.Count;

        public void Start(IMqttChannelAdapter adapter, CancellationToken cancellationToken)
        {
            if (adapter == null) throw new ArgumentNullException(nameof(adapter));

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            _workerTask = Task.Run(() => SendQueuedPacketsAsync(adapter, cancellationToken), cancellationToken);
        }

        public void WaitForCompletion()
        {
            if (_workerTask != null)
            {
                Task.WaitAll(_workerTask);
            }
        }

        public void Enqueue(MqttBasePacket packet)
        {
            if (packet == null) throw new ArgumentNullException(nameof(packet));

            _queue.Post(packet);

            _logger.Verbose<MqttClientPendingMessagesQueue>("Enqueued packet (ClientId: {0}).", _clientSession.ClientId);
        }

        private async Task SendQueuedPacketsAsync(IMqttChannelAdapter adapter, CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await SendNextQueuedPacketAsync(adapter, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception exception)
            {
                _logger.Error<MqttClientPendingMessagesQueue>(exception, "Unhandled exception while sending enqueued packet (ClientId: {0}).", _clientSession.ClientId);
            }
        }

        private async Task SendNextQueuedPacketAsync(IMqttChannelAdapter adapter, CancellationToken cancellationToken)
        {
            MqttBasePacket packet = null;
            try
            {
                packet = await _queue.ReceiveAsync(cancellationToken);

                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                await adapter.SendPacketAsync(_options.DefaultCommunicationTimeout, cancellationToken, packet).ConfigureAwait(false);

                _logger.Verbose<MqttClientPendingMessagesQueue>("Enqueued packet sent (ClientId: {0}).", _clientSession.ClientId);
            }
            catch (Exception exception)
            {
                if (exception is MqttCommunicationTimedOutException)
                {
                    _logger.Warning<MqttClientPendingMessagesQueue>(exception, "Sending publish packet failed due to timeout (ClientId: {0}).", _clientSession.ClientId);
                }
                else if (exception is MqttCommunicationException)
                {
                    _logger.Warning<MqttClientPendingMessagesQueue>(exception, "Sending publish packet failed due to communication exception (ClientId: {0}).", _clientSession.ClientId);
                }
                else if (exception is OperationCanceledException)
                {
                }
                else
                {
                    _logger.Error<MqttClientPendingMessagesQueue>(exception, "Sending publish packet failed (ClientId: {0}).", _clientSession.ClientId);
                }

                if (packet is MqttPublishPacket publishPacket)
                {
                    if (publishPacket.QualityOfServiceLevel > MqttQualityOfServiceLevel.AtMostOnce)
                    {
                        publishPacket.Dup = true;
                        _queue.Post(packet);
                    }
                }

                if (!cancellationToken.IsCancellationRequested)
                {
                    await _clientSession.StopAsync().ConfigureAwait(false);
                }
            }
        }

        public void Dispose()
        {
        }
    }
}
