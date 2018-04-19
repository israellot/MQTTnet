using System;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using MQTTnet.Channel;
using MQTTnet.Diagnostics;
using MQTTnet.Exceptions;
using MQTTnet.Packets;
using MQTTnet.Serializer;

namespace MQTTnet.Adapter
{
    public sealed class MqttChannelAdapter : IMqttChannelAdapter
    {
        private const uint ErrorOperationAborted = 0x800703E3;
        private const int ReadBufferSize = 4096;  // TODO: Move buffer size to config

        private bool _isDisposed;
        private readonly IMqttNetLogger _logger;
        private readonly IMqttChannel _channel;
        private MqttPacketStateMachine _packetStateMachine;

        private BufferBlock<MqttBasePacket> _receiveBuffer = new BufferBlock<MqttBasePacket>();
        private CancellationTokenSource _receiveTaskCts = new CancellationTokenSource();
        
        private Func<MqttBasePacket, Task> _onPacketHandler;

        private Boolean _onPacketHandlerDelegated;

        public Func<MqttBasePacket, Task> OnPacketHandler { get
            {
                return _onPacketHandler;
            }
            set
            {
                if (value != null)
                {
                    _onPacketHandler = value;
                    _packetStateMachine.OnPacketHandler = value;
                    _onPacketHandlerDelegated = true;

                    //send any buffered packet
                    while(_receiveBuffer.TryReceive(out var packet))
                    {
                        _onPacketHandler(packet);
                    }
                }
                else
                {
                    _packetStateMachine.OnPacketHandler = this.InternalOnPacketHandler;
                    _onPacketHandlerDelegated = false;
                }
                
            }
        }

        public MqttChannelAdapter(IMqttChannel channel, IMqttPacketSerializer serializer, IMqttNetLogger logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _channel = channel ?? throw new ArgumentNullException(nameof(channel));
            PacketSerializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _packetStateMachine = new MqttPacketStateMachine(PacketSerializer);

            _packetStateMachine.OnPacketHandler = this.InternalOnPacketHandler;

            DataIngestionTask().ConfigureAwait(false);
        }

        public IMqttPacketSerializer PacketSerializer { get; }

        private Task InternalOnPacketHandler(MqttBasePacket packet)
        {
            _receiveBuffer.Post(packet);
            return Task.FromResult(0);
        }

        public Task ConnectAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();
            _logger.Verbose<MqttChannelAdapter>("Connecting [Timeout={0}]", timeout);

            return ExecuteAndWrapExceptionAsync(() =>
                Internal.TaskExtensions.TimeoutAfter(ct => _channel.ConnectAsync(ct), timeout, cancellationToken));
        }

        public Task DisconnectAsync(TimeSpan timeout)
        {
            ThrowIfDisposed();
            _logger.Verbose<MqttChannelAdapter>("Disconnecting [Timeout={0}]", timeout);

            return ExecuteAndWrapExceptionAsync(() =>
                Internal.TaskExtensions.TimeoutAfter(ct => _channel.DisconnectAsync(), timeout, CancellationToken.None));
        }

        public async Task SendPacketsAsync(TimeSpan timeout, CancellationToken cancellationToken, MqttBasePacket[] packets)
        {
            ThrowIfDisposed();

            foreach (var packet in packets)
            {
                if (packet == null)
                {
                    continue;
                }

                await SendPacketAsync(timeout, cancellationToken, packet).ConfigureAwait(false);
            }
        }

        public Task SendPacketAsync(TimeSpan timeout, CancellationToken cancellationToken, MqttBasePacket packet)
        {
            return ExecuteAndWrapExceptionAsync(() =>
            {
                _logger.Verbose<MqttChannelAdapter>("TX >>> {0} [Timeout={1}]", packet, timeout);

                var packetData = PacketSerializer.Serialize(packet);

                return Internal.TaskExtensions.TimeoutAfter(ct => _channel.WriteAsync(
                    packetData.Array,
                    packetData.Offset,
                    packetData.Count,
                    ct), timeout, cancellationToken);
            });
        }

        public async Task<MqttBasePacket> ReceivePacketAsync(TimeSpan timeout, CancellationToken cancellationToken)
        {
            ThrowIfDisposed();

            if (_onPacketHandlerDelegated)
                throw new IOException();

            MqttBasePacket packet = null;

            await ExecuteAndWrapExceptionAsync(async () =>
            {
                try
                {
                    if (timeout == TimeSpan.Zero)
                    {
                        packet = await _receiveBuffer.ReceiveAsync(cancellationToken);

                    }
                    else
                    {
                        packet = await _receiveBuffer.ReceiveAsync(timeout, cancellationToken);

                    }
                }
                catch (OperationCanceledException exception)
                {
                    var timedOut = !cancellationToken.IsCancellationRequested;
                    if (timedOut)
                    {
                        throw new MqttCommunicationTimedOutException(exception);
                    }
                    else
                    {
                        throw;
                    }
                }
            }).ConfigureAwait(false);

            return packet;
        }


        private static async Task ExecuteAndWrapExceptionAsync(Func<Task> action)
        {
            try
            {
                await action().ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                throw;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (MqttCommunicationTimedOutException)
            {
                throw;
            }
            catch (MqttCommunicationException)
            {
                throw;
            }
            catch (COMException comException)
            {
                if ((uint)comException.HResult == ErrorOperationAborted)
                {
                    throw new OperationCanceledException();
                }

                throw new MqttCommunicationException(comException);
            }
            catch (IOException exception)
            {
                if (exception.InnerException is SocketException socketException)
                {
                    if (socketException.SocketErrorCode == SocketError.ConnectionAborted)
                    {
                        throw new OperationCanceledException();
                    }
                }

                throw new MqttCommunicationException(exception);
            }
            catch (Exception exception)
            {
                throw new MqttCommunicationException(exception);
            }
        }

        private Task DataIngestionTask()
        {
            return Task.Run(async () =>
            {
                var buffer = new byte[ReadBufferSize];
                while (!_isDisposed && !_receiveTaskCts.IsCancellationRequested)
                {                                      
                    try
                    {                        
                        var readBytesCount = await _channel.ReadAsync(buffer, 0, buffer.Length, _receiveTaskCts.Token).ConfigureAwait(false);
                        if (readBytesCount > 0)
                        {
                            _packetStateMachine.Push(new ArraySegment<byte>(buffer, 0, readBytesCount));
                        }                       
                    }
                    catch (Exception ex)
                    {
                        
                    }
                }
            });
        }

        public void Dispose()
        {
            _isDisposed = true;
            _channel?.Dispose();
        }

        private void ThrowIfDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(MqttChannelAdapter));
            }
        }
    }
}
