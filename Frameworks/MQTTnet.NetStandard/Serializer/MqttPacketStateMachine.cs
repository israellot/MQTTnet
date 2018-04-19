using MQTTnet.Packets;
using MQTTnet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MQTTnet.Serializer
{
    public enum MqttPacketStateMachineStep
    {
        FixedHeader,
        BodyLength,
        Body
    }

    public enum MqttPacketReadError
    {
        None=0,
        InvalidPacketType,
        InvalidBodyLength
    }

    public class MqttPacketStateMachineState
    {
        public MqttPacketStateMachineStep Step { get; set; } = MqttPacketStateMachineStep.FixedHeader;

        public MqttHeaderState Header { get; set; } = new MqttHeaderState();

        public MqttLengthState Length { get; set; } = new MqttLengthState();

        public MqttBodyState Body { get; set; } = new MqttBodyState();

        public Boolean PacketComplete { get; set; }

        

        public class MqttBodyState
        {            
            public int Offset { get; set; }

            public int WriteOffset { get; set; }

            public byte[] Buffer { get; set; }

            public int Length { get; set; }

            private byte[] _emptyBuffer = new byte[] { };

            public MqttBodyState()
            {
                //Data = new ArraySegment<byte>(new byte[size]);
                Buffer = _emptyBuffer;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                Offset = 0;
                Length = 0;
                WriteOffset = 0;
                Buffer = _emptyBuffer;
            }
        }

        public class MqttLengthState
        {
            public int Multiplier { get; set; } = 1;
            public int Value { get; set; } = 0;
            public int EncodedByte { get; set; } = 0;

            public int Count { get; set; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Reset()
            {
                Multiplier = 1;
                Value = 0;
                EncodedByte = 0;
                Count = 0;
            }
        }

        public class MqttHeaderState
        {
            public MqttControlPacketType PacketType { get; set; }

            public byte FixedHeader { get; set; }

            public void Reset()
            {

            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {            
            Length.Reset();
            Body.Reset();
            PacketComplete = false;
            Step = MqttPacketStateMachineStep.FixedHeader;
        }
    }

    public class MqttPacketStateMachine
    {

        public Func<MqttBasePacket, Task> OnPacketHandler { get; set; }

        MqttPacketStateMachineState _state;

        IMqttPacketSerializer _serializer;

        public MqttPacketStateMachine(IMqttPacketSerializer serializer)
        {
            _state = new MqttPacketStateMachineState();
            _serializer = serializer;
        }

                
        public MqttPacketReadError Push(ArraySegment<byte> data)
        {
            MqttPacketReadError error = MqttPacketReadError.None;

            start:

            switch (_state.Step)
            {
                case MqttPacketStateMachineStep.FixedHeader:
                    error = ReadFixedHeader(data.Array[data.Offset]);
                    if (error==0 && data.Count > 1)
                    {
                        data = data.Advance(1);
                        goto start;
                    }
                    break;
                case MqttPacketStateMachineStep.BodyLength:
                    error = ReadLength(data.Array[data.Offset]);

                    if (_state.PacketComplete)
                        ReadComplete();

                    if (error == 0 && data.Count > 1)
                    {
                        data = data.Advance(1);
                        goto start;
                    }
                    break;
                case MqttPacketStateMachineStep.Body:
                    error = ReadBody(data, out var remaining);

                    if (_state.PacketComplete)
                        ReadComplete();

                    if (error == 0 && remaining > 1)
                    {
                        data = data.Advance(data.Count- remaining);
                        goto start;
                    }
                    break;
            }

            if (error > 0)
            {
                //Reset state
                _state.Reset();
            }

            return error;
        }

        private MqttPacketReadError ReadFixedHeader(byte b)
        {                        
            //read type
            _state.Header.PacketType=(MqttControlPacketType)(b >> 4);

            if ((int)_state.Header.PacketType == 0 || (int)_state.Header.PacketType > 14)
                return MqttPacketReadError.InvalidPacketType;
                        
            _state.Header.FixedHeader = b;

            _state.Step = MqttPacketStateMachineStep.BodyLength;

            return MqttPacketReadError.None;
        }

        private MqttPacketReadError ReadLength(byte b)
        {
            if(_state.Length.Count==0)
                _state.Length.EncodedByte = b;

            _state.Length.Value += (byte)(_state.Length.EncodedByte & 127) * _state.Length.Multiplier;
            if (_state.Length.Multiplier > 128 * 128 * 128)
            {
                return MqttPacketReadError.InvalidBodyLength;
            }

            _state.Length.Multiplier *= 128;

            if ((_state.Length.EncodedByte & 128) == 0)
                _state.Step = MqttPacketStateMachineStep.Body;

            if (_state.Length.Value == 0)
            {
                //_state.Body.Data = new ArraySegment<byte>();
                _state.PacketComplete = true;
            }

            _state.Length.Count++;

            return MqttPacketReadError.None;

        }

        private MqttPacketReadError  ReadBody(ArraySegment<byte> data,out int remaining)
        {
            
            if (_state.Body.Length==0 && _state.Length.Value>0)
            {
                if (_state.Length.Value <= data.Count)
                {
                    //whole body in one shot shortcut
                    _state.Body.Buffer = data.Array;
                    _state.Body.Offset = data.Offset;
                    _state.Body.Length = _state.Length.Value;
                    _state.Body.WriteOffset= _state.Length.Value;

                    remaining = data.Count - _state.Length.Value;
                    _state.PacketComplete = true;
                    return MqttPacketReadError.None;

                }
                else
                {
                    //allocate new buffer
                    _state.Body.Buffer = new byte[_state.Length.Value];
                    _state.Body.Length = _state.Length.Value;
                }
                
            }

            var bytesToRead = _state.Length.Value - _state.Body.WriteOffset;
            remaining = 0;

            if (bytesToRead > 0)
            {
                var bytesAvailable = Math.Min(data.Count, bytesToRead);

                Buffer.BlockCopy(
                    data.Array,
                    data.Offset,
                    _state.Body.Buffer,
                    _state.Body.WriteOffset,
                    bytesAvailable
                    );

                _state.Body.WriteOffset += bytesAvailable;
                remaining = data.Count-bytesAvailable;
            }

            if(_state.Length.Value - _state.Body.WriteOffset == 0)            
            {
                //body complete
                _state.PacketComplete = true;
            }

            return MqttPacketReadError.None;

        }

        

        private void ReadComplete()
        {
            var header = new MqttPacketHeader()
            {
                FixedHeader = _state.Header.FixedHeader,
                BodyLength = _state.Length.Value,
                ControlPacketType = _state.Header.PacketType
            };


            MqttBasePacket packet;
            using (var ms = new MemoryStream(_state.Body.Buffer, _state.Body.Offset, _state.Body.Length,false))
            {
                packet = _serializer.Deserialize(header, ms);                
            }

            //Reset state
            _state.Reset();

            OnPacketHandler?.Invoke(packet);

        }
    }

    public static class ExtensionMethods
    {
        public static ArraySegment<byte> Advance(this ArraySegment<byte> segment,int count)
        {
            return new ArraySegment<byte>(segment.Array, segment.Offset + count, segment.Count - count);
        }
    }
}
