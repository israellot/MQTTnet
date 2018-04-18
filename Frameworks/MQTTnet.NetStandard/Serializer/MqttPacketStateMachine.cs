using MQTTnet.Packets;
using MQTTnet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

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

        public MqttBodyState Body { get; set; }

        public Boolean PacketComplete { get; set; }

        public class MqttBodyState
        {            
            public int Offset { get; set; }

            public ArraySegment<byte> Data { get; set; }

            public MqttBodyState(int size)
            {
                Data = new ArraySegment<byte>(new byte[size]);
            }

            public MqttBodyState(ArraySegment<byte> segment)
            {
                Data = segment;
            }
        }

        public class MqttLengthState
        {
            public int Multiplier { get; set; } = 1;
            public int Value { get; set; } = 0;
            public int EncodedByte { get; set; } = 0;

            public int Count { get; set; }
        }

        public class MqttHeaderState
        {
            public MqttControlPacketType PacketType { get; set; }

            public Boolean Retain { get; set; }

            public MqttQualityOfServiceLevel QoS { get; set; }

            public Boolean Dup { get; set; }

            public byte FixedHeader { get; set; }
        }
    }

    public class MqttPacketStateMachine
    {

        Action<MqttBasePacket> _onPacketAction;

        MqttPacketStateMachineState _state;

        IMqttPacketSerializer _serializer;

        public MqttPacketStateMachine(IMqttPacketSerializer serializer)
        {
            _state = new MqttPacketStateMachineState();
            _serializer = serializer;
        }

        public void OnPacket(Action<MqttBasePacket> action)
        {
            _onPacketAction = action;
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
                _state = new MqttPacketStateMachineState();
            }

            return error;
        }

        private MqttPacketReadError ReadFixedHeader(byte b)
        {                        
            //read type
            _state.Header.PacketType=(MqttControlPacketType)(b >> 4);

            if ((int)_state.Header.PacketType == 0 || (int)_state.Header.PacketType > 14)
                return MqttPacketReadError.InvalidPacketType;

            //flags
            _state.Header.Retain = (b & 0x0001b) > 0;
            _state.Header.QoS = (MqttQualityOfServiceLevel)((b << 1) & 0x0011b);
            _state.Header.Dup = (b & 0x1000b) > 0;

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
                _state.PacketComplete = true;

            _state.Length.Count++;

            return MqttPacketReadError.None;

        }

        private MqttPacketReadError  ReadBody(ArraySegment<byte> data,out int remaining)
        {
            
            if (_state.Body == null)
            {
                _state.Body = new MqttPacketStateMachineState.MqttBodyState(_state.Length.Value);

                //if (_state.Length.Value <= data.Count)
                //{
                //    //Full body in one shot
                //    _state.Body = new MqttPacketStateMachineState.MqttBodyState(
                //        new ArraySegment<byte>(data.Array,data.Offset, _state.Length.Value));

                //    remaining = data.Count-_state.Length.Value;
                //    _state.Body.Offset = _state.Length.Value;
                //}
                //else
                //{
                //    //Need to buffer
                    
                //}
            }

            var bytesToRead = _state.Length.Value - _state.Body.Offset;
            remaining = 0;

            if (bytesToRead > 0)
            {
                var bytesAvailable = Math.Min(data.Count, bytesToRead);

                Buffer.BlockCopy(
                    data.Array,
                    data.Offset,
                    _state.Body.Data.Array,
                    _state.Body.Offset,
                    bytesAvailable
                    );

                _state.Body.Offset += bytesAvailable;
                remaining = data.Count-bytesAvailable;
            }

            if(_state.Length.Value - _state.Body.Offset==0)            
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

            using (var ms = new MemoryStream(_state.Body.Data.Array, _state.Body.Data.Offset, _state.Body.Data.Count,false))
            {
                var packet = _serializer.Deserialize(header, ms);
                _onPacketAction?.Invoke(packet);
            }

            //Reset state
            _state = new MqttPacketStateMachineState();

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
