using System;

namespace RocketMQ.Client
{
    public class ByteBuffer
    {
        /// <summary>
        /// 默认大端[保持和网络序列一致][java一致]
        /// </summary>
        public bool IsLittleEndian { get; private set; } = false;

        private byte[] data;
        public byte[] Data { get { return data; } }
        private int position;
        public int Position { get { return position; } }
        public int limit { private set; get; }
        public int Capacity { get; private set; }

        public ByteBuffer(int capacity, bool littleEndian=false)
        {
            IsLittleEndian = littleEndian;
            data = new byte[capacity];
            Capacity = capacity;
        }

        public static ByteBuffer allocate(int capacity, bool littleEndian = false)
        {
            if (capacity < 0)
                throw new ArgumentException("capacity < 0: (" + capacity + " < 0)");
            return new ByteBuffer(capacity, littleEndian);
        }

        public static ByteBuffer wrap(byte[] arr, bool littleEndian = false)
        {
            if (arr == null  || arr.Length < 0)
                throw new ArgumentException("capacity < 0: (" + arr.Length + " < 0)");
            var res = new ByteBuffer(arr.Length, littleEndian);
            res.put(arr);
            return res;
        }

        /// <summary>
        /// ???
        /// </summary>
        /// <param name="newLimit"></param>
        /// <returns></returns>
        public ByteBuffer ResetLimit(int newLimit)
        {
            if (newLimit > Capacity || newLimit < 0)
                throw new ArgumentException($"newLimit is big than capacity or less tha zero:{newLimit}");
            limit = newLimit;
            if (position > newLimit) position = newLimit;
            return this;
        }

        /// <summary>
        /// ???
        /// </summary>
        /// <param name="newPosition"></param>
        /// <returns></returns>
        public ByteBuffer ResetPosition(int newPosition)
        {
            if (newPosition > limit | newPosition < 0)
                throw new ArgumentException($"newPosition is big than limit or less tha zero:{newPosition}");
            //if (mark > newPosition) mark = -1;
            position = newPosition;
            return this;
        }

        public byte[] array()
        {
            return data;
        }

        #region write

        public void putByte(byte val)
        {
            XBuffer.WriteByte(val, data, ref position);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.byteSize;
                putByte(val);
            }
        }

        public void putBool(bool val)
        {
            XBuffer.WriteBool(val, data, ref position);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.boolSize;
                putBool(val);
            }
        }

        public void putShort(short val)
        {
            XBuffer.WriteShort(val, data, ref position, IsLittleEndian);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.shortSize;
                putShort(val);
            }
        }

        public void putInt(int val)
        {
            XBuffer.WriteInt(val, data, ref position, IsLittleEndian);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.intSize;
                putInt(val);
            }
        }

        public void putLong(long val)
        {
            XBuffer.WriteLong(val, data, ref position, IsLittleEndian);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.longSize;
                putLong(val);
            }
        }

        public void putFloat(float val)
        {
            XBuffer.WriteFloat(val, data, ref position, IsLittleEndian);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.floatSize;
                putFloat(val);
            }
        }

        public void putDouble(double val)
        {
            XBuffer.WriteDouble(val, data, ref position, IsLittleEndian);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.doubleSize;
                putDouble(val);
            }
        }

        public void put(byte val)
        {
            XBuffer.WriteByte(val, data, ref position);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= XBuffer.byteSize;
                put(val);
            }
        }

        public void put(byte[] val)
        {
            XBuffer.WriteRawBytes(val, data, ref position);
            if (position > Capacity)
            {
                Capacity *= 2;
                Array.Resize(ref data, Capacity);
                position -= val.Length;
                put(val);
            }
        }

        public void put(ByteBuffer val)
        {
            put(val.data);
        }

        public ByteBuffer put(byte[] src, int offset, int length)
        {
            if(length > remaining())
                throw new ArgumentException($"length is big than remaining :{length}");
            XBuffer.WriteRawBytes(src, length, data, ref position);
            return this;
        }


        #endregion

        public int remaining()
        {
            int rem = limit - position;
            return rem > 0 ? rem : 0;
        }

        public ByteBuffer flip()
        {
            limit = position;
            position = 0;
            return this;
        }

        public bool hasRemaining()
        {
            return limit - position > 0;
        }

        #region read

        public byte getByte()
        {
            return XBuffer.ReadByte(data, ref position);
        }

        public bool getBool()
        {
            return XBuffer.ReadBool(data, ref position);
        }

        
        public short getShort()
        {
            return XBuffer.ReadShort(data, ref position, IsLittleEndian);
        }

        /// <summary>
        /// 不会修改position
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public short getShort(int index)
        {
            return XBuffer.GetShort(data, index, IsLittleEndian);
        }

        public int getInt()
        {
            return XBuffer.ReadInt(data, ref position, IsLittleEndian);
        }

        public int getInt(int index)
        {
            return XBuffer.GetInt(data, index, IsLittleEndian);
        }

        public long getLong()
        {
            return XBuffer.ReadLong(data, ref position, IsLittleEndian);
        }

        public long getLong(int index)
        {
            return XBuffer.GetLong(data, index, IsLittleEndian);
        }

        public float getFloat()
        {
            return XBuffer.ReadFloat(data, ref position, IsLittleEndian);
        }

        public double getDouble()
        {
            return XBuffer.ReadDouble(data, ref position, IsLittleEndian);
        }

        public byte get()
        {
            return XBuffer.ReadByte(data, ref position);
        }

        public byte get(int index)
        {
            return XBuffer.GetByte(data, index);
        }

        //public byte[] get(int len)
        //{
        //    return XBuffer.ReadRawBytes(data, len, ref position);
        //}

        public ByteBuffer get(byte[] dst)
        {
            XBuffer.ReadRawBytes(data, dst, 0, dst.Length, ref position);
            return this;
        }

        public ByteBuffer get(byte[] dst, int dstIndex, int len)
        {
            XBuffer.ReadRawBytes(data, dst, dstIndex, len, ref position);
            return this;
        }

        #endregion


    }
}
