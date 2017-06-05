using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using Sitecore.Diagnostics;

namespace Sitecore.Data.LocalDataProvider
{
    internal static class StreamExtension
    {
        internal static readonly string FileMark = "sitecore-cache/data";
        internal static readonly int FileMarkLength = 38;
        internal static Dictionary<Type, byte> TypeMap;
        static StreamExtension()
        {
            TypeMap = new Dictionary<Type, byte>
            {
                [typeof(byte)] = (byte)DbType.Byte,
                [typeof(sbyte)] = (byte)DbType.SByte,
                [typeof(short)] = (byte)DbType.Int16,
                [typeof(ushort)] = (byte)DbType.UInt16,
                [typeof(int)] = (byte)DbType.Int32,
                [typeof(uint)] = (byte)DbType.UInt32,
                [typeof(long)] = (byte)DbType.Int64,
                [typeof(ulong)] = (byte)DbType.UInt64,
                [typeof(float)] = (byte)DbType.Single,
                [typeof(double)] = (byte)DbType.Double,
                [typeof(decimal)] = (byte)DbType.Decimal,
                [typeof(bool)] = (byte)DbType.Boolean,
                [typeof(string)] = (byte)DbType.String,
                [typeof(char)] = (byte)DbType.StringFixedLength,
                [typeof(Guid)] = (byte)DbType.Guid,
                [typeof(DateTime)] = (byte)DbType.DateTime,
                [typeof(DateTimeOffset)] = (byte)DbType.DateTimeOffset,
                [typeof(byte[])] = (byte)DbType.Binary,
                [typeof(byte?)] = (byte)DbType.Byte,
                [typeof(sbyte?)] = (byte)DbType.SByte,
                [typeof(short?)] = (byte)DbType.Int16,
                [typeof(ushort?)] = (byte)DbType.UInt16,
                [typeof(int?)] = (byte)DbType.Int32,
                [typeof(uint?)] = (byte)DbType.UInt32,
                [typeof(long?)] = (byte)DbType.Int64,
                [typeof(ulong?)] = (byte)DbType.UInt64,
                [typeof(float?)] = (byte)DbType.Single,
                [typeof(double?)] = (byte)DbType.Double,
                [typeof(decimal?)] = (byte)DbType.Decimal,
                [typeof(bool?)] = (byte)DbType.Boolean,
                [typeof(char?)] = (byte)DbType.StringFixedLength,
                [typeof(Guid?)] = (byte)DbType.Guid,
                [typeof(DateTime?)] = (byte)DbType.DateTime,
                [typeof(DateTimeOffset?)] = (byte)DbType.DateTimeOffset
            };
        }
        internal static void Write(this FileStream stream, Guid guid)
        {
            stream.Write(guid.ToByteArray());
        }
        internal static void Write(this FileStream stream, string stringVal)
        {
            byte[] byteVal = Encoding.Unicode.GetBytes(stringVal);
            stream.Write(byteVal, true);
        }
        internal static void Write(this FileStream stream, int intVal)
        {
            stream.Write(BitConverter.GetBytes(intVal));
        }
        internal static void Write(this FileStream stream, DateTime datetimeVal)
        {
            stream.Write(BitConverter.GetBytes(datetimeVal.ToBinary()));
        }
        internal static void Write(this FileStream stream, long longVal)
        {
            stream.Write(BitConverter.GetBytes(longVal));
        }
        internal static void Write(this FileStream stream, bool boolVal)
        {
            stream.Write(BitConverter.GetBytes(boolVal));
        }
        internal static void Write(this FileStream stream, byte[] bytes, bool setLength = false)
        {
            int length = bytes.Length;
            if (setLength)
                stream.Write(BitConverter.GetBytes(length), 0, 4);
            stream.Write(bytes, 0, length);
        }
        internal static string GetString(this FileStream stream)
        {
            int len = stream.GetInt32();
            byte[] strBty = new byte[len];
            stream.Read(strBty, 0, len);
            return Encoding.Unicode.GetString(strBty);
        }
        internal static Guid GetGuid(this FileStream stream)
        {
            byte[] guidByte = new byte[16];
            stream.Read(guidByte, 0, 16);
            Guid guid = new Guid(guidByte);
            return guid;
        }
        internal static int GetInt32(this FileStream stream)
        {
            byte[] intByte = new byte[4];
            stream.Read(intByte, 0, 4);
            return BitConverter.ToInt32(intByte, 0);
        }
        internal static long GetInt64(this FileStream stream)
        {
            byte[] longByte = new byte[8];
            stream.Read(longByte, 0, 8);
            return BitConverter.ToInt64(longByte, 0);
        }
        internal static bool GetBoolean(this FileStream stream)
        {
            byte[] boolByte = new byte[1];
            stream.Read(boolByte, 0, 1);
            return BitConverter.ToBoolean(boolByte, 0);
        }
        internal static byte[] GetBytes(this FileStream stream)
        {
            long length = stream.GetInt32();
            byte[] strBty = new byte[length];
            stream.Read(strBty, 0, (int)length);
            return strBty;
        }
        internal static byte GetByte(this FileStream stream)
        {
            byte[] strBty = new byte[1];
            stream.Read(strBty, 0, 1);
            return strBty[0];
        }
        internal static DateTime GetDateTime(this FileStream stream)
        {
            long longVal = stream.GetInt64();
            return new DateTime(longVal);
        }
    }
}
