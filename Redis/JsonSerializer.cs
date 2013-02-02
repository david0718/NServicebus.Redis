using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NServiceBus.Redis
{
	public class JsonSerializer : ISerializer
	{
		#region ISerializer Members

		public string SerializeToString<T>(T value)
		{
			return ServiceStack.Text.JsonSerializer.SerializeToString(value);
		}

		public T DeserializeFromString<T>(string value)
		{
			return ServiceStack.Text.JsonSerializer.DeserializeFromString<T>(value);
		}

		#endregion
	}
}
