using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NServiceBus.Redis
{
	public interface ISerializer
	{

		string SerializeToString<T>(T value);

		T DeserializeFromString<T>(string value);

	}
}
