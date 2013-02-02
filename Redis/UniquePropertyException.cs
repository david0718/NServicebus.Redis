using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NServiceBus.Redis
{
	public class UniquePropertyException : Exception
	{

		public UniquePropertyException(string message) : base(message) { }

	}
}
