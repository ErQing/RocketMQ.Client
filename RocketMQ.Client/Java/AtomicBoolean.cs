using System.Threading;


namespace RocketMQ.Client
{
    /// <summary>
    /// Provides lock-free atomic read/write utility for a <c>bool</c> value. The atomic classes found in this package
    /// were are meant to replicate the <c>java.util.concurrent.atomic</c> package in Java by Doug Lea. The two main differences
    /// are implicit casting back to the <c>bool</c> data type, and the use of a non-volatile inner variable.
    /// 
    /// <para>The internals of these classes contain wrapped usage of the <c>System.Threading.Interlocked</c> class, which is how
    /// we are able to provide atomic operation without the use of locks. </para>
    /// </summary>
    /// <remarks>
    /// It's also important to note that <c>++</c> and <c>--</c> are never atomic, and one of the main reasons this class is 
    /// needed. I don't believe its possible to overload these operators in a way that is autonomous.
    /// </remarks>
    /// \author Matt Bolt
    public class AtomicBoolean
    {

        private int _value;

        /// <summary>
        /// Creates a new <c>AtomicBoolean</c> instance with an initial value of <c>false</c>.
        /// </summary>
        public AtomicBoolean()
            : this(false)
        {

        }

        /// <summary>
        /// Creates a new <c>AtomicBoolean</c> instance with the initial value provided.
        /// </summary>
        public AtomicBoolean(bool value)
        {
            _value = value ? 1 : 0;
        }

        /// <summary>
        /// This method returns the current value.
        /// </summary>
        /// <returns>
        /// The <c>bool</c> value to be accessed atomically.
        /// </returns>
        public bool get()
        {
            return _value != 0;
        }

        /// <summary>
        /// This method sets the current value atomically.
        /// </summary>
        /// <param name="value">
        /// The new value to set.
        /// </param>
        public void Set(bool value)
        {
            Interlocked.Exchange(ref _value, value ? 1 : 0);
        }

        /// <summary>
        /// This method atomically sets the value and returns the original value.
        /// </summary>
        /// <param name="value">
        /// The new value.
        /// </param>
        /// <returns>
        /// The value before setting to the new value.
        /// </returns>
        public bool GetAndSet(bool value)
        {
            return Interlocked.Exchange(ref _value, value ? 1 : 0) != 0;
        }

        /// <summary>
        /// Atomically sets the value to the given updated value if the current value <c>==</c> the expected value.
        /// </summary>
        /// <param name="expected">
        /// The value to compare against.
        /// </param>
        /// <param name="result">
        /// The value to set if the value is equal to the <c>expected</c> value.
        /// </param>
        /// <returns>
        /// <c>true</c> if the comparison and set was successful. A <c>false</c> indicates the comparison failed.
        /// </returns>
        public bool compareAndSet(bool expected, bool result)
        {
            int e = expected ? 1 : 0;
            int r = result ? 1 : 0;
            return Interlocked.CompareExchange(ref _value, r, e) == e;
        }

        /// <summary>
        /// This operator allows an implicit cast from <c>AtomicBoolean</c> to <c>int</c>.
        /// </summary>
        public static implicit operator bool(AtomicBoolean value)
        {
            return value.get();
        }

    }
}


