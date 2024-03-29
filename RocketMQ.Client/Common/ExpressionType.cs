﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RocketMQ.Client
{
    public class ExpressionType
    {
        /**
         * <ul>
         * Keywords:
         * <li>{@code AND, OR, NOT, BETWEEN, IN, TRUE, FALSE, IS, NULL}</li>
         * </ul>
         * <p/>
         * <ul>
         * Data type:
         * <li>Boolean, like: TRUE, FALSE</li>
         * <li>String, like: 'abc'</li>
         * <li>Decimal, like: 123</li>
         * <li>Float number, like: 3.1415</li>
         * </ul>
         * <p/>
         * <ul>
         * Grammar:
         * <li>{@code AND, OR}</li>
         * <li>{@code >, >=, <, <=, =}</li>
         * <li>{@code BETWEEN A AND B}, equals to {@code >=A AND <=B}</li>
         * <li>{@code NOT BETWEEN A AND B}, equals to {@code >B OR <A}</li>
         * <li>{@code IN ('a', 'b')}, equals to {@code ='a' OR ='b'}, this operation only support string type.</li>
         * <li>{@code IS NULL}, {@code IS NOT NULL}, check parameter whether is null, or not.</li>
         * <li>{@code =TRUE}, {@code =FALSE}, check parameter whether is true, or false.</li>
         * </ul>
         * <p/>
         * <p>
         * Example:
         * (a > 10 AND a < 100) OR (b IS NOT NULL AND b=TRUE)
         * </p>
         */
        public static readonly string SQL92 = "SQL92";

        /**
         * Only support or operation such as
         * "tag1 || tag2 || tag3", <br>
         * If null or * expression,meaning subscribe all.
         */
        public static readonly string TAG = "TAG";

        public static bool isTagType(string type)
        {
            if (type == null || "".Equals(type) || TAG.Equals(type))
            {
                return true;
            }
            return false;
        }
    }
}
