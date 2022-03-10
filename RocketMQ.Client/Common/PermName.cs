using System;

namespace RocketMQ.Client
{
    public class PermName
    {
        public static readonly int PERM_PRIORITY = 0x1 << 3;
        public static readonly int PERM_READ = 0x1 << 2;
        public static readonly int PERM_WRITE = 0x1 << 1;
        public static readonly int PERM_INHERIT = 0x1;

        public static String perm2String(int perm)
        {
            string res = null;
            //StringBuilder sb = new StringBuilder("---");
            if (isReadable(perm))
            {
                //sb.replace(0, 1, "R");
                res = "R--";
            }

            if (isWriteable(perm))
            {
                //sb.replace(1, 2, "W");
                res = "-W-";
            }

            if (isInherited(perm))
            {
                //sb.replace(2, 3, "X");
                res = "--X";
            }
            return res;
            //return sb.ToString();
        }

        public static bool isReadable(int perm)
        {
            return (perm & PERM_READ) == PERM_READ;
        }

        public static bool isWriteable(int perm)
        {
            return (perm & PERM_WRITE) == PERM_WRITE;
        }

        public static bool isInherited(int perm)
        {
            return (perm & PERM_INHERIT) == PERM_INHERIT;
        }
    }
}
