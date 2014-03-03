using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TEST {
    public class TestObject {

        private int _TestObjectID;
        public int TestObjectID {
            get { return _TestObjectID; }
            set { _TestObjectID = value; }
        }

        private string _TestString;
        public string TestString {
            get { return _TestString; }
            set { _TestString = value; }
        }

        private int _TestInt;
        public int TestInt {
            get { return _TestInt; }
            set { _TestInt = value; }
        }

        private Decimal _TestDecimal;
        public Decimal TestDecimal {
            get { return _TestDecimal; }
            set { _TestDecimal = value; }
        }
    }
}
