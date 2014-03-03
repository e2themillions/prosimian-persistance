using System;
using System.Windows.Forms;
using prosimian.persistence;
using log4net;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Text;


namespace TEST {
    public partial class Form1 : Form {
        // let's get some basic logging..
        private static readonly ILog Log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static BaseObjectManager _bom = null;
        private static BaseObjectManager bom {
            get {
                if (null == _bom) {
                    _bom = new BaseObjectManager();
                }
                return _bom;  
            }
            set {
                _bom = value;
            }        
        }
        
        public Form1() {
            InitializeComponent();
            log4net.Config.XmlConfigurator.Configure();
            Log.Debug("TEST form loaded...");            
        }           
        


        private void button1_Click(object sender, EventArgs e) {
            try {
                bom.OpenConnection();
                bom.CloseConnection();
                textBox1.Text = "Connection succeeded";
            } catch {
                textBox1.Text = "Connection failed";
            }
        }

        private void button2_Click(object sender, EventArgs e) {
            TestObject t = new TestObject();
            t.TestDecimal = 2.4M;
            t.TestInt = 42;
            t.TestString = "Hello persistence..";
            if (bom.Insert(t)) {
                textBox1.Text += "\nObject inserted. New ID = " + t.TestObjectID.ToString();
            } else {
                textBox1.Text += "\nObject insertion failed...";
            }
        }

        
    }
}
