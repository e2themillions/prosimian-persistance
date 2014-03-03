using System.Resources;
using System.Collections;
using System;

namespace prosimian.persistence {
    public class DataResource {

        private static string _connectionString = "";
        private static ResourceManager rm = new ResourceManager("prosimian.persistence.ApplicationSQL", System.Reflection.Assembly.GetExecutingAssembly());
        private static Hashtable hashSQL = new Hashtable();

        // Using app.config / Web.config for getting the connection string
        public static string ConnectionString {
            get {
                return ADOConnectionString;
            }
        }

        private static string ADOConnectionString {
            get {
                if (_connectionString == "") {
                    _connectionString = System.Configuration.ConfigurationManager.AppSettings["prosimian.persistence.connectionstring"];
                }
                if (_connectionString == null) {
                    throw new Exception("prosimian.persitence was unable to find an appropriate connectionstring");
                    //_connectionString = "User ID=root;Password=;Host=localhost;Port=3306;Database=prosimian;Protocol=TCP;Compress=false;Pooling=true;Min Pool Size=0;Max Pool Size=100;Connection Lifetime=0";
                }
                return _connectionString;
            }
        }

        
        /// <summary>
        ///  Returns a sql-statement from the ApplicationSQL resource.
        ///  If the relevant sql-statement is not found, it will be inferred if it is one of the following:
        ///  _Insert
        ///  _Update
        ///  _Delete
        ///  _SelectByProperty
        ///  _SelectByCustomSQL
        ///  _SelectOne
        ///  _SelectAll
        /// The tablename in database has to be equal to object-type-name, and ID must also be standard...
        /// (i.e. you only need to override these, if you have special requirements)
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public static string GetString(string key) {            
            string sql = rm.GetString(key, System.Globalization.CultureInfo.InvariantCulture);
            if (null != sql && sql.Length > 0) {
                return sql;
            } else {
                return TryToInferSQL(key);
            }
        }

        private static string TryToInferSQL(string key) {
            string rpart = key.Substring(key.IndexOf('_')).ToLower();
            string tblName = key.Substring(0, key.IndexOf('_')).ToLower();
            switch (rpart) {
                case "_insert":                    
                    return "INSERT INTO " + tblName + "({ColumnData}) VALUES({ValueData})";
                case "_update":
                    return "UPDATE " + tblName + " {ColumnData} WHERE " + tblName + "ID = ?ID";
                case "_delete":
                    return "DELETE FROM " + tblName + " WHERE " + tblName + "ID = ?ID";
                case "_selectbyproperty": 
                    return "SELECT * FROM " + tblName + " WHERE {CustomExpression}";
                case "_selectbycustomsql":
                    return "SELECT * FROM " + tblName + " WHERE {CustomSQL}";
                case "_selectone":
                    return "SELECT * FROM " + tblName + " WHERE " + tblName + "ID = ?ID";
                case "_selectall":
                    return "SELECT * FROM " + tblName; 
                default:
                    throw new Exception("Unable to find SQL-string with key: " + key);

            }
        }

    }
}
