using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Reflection;
using System.Xml;
using log4net;
using MySql.Data.MySqlClient;
using MySql.Data.Types;
using MySql;



namespace prosimian.persistence {
    /// <summary>
    /// Custom SQL for the following actions can be defined in ApplicationSQL.resx to override inferred SQL
    ///  [object_name]_Insert
    ///  [object_name]_Update
    ///  [object_name]_Delete
    ///  [object_name]_SelectByProperty
    ///  [object_name]_SelectByCustomSQL
    ///  [object_name]_SelectOne
    ///  [object_name]_SelectAll
    ///  [object_name]_SelectByForeignKey
    /// </summary>
    public class BaseObjectManager {
        protected static readonly ILog log = LogManager.GetLogger("persistence");
        MySqlConnection connection = new MySqlConnection();
        private bool manuallyOpenedConnection = false;

        public BaseObjectManager() {

        }

        ~BaseObjectManager() {
            //destructor..
            if (null != connection &&
                connection.State != ConnectionState.Closed &&
                connection.State != ConnectionState.Broken) {
                log.Error("Somebody forgot to close database connection. Destructor mechanism is trying forcing close..");
                try {
                    connection.Close();
                } catch (Exception exception) {
                    log.Error("Unable to nicely close database connection.", exception);
                }
            }
        }

        /// <summary>
        /// If you intend to do more than one database request, you can manually open the
        /// connection to boost performance.. Remember to close it after yourself though..
        /// </summary>
        public void OpenConnection() {
            if (connection.State != ConnectionState.Open) {
                try {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    manuallyOpenedConnection = true;
                    log.Debug("Opened db connection manually..");
                } catch (System.Data.Odbc.OdbcException exc) {
                    log.Error("Kunne ikke åbne forbindelsen til databasen (ODBC-fejl)", exc);
                    throw exc;
                } catch (MySql.Data.MySqlClient.MySqlException exc) {
                    log.Error("Kunne ikke åbne forbindelsen til databasen (MySQL-fejl)", exc);
                    throw exc;
                } catch (Exception exc) {
                    log.Error("Kunne ikke åbne forbindelsen til databasen", exc);
                    throw exc;
                }
            }
        }
        /// <summary>
        /// Method to manually close connection.. (only needed if you opened it manually)
        /// </summary>
        public void CloseConnection() {
            if (connection.State != ConnectionState.Closed && connection.State != ConnectionState.Executing) {
                try {
                    connection.Close();
                    log.Debug("Closed db connection manually..");
                } catch (Exception exc) {
                    log.Error("Unable to peacefully close connection.", exc);
                }
            }
            manuallyOpenedConnection = false;
        }
        public IDbConnection GetConnection() {
            return connection;
        }
        #region Various helper-methods
        private object getConvertedValue(object unknownObject, MySqlDbType dbType) {
            if (null != unknownObject) {
                if (unknownObject.GetType() == typeof(bool)) {
                    if ((bool)unknownObject) {
                        return 1;
                    } else {
                        return 0;
                    }
                } else {
                    return unknownObject;
                }
            } else {
                if (dbType == MySqlDbType.Int32) {
                    return 0;
                } else {
                    return null;
                }
            }
        }
        private static object GetInternalValue(MySqlDataReader reader, Type datatype, int index) {
            if (reader.IsDBNull(index)) {
                if (datatype.ToString() == "System.String") {
                    return "";
                } else if ((datatype.ToString() == "System.Int8") || (datatype.ToString() == "System.Int16") || (datatype.ToString() == "System.Int32") || (datatype.ToString() == "System.Int64")) {
                    return 0;
                } else {
                    return null;
                }
            } else {
                switch (datatype.ToString()) {
                    case "System.Boolean":
                        return Convert.ToBoolean(reader.GetValue(index)); //fuck you mySql..
                    case "System.Int32":
                        return Convert.ToInt32(reader.GetValue(index));
                    case "prosimian.core.shop.Category+CategoryTypes":
                        //Type ty = Type.GetType("prosimian.core.shop.Category+CategoryTypes", true, true);

                        return Enum.ToObject(datatype, Convert.ToInt32(reader.GetValue(index)));
                    default:
                        return reader.GetValue(index);
                }
            }
        }

        private static MySqlDbType GetMySqlType(Type clrType) {
            switch (clrType.ToString()) {
                case "System.Int16":
                    return MySqlDbType.Int16;
                case "System.Int32":
                    return MySqlDbType.Int32;
                case "System.Int64":
                    return MySqlDbType.Int64;
                case "System.DateTime":
                    return MySqlDbType.Datetime;
                case "System.String":
                    return MySqlDbType.String;
                case "System.Char":
                    return MySqlDbType.VarChar;
                case "System.Decimal":
                    return MySqlDbType.Decimal;
                case "System.Double":
                    return MySqlDbType.Double;
                case "System.Boolean":
                    return MySqlDbType.Byte;
                default:
                    log.Error("GetMySqlType doesn't know how to translate " + clrType.ToString() + " into a db type!");
                    return MySqlDbType.String;
            }
        }

        private ArrayList GetColumnNames(Type objType) {
            ArrayList retVal = null;
            string strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

            #region initialize the command and connection
            MySqlCommand command;
            MySqlDataReader reader = null;
            bool didOpen = false;
            if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                connection.ConnectionString = DataResource.ConnectionString;
                connection.Open();
                didOpen = true;
            } else if (connection.State != ConnectionState.Open) {
                connection.ConnectionString = DataResource.ConnectionString;
                connection.Open();
                log.Info("Had to open connection, even though it was supposed to be open already...");
            }
            #endregion

            string strStoredProc;
            try {

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectByProperty";

                command = new MySqlCommand(DataResource.GetString(strStoredProc).Replace("{CustomExpression}", "false"), connection);

                ArrayList fieldNames = new ArrayList(0);
                reader = command.ExecuteReader();
                retVal = new ArrayList(reader.FieldCount);
                for (int i = 0; i < reader.FieldCount; i++) {
                    retVal.Add(reader.GetName(i));
                }

            } catch (Exception exception) {
                throw exception;
            } finally {
                #region Handle close..
                if (reader != null) {
                    if (reader.IsClosed == false) {
                        reader.Close();
                    }
                }
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection && didOpen) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
                #endregion
            }
            return retVal;
        }
        #endregion

        #region Various get-methods
        /// <summary>
        /// Hydrates simple attributes (ie. no lists/compound/cascading objects are loaded)
        /// </summary>
        /// <param name="objSource"></param>
        /// <param name="objObjectID"></param>
        /// <returns></returns>
        public virtual object Get(Type objType, object objObjectID) {
            object returnObject = null;
            string strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);
            string strStoredProc;
            MySqlParameter parameter;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                // get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectOne";

                // initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Info("Had to open connection, even though it was supposed to be open already...");
                }
                command = new MySqlCommand(DataResource.GetString(strStoredProc), connection);

                //add the ID parameter
                parameter = new MySqlParameter("ID", MySqlDbType.Int64);
                parameter.Direction = ParameterDirection.Input;
                parameter.Value = objObjectID;
                command.Parameters.Add(parameter);

                reader = command.ExecuteReader();
                if (reader.Read()) {
                    #region hydrate object
                    returnObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);

                        if (null != field) {
                            try {
                                field.SetValue(returnObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType.ToString() + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance |
                                                                 BindingFlags.Public |
                                                                 BindingFlags.NonPublic |
                                                                 BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(returnObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }
                    }
                    log.Debug("Hydrated object of type " + strObject + " with ID = " + objObjectID.ToString());
                    #endregion
                } else {
                    log.Debug("Not found! Object of type " + strObject + " with ID = " + objObjectID.ToString());
                }


            } catch (Exception exception) {
                log.Error("Fejl ved Hydrate af " + objType.ToString() + " (ID= " + objObjectID.ToString() + ")", exception);
            } finally {
                // always call Close when done reading.
                reader.Close();

                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return returnObject;
        }
        public IList GetAll(Type objType) {
            return GetAll(objType, "");
        }

        public IList GetAll(Type objType, string OrderByText) {
            ArrayList listObjects = new ArrayList();
            string strObject;
            string strStoredProc;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                //get the object name
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectAll";
                string strSql = "";
                try {
                    strSql = DataResource.GetString(strStoredProc);
                } catch (Exception exception) {
                    log.Error("Kunne ikke finde SQL-streng for " + strStoredProc + " i resource-filen. Aborting mission!");
                    throw exception;
                }

                // append order-by clause
                if (OrderByText.Length > 0) {
                    if ((strSql.ToLower().IndexOf("order by") + strSql.ToLower().IndexOf("where") > 0) || strSql.EndsWith(";")) {
                        // SQL not suited for "blind" order by suffix
                        log.Info("SQL not suited for \"blind\" order by suffix - ignoring parameter OrderByText");
                    } else {
                        strSql += " ORDER BY " + OrderByText;
                        log.Fatal("FIRING SQL: " + strSql);
                    }
                }

                // initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Info("Had to open connection, even though it was supposed to be open already...");
                }

                command = new MySqlCommand(strSql, connection);

                //execute query
                reader = command.ExecuteReader();

                while (reader.Read()) {
                    object newObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);

                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        //log.Debug("Setting field " + strColName); //This will result in a LOT of logging..
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance |
                                                                            BindingFlags.Public |
                                                                            BindingFlags.NonPublic |
                                                                            BindingFlags.IgnoreCase);

                        if (null != field) {
                            try {
                                field.SetValue(newObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance |
                                                                 BindingFlags.Public |
                                                                 BindingFlags.NonPublic |
                                                                 BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(newObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }

                    }
                    listObjects.Add(newObject);
                }
                log.Debug("GetAll hentede " + listObjects.Count + " objekter af typen " + objType.ToString());
            } catch (Exception exception) {
                log.Error("Fejl ved GetAll for " + objType.ToString(), exception);
            } finally {
                reader.Close();
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return listObjects;
        }
        public IList GetByForeignKey(Type objType, object objForeignKey) {
            ArrayList listObjects = new ArrayList();
            string strObject;
            string strStoredProc;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                //get the object name
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectByForeignKey";
                string strSql = "";
                try {
                    strSql = DataResource.GetString(strStoredProc);
                } catch (Exception exception) {
                    log.Error("Kunne ikke finde SQL-streng for " + strStoredProc + " i resource-filen. Aborting mission!");
                    throw exception;
                }

                #region initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Debug("Had to open connection, even though it was supposed to be open already...");
                }
                #endregion

                command = new MySqlCommand(strSql, connection);

                //add the foreign key parameter
                MySqlParameter parameter = new MySqlParameter("ForeignKey", GetMySqlType(objForeignKey.GetType()));
                parameter.Direction = ParameterDirection.Input;
                parameter.Value = objForeignKey;
                command.Parameters.Add(parameter);


                //execute query
                log.Debug("GetByForeignKey SQL:" + command.CommandText);
                reader = command.ExecuteReader();

                #region Hydrate properties
                while (reader.Read()) {
                    object newObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                        if (null != field) {
                            try {
                                field.SetValue(newObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType.ToString() + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(newObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }

                    }
                    listObjects.Add(newObject);
                }
                #endregion

                log.Debug("GetByForeignKey hentede " + listObjects.Count + " objekter af typen " + objType.ToString());
            } catch (Exception exception) {
                log.Error("Fejl ved GetByForeignKey for " + objType.ToString(), exception);
            } finally {
                reader.Close();
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return listObjects;
        }
        public IList GetByProperty(Type objType, string propName, object propVal) {
            Hashtable h = new Hashtable(1);
            h.Add(propName, propVal);
            return GetByProperty(objType, h, true);
        }
        public IList GetByProperty(Type objType, Hashtable propNamesAndVals) {
            return GetByProperty(objType, propNamesAndVals, true);
        }
        public IList GetByProperty(Type objType, Hashtable propNamesAndVals, bool UseInConjunction) {
            ArrayList listObjects = new ArrayList();
            string strObject;
            string strStoredProc;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                //get the object name
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectByProperty";
                string strSql = "";
                try {
                    strSql = DataResource.GetString(strStoredProc);
                } catch (Exception exception) {
                    log.Error("Kunne ikke finde SQL-streng for " + strStoredProc + " i resource-filen. Aborting mission!");
                    throw exception;
                }

                #region initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Debug("Had to open connection, even though it was supposed to be open already...");
                }
                #endregion

                command = new MySqlCommand(strSql, connection);

                //add the property parameters
                string strParamText = "";
                string strJoinerText = " AND ";
                if (!UseInConjunction) strJoinerText = " OR ";
                foreach (string propName in propNamesAndVals.Keys) {
                    strParamText += propName + " = ?" + propName + strJoinerText;
                }
                strParamText = strParamText.Substring(0, strParamText.Length - strJoinerText.Length);
                command.CommandText = command.CommandText.Replace("{CustomExpression}", strParamText);

                foreach (string propName in propNamesAndVals.Keys) {
                    object propValue = propNamesAndVals[propName];
                    MySqlParameter parameter = new MySqlParameter(propName, GetMySqlType(propValue.GetType()));
                    parameter.Direction = ParameterDirection.Input;
                    parameter.Value = propValue;
                    command.Parameters.Add(parameter);
                }



                //log sql and execute query
                string paramTexts = " Parameters: "; foreach (MySqlParameter p in command.Parameters) { paramTexts += p.Value + ", "; } log.Debug(command.CommandText + paramTexts);
                reader = command.ExecuteReader();

                #region Hydrate properties
                while (reader.Read()) {
                    object newObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                        if (null != field) {
                            try {
                                field.SetValue(newObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType.ToString() + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(newObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }

                    }
                    listObjects.Add(newObject);
                }
                #endregion

                log.Debug("GetByProperty hentede " + listObjects.Count + " objekter af typen " + objType.ToString());
            } catch (Exception exception) {
                log.Error("Fejl ved GetByProperty for " + objType.ToString(), exception);
            } finally {
                reader.Close();
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return listObjects;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="objType"></param>
        /// <param name="propNamesAndValsHashtables"></param>
        /// <param name="UseOuterConjunction"></param>
        /// <param name="UseInnerConjunction"></param>
        /// <param name="strOperator">Could be "=" or "LIKE" etc..</param>
        /// <param name="strAppendSql">Raw SQL to append in the last part of the CommandText (ignores joiners). Good for e.g. " AND yy=false ORDER BY xx"</param>
        /// <returns></returns>
        public IList GetByProperty(Type objType, ArrayList propNamesAndValsHashtables, bool UseOuterConjunction, bool UseInnerConjunction, string strOperator, string strAppendSql) {
            ArrayList listObjects = new ArrayList();
            string strObject;
            string strStoredProc;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                //get the object name
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectByProperty";
                string strSql = "";
                try {
                    strSql = DataResource.GetString(strStoredProc);
                } catch (Exception exception) {
                    log.Error("Kunne ikke finde SQL-streng for " + strStoredProc + " i resource-filen. Aborting mission!");
                    throw exception;
                }

                #region initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Debug("Had to open connection, even though it was supposed to be open already...");
                }
                #endregion

                command = new MySqlCommand(strSql, connection);

                //add the property parameters
                string strParamText = "";
                string strOuterJoinerText = " AND ";
                string strInnerJoinerText = " AND ";
                if (!UseInnerConjunction) strInnerJoinerText = " OR ";
                if (!UseOuterConjunction) strOuterJoinerText = " OR ";

                int groupCounter = 0;
                foreach (Hashtable propNamesAndVals in propNamesAndValsHashtables) {
                    string strSubParamText = "";
                    foreach (string propName in propNamesAndVals.Keys) {
                        string strippedPropName = propName;
                        if (propName.IndexOf('$') > 0) {
                            strippedPropName = propName.Substring(0, propName.IndexOf('$'));
                        }
                        strSubParamText += strippedPropName + " " + strOperator + " ?" + propName + "$" + (++groupCounter).ToString() + strInnerJoinerText;
                    }
                    strSubParamText = strSubParamText.Substring(0, strSubParamText.Length - strInnerJoinerText.Length);

                    strParamText += "(" + strSubParamText + ")" + strOuterJoinerText;

                }
                strParamText = strParamText.Substring(0, strParamText.Length - strOuterJoinerText.Length);
                strParamText += strAppendSql;
                command.CommandText = command.CommandText.Replace("{CustomExpression}", strParamText);

                groupCounter = 0;
                foreach (Hashtable propNamesAndVals in propNamesAndValsHashtables) {
                    foreach (string propName in propNamesAndVals.Keys) {
                        object propValue = propNamesAndVals[propName];
                        MySqlParameter parameter = new MySqlParameter(propName + "$" + (++groupCounter).ToString(), GetMySqlType(propValue.GetType()));
                        parameter.Direction = ParameterDirection.Input;
                        if (propValue.GetType() == typeof(String) && "LIKE" == strOperator.ToUpper().Trim()) {
                            propValue = "%" + propValue + "%";
                        }
                        parameter.Value = propValue;
                        command.Parameters.Add(parameter);
                    }
                }



                //log sql and execute query
                string paramTexts = " Parameters: "; foreach (MySqlParameter p in command.Parameters) { paramTexts += p.Value + ", "; } log.Debug(command.CommandText + paramTexts);
                reader = command.ExecuteReader();

                #region Hydrate properties
                while (reader.Read()) {
                    object newObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                        if (null != field) {
                            try {
                                field.SetValue(newObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType.ToString() + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(newObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }

                    }
                    listObjects.Add(newObject);
                }
                #endregion

                log.Debug("GetByProperty hentede " + listObjects.Count + " objekter af typen " + objType.ToString());
            } catch (Exception exception) {
                log.Error("Fejl ved GetByProperty for " + objType.ToString(), exception);
            } finally {
                reader.Close();
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return listObjects;
        }
        public IList GetByCustomSQL(Type objType, string strSQL) {
            ArrayList listObjects = new ArrayList();
            string strObject;
            string strStoredProc;
            MySqlCommand command;
            MySqlDataReader reader = null;

            try {
                //get the object name
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_SelectByCustomSQL";
                string strSql = "";
                try {
                    strSql = DataResource.GetString(strStoredProc);
                } catch (Exception exception) {
                    log.Error("Kunne ikke finde SQL-streng for " + strStoredProc + " i resource-filen. Aborting mission!");
                    throw exception;
                }

                #region initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Debug("Had to open connection, even though it was supposed to be open already...");
                }
                #endregion

                command = new MySqlCommand(strSql, connection);
                command.CommandText = command.CommandText.Replace("{CustomSQL}", strSQL);

                //log sql and execute query
                log.Debug(command.CommandText);
                reader = command.ExecuteReader();

                #region Hydrate properties
                while (reader.Read()) {
                    object newObject = objType.GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);
                    for (int intIndex = 0; intIndex < reader.FieldCount; intIndex++) {
                        string strColName = reader.GetName(intIndex);
                        PropertyInfo field = objType.GetProperty(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                        if (null != field) {
                            try {
                                field.SetValue(newObject, GetInternalValue(reader, field.PropertyType.UnderlyingSystemType, intIndex), null);
                            } catch (Exception exception) {
                                log.Error("Unable to hydrate " + field.PropertyType.UnderlyingSystemType.ToString() + " field: " + field.Name + " from column " + strColName, exception);
                            }
                        } else {
                            // No property found..
                            //Try non-property..
                            FieldInfo xfield = objType.GetField(strColName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                xfield.SetValue(newObject, GetInternalValue(reader, xfield.GetType(), intIndex));
                            } else {
                                log.Error("Not hydrating property/field from column " + strColName + " (unable to find field)");
                            }
                        }

                    }
                    listObjects.Add(newObject);
                }
                #endregion

                log.Debug("GetByProperty hentede " + listObjects.Count + " objekter af typen " + objType.ToString());
            } catch (Exception exception) {
                log.Error("Fejl ved GetByProperty for " + objType.ToString(), exception);
            } finally {
                reader.Close();
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return listObjects;
        }
        #endregion

        /// <summary>
        /// Updates the database to reflect object changes..
        /// </summary>
        /// <param name="objSource">The object to persist</param>
        /// <returns></returns>
        public virtual bool Update(object objSource) {

            Type objType = objSource.GetType();
            bool boolStatus = false;
            string strObject;
            string strStoredProc;
            MySqlParameter parameter;
            MySqlCommand command;

            try {
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_Update";

                #region initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Info("Had to open connection, even though it was supposed to be open already...");
                }
                command = new MySqlCommand(DataResource.GetString(strStoredProc), connection);
                #endregion


                ArrayList fieldTypes = new ArrayList();
                ArrayList fieldValues = new ArrayList();
                ArrayList fieldNames = GetColumnNames(objType);

                // Populate the column names..
                string strTmpCmdTxt = "";
                string strRemoveTxt = "";
                foreach (string fName in fieldNames) {
                    if (strObject + "ID" != fName) {
                        strTmpCmdTxt += fName + " = ?" + fName + ", ";

                        // populate the type and value list
                        PropertyInfo pInfo = objType.GetProperty(fName);
                        if (null != pInfo) {
                            MySqlDbType t = GetMySqlType(pInfo.PropertyType);
                            fieldTypes.Add(t);
                            fieldValues.Add(getConvertedValue(pInfo.GetValue(objSource, null), t));
                        } else {
                            //try as a field..
                            FieldInfo xfield = objType.GetField(fName, BindingFlags.Instance |
                                     BindingFlags.Public |
                                     BindingFlags.NonPublic |
                                     BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                MySqlDbType t = GetMySqlType(xfield.FieldType);
                                fieldTypes.Add(t);
                                fieldValues.Add(getConvertedValue(xfield.GetValue(objSource), t));
                            } else {
                                //Unable to find the field..
                                log.Error("Not updating column: " + fName + " (unable to find it on object)");
                            }
                        }
                    } else {
                        strRemoveTxt = fName;
                    }
                }
                fieldNames.Remove(strRemoveTxt);
                strTmpCmdTxt = "SET " + strTmpCmdTxt.Substring(0, strTmpCmdTxt.Length - 2);
                command.CommandText = command.CommandText.Replace("{ColumnData}", strTmpCmdTxt);

                log.Debug("Commandtext = " + command.CommandText);
                

                //add the parameters using field names and types
                for (int intIndex = 0; intIndex < fieldValues.Count; intIndex++) {
                    parameter = new MySqlParameter((string)fieldNames[intIndex], (MySqlDbType)fieldTypes[intIndex]);
                    parameter.Direction = ParameterDirection.Input;
                    parameter.Value = fieldValues[intIndex];
                    command.Parameters.Insert(command.Parameters.Count, parameter);
                }

                //add the original id parameter
                parameter = new MySqlParameter("ID", MySqlDbType.Int64);
                parameter.Direction = ParameterDirection.Input;
                PropertyInfo field = objType.GetProperty(strObject + "ID");
                object objID = field.GetValue(objSource, null);
                parameter.Value = objID;
                command.Parameters.Insert(command.Parameters.Count, parameter);

                //execute query
                log.Debug("Update SQL: " + command.CommandText);
                int resCount = command.ExecuteNonQuery();
                if (resCount <= 1) {
                    log.Debug("Successfully updated object of type " + strObject + " with id = " + objID.ToString());
                } else {
                    log.Error("Update returned unexpected rowcount: " + resCount.ToString() + " (object of type: " + strObject + " with id = " + objID.ToString());
                    throw new Exception("Unexpected rowcount affected by update (" + resCount.ToString() + ")");
                }

            } catch (SqlException exception) {
                log.Error("SQL-Fejl ved update for " + objType.ToString(), exception);
                throw exception;
            } catch (Exception exception) {
                log.Error("Fejl ved update for " + objType.ToString(), exception);
                throw exception;
            } finally {
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }

            return boolStatus;
        }

        /// <summary>
        /// Either creates a new record in the db or updates the existing if it exists
        /// </summary>
        /// <param name="objSource">The object to persist</param>
        /// <returns></returns>
        public virtual bool InsertOrUpdate(object objSource) {
            bool returnStatus = false;
            Type objType = objSource.GetType();
            string strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

            PropertyInfo pi = objSource.GetType().GetProperty(strObject + "ID");
            if (null != pi) {
                if (Convert.ToInt64(pi.GetValue(objSource, null)) > 0) {
                    returnStatus = Update(objSource);
                } else {
                    returnStatus = Insert(objSource);
                }
            } else {
                Exception exc = new Exception("InsertOrUpdate will only work with objects, that conform to having an ID named: <type>ID (ie. PageID)");
                log.Error("Aborting insert (non-saveable object)", exc);
                throw exc;
            }
            return returnStatus;
        }
        
        /// <summary>
        /// Create a new record in the db
        /// </summary>
        /// <param name="objSource">The object to persist</param>
        /// <returns></returns>
        public virtual bool Insert(object objSource) {
            bool boolStatus = false;
            Type objType = objSource.GetType();
            string strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

            #region check that objSource's ID is pure.. (otherwise an update should be issued)
            PropertyInfo pi = objSource.GetType().GetProperty(strObject + "ID");
            if (null != pi) {
                if (Convert.ToInt64(pi.GetValue(objSource, null)) > 0) {
                    Exception exc = new Exception("Insert will only work with fresh objects, not objects that already have an ID.. May I suggest using 'Update' instead ;-)");
                    log.Error("Aborting insert (object id is already > 0)", exc);
                    throw exc;
                }
            } else {
                Exception exc = new Exception("Insert will only work with objects, that conform to having an ID named: <type>ID (ie. PageID)");
                log.Error("Aborting insert (non-saveable object)", exc);
                throw exc;
            }
            #endregion

            #region initialize the command and connection
            MySqlParameter parameter;
            MySqlCommand command;
            MySqlDataReader reader = null;

            if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                connection.ConnectionString = DataResource.ConnectionString;
                connection.Open();
            } else if (connection.State != ConnectionState.Open) {
                connection.ConnectionString = DataResource.ConnectionString;
                connection.Open();
                log.Info("Had to open connection, even though it was supposed to be open already...");
            }
            #endregion

            string strStoredProc;
            try {

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_Insert";

                command = new MySqlCommand(DataResource.GetString(strStoredProc), connection);

                ArrayList fieldTypes = new ArrayList();
                ArrayList fieldValues = new ArrayList();
                ArrayList fieldNames = GetColumnNames(objType);

                // Populate the column names..
                string strTmpColumnTxt = "";
                string strTmpValueTxt = "";
                string strRemoveItem = "";
                //int debugCount = 0;
                foreach (string fName in fieldNames) {
                    //if (debugCount++ > 2) break;
                    if (strObject + "ID" != fName) {
                        strTmpColumnTxt += fName + ", ";
                        strTmpValueTxt += "?" + fName + ", ";

                        // populate the type and value list

                        PropertyInfo pInfo = objType.GetProperty(fName);
                        if (null != pInfo) {
                            MySqlDbType t = GetMySqlType(pInfo.PropertyType);
                            fieldTypes.Add(t);
                            fieldValues.Add(getConvertedValue(pInfo.GetValue(objSource, null), t));
                        } else {
                            //try as a field..
                            FieldInfo xfield = objType.GetField(fName, BindingFlags.Instance |
                                     BindingFlags.Public |
                                     BindingFlags.NonPublic |
                                     BindingFlags.IgnoreCase);
                            if (null != xfield) {
                                MySqlDbType t = GetMySqlType(xfield.FieldType);
                                fieldTypes.Add(t);
                                fieldValues.Add(getConvertedValue(xfield.GetValue(objSource), t));
                            } else {
                                //Unable to find the field..
                                log.Error("Not updating column: " + fName + " (unable to find it on object)");
                            }
                        }
                    } else {
                        strRemoveItem = fName; // This is the ID field
                    }
                }

                fieldNames.Remove(strRemoveItem);

                strTmpColumnTxt = strTmpColumnTxt.Substring(0, strTmpColumnTxt.Length - 2);
                strTmpValueTxt = strTmpValueTxt.Substring(0, strTmpValueTxt.Length - 2);
                command.CommandText = command.CommandText.Replace("{ColumnData}", strTmpColumnTxt);
                command.CommandText = command.CommandText.Replace("{ValueData}", strTmpValueTxt);

                log.Debug("CommandText = " + command.CommandText);

                //add the parameters using field names and types
                for (int intIndex = 0; intIndex < fieldValues.Count; intIndex++) {
                    parameter = new MySqlParameter((string)fieldNames[intIndex], (MySqlDbType)fieldTypes[intIndex]);
                    parameter.Direction = ParameterDirection.Input;
                    parameter.Value = fieldValues[intIndex];
                    command.Parameters.Add(parameter);
                }

                //execute query
                command.Prepare();
                log.Debug("SQL: " + command.CommandText);
                int resCount = command.ExecuteNonQuery();
                if (resCount != 1) {
                    //This could be a real problem..
                    log.Error("Insert got an unexpected row count (" + resCount.ToString() + ")");
                    throw new Exception("Insert got an unexpected row count (" + resCount.ToString() + ")");
                }

                // automatically set the auto incremented id
                command.Parameters.Clear();
                command.CommandText = "SELECT LAST_INSERT_ID();";
                // open the connection and execute query
                reader = command.ExecuteReader();

                object objID = 0;
                if (reader.Read()) {
                    PropertyInfo field = objType.GetProperty(strObject + "ID");
                    objID = reader.GetValue(0);
                    //The system supports both int32 and int63 as IDs..
                    if (field.GetType() == typeof(Int64)) {
                        field.SetValue(objSource, Convert.ToInt64(objID), null);
                    } else {
                        field.SetValue(objSource, Convert.ToInt32(objID), null);
                    }
                    boolStatus = true;
                }

                log.Debug("Inserted object of type " + strObject + " (new ID = " + objID.ToString() + ")");
                // always call Close when done reading.
                reader.Close();

            } catch (MySqlException exception) {
                log.Error("MySql-Fejl ved Insert for " + objType.ToString(), exception);
            } catch (Exception exception) {
                log.Error("Fejl ved Insert for " + objType.ToString(), exception);
            } finally {
                #region Handle close..
                if (reader != null) {
                    if (reader.IsClosed == false) {
                        reader.Close();
                    }
                }
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
                #endregion
            }

            return boolStatus;
        }

        /// <summary>
        /// Deletes the associated record from the database
        /// </summary>
        /// <param name="objSource">Object to remove from db</param>
        /// <returns></returns>
        public bool Delete(object objSource) {
            Type objType = objSource.GetType();
            string strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);
            PropertyInfo field = objType.GetProperty(strObject + "ID");
            object objID = field.GetValue(objSource, null);
            return Delete(objType, objID);
        }
        /// <summary>
        /// Deletes the associated record from the database
        /// </summary>
        /// <param name="objType">The object type (=table name)</param>
        /// <param name="objID">ID of the object</param>
        /// <returns></returns>
        public bool Delete(Type objType, object objID) {
            bool boolStatus = false;
            string strObject;
            string strStoredProc;
            MySqlParameter parameter;
            MySqlCommand command;

            try {
                strObject = objType.FullName.Substring(objType.FullName.LastIndexOf(".") + 1);

                //get the stored procedure name
                strStoredProc = strObject;
                strStoredProc += "_Delete";

                // initialize the command and connection
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Info("Had to open connection, even though it was supposed to be open already...");
                }
                command = new MySqlCommand(DataResource.GetString(strStoredProc), connection);

                //UNUSED? PropertyInfo[] fields = objType.GetProperties();
                //add the original id parameter
                parameter = new MySqlParameter("ID", MySqlDbType.Int64);
                parameter.Direction = ParameterDirection.Input;
                parameter.Value = objID;
                command.Parameters.Insert(command.Parameters.Count, parameter);

                //execute query
                int resCount = command.ExecuteNonQuery();
                if (resCount > 1) {
                    log.Error("Wrong number of rows affected (" + resCount.ToString() + ") expected only 1 or 0!");
                } else {
                    log.Debug("Slettede objekt af typen " + strObject + " med ID = " + objID.ToString());
                    boolStatus = true;
                }

            } catch (SqlException exception) {
                log.Error("SQL-Fejl ved delete for " + objType.ToString(), exception);
            } catch (Exception exception) {
                log.Error("Fejl ved delete for " + objType.ToString(), exception);
            } finally {
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }
            return boolStatus;
        }

        /// <summary>
        /// Gives you a direct handle to execute SQL against the db..
        /// </summary>
        /// <param name="strSql"></param>
        /// <returns></returns>
        public int ExecuteCustomSQL(string strSql) {
            MySqlCommand command;
            int resCount = 0;
            try {

                #region init connection and cmd
                if (!manuallyOpenedConnection && connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                } else if (connection.State != ConnectionState.Open) {
                    connection.ConnectionString = DataResource.ConnectionString;
                    connection.Open();
                    log.Info("Had to open connection, even though it was supposed to be open already...");
                }
                #endregion
                command = new MySqlCommand(strSql, connection);
                log.Debug("ExecuteCustomSQL is about to execute SQL: " + strSql);

                //execute query
                resCount = command.ExecuteNonQuery();

            } catch (SqlException exception) {
                log.Error("SQL-Fejl ved ExecuteCustomSQL for " + strSql, exception);
            } catch (Exception exception) {
                log.Error("Fejl ved ExecuteCustomSQL for " + strSql, exception);
            } finally {
                if (connection.State != ConnectionState.Closed && !manuallyOpenedConnection) {
                    // always call Close when done reading (if we opened the conn. ourselves).
                    connection.Close();
                }
            }
            return resCount;
        }


    }


}
