/*
 * Copyright (c) 2014, Dimitar Misev
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * 
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.asqldb.ras;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import org.asqldb.types.MDADimensionType;
import org.asqldb.types.MDADomainType;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FrameworkLogger;
import org.odmg.DBag;
import org.odmg.Database;
import org.odmg.ODMGException;
import org.odmg.OQLQuery;
import org.odmg.QueryException;
import org.odmg.Transaction;
import rasj.RasClientInternalException;
import rasj.RasConnectionFailedException;
import rasj.RasGMArray;
import rasj.RasImplementation;
import rasj.RasIndexOutOfBoundsException;
import rasj.RasMArrayByte;
import rasj.RasMInterval;
import rasj.RasSInterval;

/**
 * Rasdaman utility classes - execute queries, etc.
 *
 * @author Dimitar Misev
 * @author Johannes Bachhuber
 */
public class RasUtil {

    private static final FrameworkLogger log = FrameworkLogger.getLog(RasUtil.class);
    public static boolean printLog = false;

    private static final String DEFAULT_SERVER = "127.0.0.1";
    private static final String DEFAULT_BASE = "RASBASE";
    private static final String DEFAULT_PORT = "7001";
    private static final String DEFAULT_USER = "rasguest";
    private static final String DEFAULT_PASSWD = "rasguest";
    private static final String DEFAULT_ADMIN_USER = "rasadmin";
    private static final String DEFAULT_ADMIN_PASSWD = "rasadmin";

    private static final int MDA_MAX_ATTEMPTS = 5;
    private static final int MDA_TIMEOUT = 1000;

    private static final String HOME_DIR = System.getProperty("user.home");
    private static final String CONFIG_FILE_NAME = ".asqldb.properties";
    private static final String CONFIG_FILE = HOME_DIR + File.separator + CONFIG_FILE_NAME;

    private static PrintStream queryOutputStream = System.out;

    private static Database db = null;
    private static RasImplementation rasImplementation = null;

    private static String server;
    private static String database;
    private static String port;

    public static String username;
    public static String password;
    public static String adminUsername;
    public static String adminPassword;

    static {
        loadProperties();
    }

    private static void loadProperties() {
        InputStream input = null;
        try {
            input = loadPropertiesFromFile(CONFIG_FILE);
        } catch (Exception ex) {
            log.warning("Failed loading configuration file " + CONFIG_FILE + ", using default values.", ex);
            loadDefaultProperties();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private static InputStream loadPropertiesFromFile(String configFile) throws FileNotFoundException, IOException {
        final InputStream input = new FileInputStream(configFile);
        final Properties prop = new Properties();
        prop.load(input);

        server = prop.getProperty("ras.server", DEFAULT_SERVER);
        database = prop.getProperty("ras.database", DEFAULT_BASE);
        port = prop.getProperty("ras.port", DEFAULT_PORT);
        username = prop.getProperty("ras.username", DEFAULT_USER);
        password = prop.getProperty("ras.password", DEFAULT_PASSWD);
        adminUsername = prop.getProperty("ras.admin.username", DEFAULT_ADMIN_USER);
        adminPassword = prop.getProperty("ras.admin.password", DEFAULT_ADMIN_PASSWD);
        return input;
    }

    private static void loadDefaultProperties() {
        server = DEFAULT_SERVER;
        database = DEFAULT_BASE;
        port = DEFAULT_PORT;
        username = DEFAULT_USER;
        password = DEFAULT_PASSWD;
        adminUsername = DEFAULT_ADMIN_USER;
        adminPassword = DEFAULT_ADMIN_PASSWD;
    }

    /**
     * Executes an Hsql multidimensional array query.
     * @param selector Selector string with rasql expressions
     * @param rasArrayIds Varargs list of RasArrayIds to select from
     * @return String array with one element containing the path to the array file.
     * @throws HsqlException If any error occurs processing the query, this exception is thrown.
     */
    public static Object executeHsqlArrayQuery(String selector, RasArrayId... rasArrayIds) throws HsqlException {
        return executeHsqlArrayQuery(selector, new RasArrayIdSet(Arrays.asList(rasArrayIds)));
    }

    /**
     * Executes an Hsql multidimensional array query.
     * @param selector Selector string with rasql expressions
     * @param rasArrayIds Set of RasArrayIds to select from
     * @return String array with one element containing the path to the array file.
     * @throws HsqlException If any error occurs processing the query, this exception is thrown.
     */
    public static Object executeHsqlArrayQuery(String selector, RasArrayIdSet rasArrayIds) throws HsqlException {
        return executeHsqlArrayQuery(selector, ".array", rasArrayIds);
    }

    /**
     * Executes an Hsql multidimensional array query.
     * @param selector Selector string with rasql expressions
     * @param extension extension for the output file(s)
     * @param rasArrayIds Set of RasArrayIds to select from
     * @return String array with one element containing the path to the array file.
     * @throws HsqlException If any error occurs processing the query, this exception is thrown.
     */
    public static Object executeHsqlArrayQuery(final String selector, final String extension, final RasArrayIdSet rasArrayIds) throws HsqlException {
        final String query;
        if (rasArrayIds.isEmpty()) {
            query = String.format("SELECT "+ selector);
        } else {
            query = String.format("SELECT %s FROM %s WHERE %s", selector, rasArrayIds.stringifyRasColls(), rasArrayIds.stringifyOids());
        }
        if(printLog) queryOutputStream.println(query);

        //important: rasdaman database is close in the session.execute method,
        //aka after the entire query is executed
        DBag result = (DBag) executeRasqlQuery(query, false, false);

        final Iterator it = result.iterator();
        if (!(it.hasNext()))
            throw Error.error(ErrorCode.MDA_OIDNOTFOUND, query);

        final Object obj = it.next();

//        if ((obj instanceof RasGMArray)) {
//            RasGMArray arr = (RasGMArray) obj;
//            final String filename = rasArrayIds.stringifyIdentifier() + extension;
//            writeToFile(arr, filename);
//        }
        //result is a scalar
        return obj;
    }

    private static void writeToFile(final RasGMArray arr, final String filename) throws HsqlException {
        byte dataToWrite[] = arr.getArray();
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(filename);
            out.write(dataToWrite);
        } catch (FileNotFoundException ex) {
            throw Error.error(ex, ErrorCode.MDA_IOERROR, filename);
        } catch (IOException ex) {
            throw Error.error(ex, ErrorCode.MDA_IOERROR, "write error");
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ex) {
                    //noinspection ThrowFromFinallyBlock
                    throw Error.error(ex, ErrorCode.MDA_IOERROR, "close error");
                }
            }
        }
    }

    public static void openDatabase(final String username, final String password,
                                    final boolean writeAccess) throws HsqlException{
        if (db!= null)
            closeDatabase();
        rasImplementation = new RasImplementation("http://"+ server +":"+ port);
        rasImplementation.setUserIdentification(username, password);


        db = rasImplementation.newDatabase();
        int attempts = 0;

        //Try to connect until the maximum number of attempts is reached
        //This loop handles connection attempts to a saturated rasdaman
        //complex which will refuse the connection until a server becomes
        //available.
        boolean dbOpened = false;

        while(!dbOpened) {

            //Try to obtain a free rasdaman server
            try {
                if(printLog) log.finer("Opening database ...");
                db.open(database, writeAccess ? Database.OPEN_READ_WRITE : Database.OPEN_READ_ONLY);
                dbOpened = true;
            } catch(RasConnectionFailedException ex) {

                //A connection with a Rasdaman server could not be established
                //retry shortly unless connection attempts exceeded the maximum
                //possible connection attempts.
                attempts++;
                dbOpened = false;
                if(!(attempts < MDA_MAX_ATTEMPTS))
                    //Throw a RasConnectionFailedException if the connection
                    //attempts exceeds the maximum connection attempts.
                    throw Error.error(ex, ErrorCode.MDA_UNAVAILABLE, attempts+" attempts");

                //Sleep before trying to open another connection
                try {
                    Thread.sleep(MDA_TIMEOUT);
                } catch(InterruptedException e) {
                    if(printLog) log.error("Thread " + Thread.currentThread().getName() +
                            " was interrupted while searching a free server.");
                    throw Error.error(ex, ErrorCode.MDA_UNAVAILABLE, attempts+" attempts");
                }
            } catch(ODMGException ex) {

                //The maximum amount of connection attempts was exceeded
                //and a connection could not be established. Return
                //an exception indicating Rasdaman is unavailable.

                if(printLog) log.info("A Rasdaman request could not be fulfilled since no "+
                        "free Rasdaman server were available. Consider adjusting "+
                        "the values of rasdaman_retry_attempts and rasdaman_retry_timeout "+
                        "or adding more Rasdaman servers.",ex);
                throw Error.error(ex, ErrorCode.MDA_UNAVAILABLE, attempts+" attempts");
            } catch (RasClientInternalException ex) {
                //A connection with a Rasdaman server could not be established
                //retry shortly unless connection attempts exceeded the maximum
                //possible connection attempts.
                System.out.println("WARNING: internal ras client exception..., "+attempts+" attempts");
                attempts++;
                dbOpened = false;
                if(!(attempts < MDA_MAX_ATTEMPTS))
                    //Throw a RasConnectionFailedException if the connection
                    //attempts exceeds the maximum connection attempts.
                {
                    throw Error.error(ex, ErrorCode.MDA_UNAVAILABLE, attempts + " attempts");
                }

                //Sleep before trying to open another connection
                try {
                    Thread.sleep(MDA_TIMEOUT);
                } catch(InterruptedException e) {
                    if(printLog) log.error("Thread " + Thread.currentThread().getName() +
                            " was interrupted while searching a free server.");
                    throw Error.error(ex, ErrorCode.MDA_UNAVAILABLE, attempts+" attempts");
                }
            }
        }
    }

    public static void closeDatabase() throws HsqlException {
        try {
            if(printLog) log.finer("Closing database ...");
            if (db != null)
                db.close();
            else
                System.out.println("Db was already closed.");
        } catch (final Exception ex) {
            if(printLog) log.info("Error closing database connection: ", ex);
            throw Error.error(ex, ErrorCode.MDA_CONNECTION, "Count not close database");
        }
        rasImplementation = null;
        db = null;
    }

    /**
     * Execute a RasQL query with specified credentials.
     *
     * @param query The rasql query string.
     * @param ignoreFailedQuery if true, a failed query will be silently ignored
     * @return result object.
     * @throws org.hsqldb.HsqlException
     */
    public static Object executeRasqlQuery(final String query, boolean ignoreFailedQuery) throws HsqlException {
        return executeRasqlQuery(query, ignoreFailedQuery, false);
    }

    /**
     * Execute a RasQL query with specified credentials.
     *
     * @param query The rasql query string.
     * @param ignoreFailedQuery if true, a failed query will be silently ignored
     * @param writeAccess open database with write access
     * @return result object.
     * @throws org.hsqldb.HsqlException
     */
    public static Object executeRasqlQuery(final String query, boolean ignoreFailedQuery, boolean writeAccess) throws HsqlException {
        return executeRasqlQuery(query, ignoreFailedQuery, writeAccess, null);
    }

    /**
     * Execute a RasQL query with specified credentials.
     *
     * @param query The rasql query string.
     * @param ignoreFailedQuery if true, a failed query will be silently ignored
     * @param writeAccess open database with write access
     * @return result object.
     * @throws org.hsqldb.HsqlException
     */
    public static Object executeRasqlQuery(final String query, boolean ignoreFailedQuery, boolean writeAccess, Object data) throws HsqlException {
        String user = username;
        String pass = password;
        if (writeAccess) {
            user = adminUsername;
            pass = adminPassword;
        }
        if (rasImplementation == null || db == null)
            openDatabase(user, pass, writeAccess);

        //The result of the query will be assigned to ret
        //Should always return a result (empty result possible)
        //since a RasdamanException will be thrown in case of error
        Object ret=null;

        Transaction tr;

        if(printLog) log.finer("Starting transaction ...");
        tr = rasImplementation.newTransaction();
        tr.begin();

        if(printLog) log.finer("Instantiating query ...");
        OQLQuery q = rasImplementation.newOQLQuery();

        //A free rasdaman server was obtained, executing query
        try {
            q.create(query);
            if (data != null) {
                q.bind(data);
            }
            
            if(printLog) log.finer("Executing query "+ query);
            ret = q.execute();

            if(printLog) log.finer("Committing transaction ...");
            tr.commit();
        } catch (QueryException ex) {
            //Executing a rasdaman query failed
            tr.abort();
            if (!ignoreFailedQuery)
                throw Error.error(ex, ErrorCode.MDA_QUERY, query);
        } catch (java.lang.Error ex) {
            tr.abort();
            throw Error.error(ErrorCode.MDA_OVERLOAD, query);
        } catch(NullPointerException ex) {
            //there is a rasj bug that throws a NullPointerException for queries that retrieve scalars
            tr.abort();
            throw Error.error(ErrorCode.MDA_RASJ_BUG, query);
        }
        return ret;
    }
    
    public static RasGMArray convertBlobToArray(byte[] data) {
        RasMInterval domain = new RasMInterval(1);
        try {
            domain.setItem(0, new RasSInterval(0, data.length - 1));
        } catch (Exception ex) {
            throw Error.error(ErrorCode.MDA_INVALID_PARAMETER, ex);
        }
        RasGMArray ret = new RasGMArray(domain, 1);
        ret.setArray(data);
        ret.setObjectTypeName("GreyString");
        return ret;
    }
    
    /**
     * Convert rasj DBag of char arrays to a set of Strings
     */
    public static Set<String> dbagArrayToSetString(Object dbag) {
        Set<String> ret = new HashSet<String>();
        if (dbag != null) {
            Iterator it = ((DBag) dbag).iterator();
            while (it.hasNext()) {
                RasGMArray rasArray = (RasGMArray) it.next();
                String javaVal = new String(rasArray.getArray());
                javaVal = javaVal.trim();
                ret.add(javaVal);
            }
        }
        return ret;
    }
    
    /**
     * @return the first OID in a set of OIDs. The returned OID is -1 if no
     * oid is returned.
     */
    public static Integer dbagToOid(Object dbag) {
        Integer ret = -1;
        if (dbag != null) {
            Iterator it = ((DBag) dbag).iterator();
            while (it.hasNext()) {
                Object o = it.next();
                if (o instanceof Integer) {
                    ret = (Integer) o;
                    break;
                }
            }
        }
        return ret;
    }
    
    /**
     * @return true if coll exists, false otherwise.
     */
    public static boolean collectionExists(String coll) {
        Set<String> colls = RasUtil.dbagArrayToSetString(
                RasUtil.executeRasqlQuery("select c from RAS_COLLECTIONNAMES as c", false, true));
        return colls.contains(coll);
    }
    
    /**
     * @param table ASQLDB table name
     * @param field column name
     * @return collection contents as csv, or null in case of an error
     */
    public static String collectionAsCsv(String table, String field) {
        return collectionAsFunction(table, field, "csv");
    }
    
    /**
     * @param table ASQLDB table name
     * @param field column name
     * @return collection contents as sdom, or null in case of an error
     */
    public static String collectionAsSdom(String table, String field) {
        return collectionAsFunction(table, field, "sdom");
    }
    
    /**
     * @param table ASQLDB table name
     * @param field column name
     * @param func rasql function to apply to the given collection
     * @return collection contents as string, or null in case of an error
     */
    public static String collectionAsFunction(String table, String field, String func) {
        String ret = null;
        String coll = "PUBLIC_" + table.toUpperCase() + "_" + field.toUpperCase();
        Object res = RasUtil.executeRasqlQuery("select " + func + "(c) from " + coll + " as c", true);
        if (res instanceof DBag) {
            DBag b = (DBag) res;
            Iterator it = b.iterator();
            Object o = it.next();
            if (o instanceof RasMArrayByte) {
                RasMArrayByte m = (RasMArrayByte) o;
                ret = new String(m.getArray());
            } else {
                ret = o.toString();
            }
        }
        return ret;
    }

    /**
     * Type cast utility used to conserve data types within JDBC/HSQLDB.
     * Unwraps a String from an Object array.
     * @param object The Object array to be cast to a String
     * @return the corresponding String
     */
    public static String objectArrayToString(Object object) {
        return (String) ((Object[]) object)[0];
    }

    /**
     * Type cast utility used to conserve data types within JDBC/HSQLDB.
     * Wraps a string in an Object array.
     * @param str the String to be wrapped in an Object array
     * @return an Object array wrapped the string
     */
    public static Object[] stringToObjectArray(String str) {
        return new Object[]{str};
    }

    public static void setQueryOutputStream(PrintStream newStream) {
        queryOutputStream = newStream;
    }
    
    /**
     * @return the first element of a DBag, or null if bag is empty or not a DBag.
     */
    public static Object head(Object bag) {
        Object ret = null;
        if (bag instanceof DBag) {
            DBag dbag = (DBag) bag;
            Iterator it = dbag.iterator();
            if (it.hasNext()) {
                ret = it.next();
            }
        }
        return ret;
    }
    
    /**
     * Convert a rasdaman minterval to an array, taking dimension names
     * from type into account.
     * @return an array representing the rasql interval
     */
    public static Object[] toArray(RasMInterval sdom, MDADomainType type) {
        Object[] ret = new Object[sdom.dimension()];
        for (int i = 0; i < ret.length; i++) {
            MDADimensionType hdim = type.getDimension(i);
            try {
                RasSInterval rdim = sdom.item(i);
                ret[i] = new Object[]{hdim.getDimensionName(), rdim.low(), rdim.high()};
            } catch (RasIndexOutOfBoundsException ex) {
            }
        }
        return ret;
    }
}
