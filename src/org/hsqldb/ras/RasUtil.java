/*
 * This file is part of rasdaman community.
 *
 * Rasdaman community is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Rasdaman community is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU  General Public License for more details.
 *
 * You should have received a copy of the GNU  General Public License
 * along with rasdaman community.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright 2003 - 2014 Peter Baumann / rasdaman GmbH.
 *
 * For more information please see <http://www.rasdaman.org>
 * or contact Peter Baumann via <baumann@rasdaman.com>.
 */

package org.hsqldb.ras;

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
import rasj.RasConnectionFailedException;
import rasj.RasGMArray;
import rasj.RasImplementation;

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

/**
 * Rasdaman utility classes - execute queries, etc.
 *
 * @author <a href="mailto:d.misev@jacobs-university.de">Dimitar Misev</a>
 * @author Johannes Bachhuber
 */
public class RasUtil {

    private static final String DEFAULT_SERVER = "127.0.0.1";
    private static final String DEFAULT_BASE = "RASBASE";
    private static final String DEFAULT_PORT = "7001";
    private static final String DEFAULT_USER = "rasguest";
    private static final String DEFAULT_PASSWD = "rasguest";
    private static final String DEFAULT_ADMIN_USER = "rasadmin";
    private static final String DEFAULT_ADMIN_PASSWD = "rasadmin";

    private static final int RAS_MAX_ATTEMPTS = 5;
    private static final int RAS_TIMEOUT = 1000;

    private static PrintStream queryOutputStream = System.out;

    private static String server;
    private static String database;
    private static String port;

    public static String username;
    public static String password;
    public static String adminUsername;
    public static String adminPassword;

    public static boolean printLog = true;

    private static FrameworkLogger log = FrameworkLogger.getLog(RasUtil.class);

    static {//load properties from config file

        final Properties prop = new Properties();
        InputStream input = null;

        try {

            input = new FileInputStream("config.properties");
            prop.load(input);

            server = prop.getProperty("ras.server", DEFAULT_SERVER);
            database = prop.getProperty("ras.database", DEFAULT_BASE);
            port = prop.getProperty("ras.port", DEFAULT_PORT);
            username = prop.getProperty("ras.username", DEFAULT_USER);
            password = prop.getProperty("ras.password", DEFAULT_PASSWD);
            adminUsername = prop.getProperty("ras.admin.username", DEFAULT_ADMIN_USER);
            adminPassword = prop.getProperty("ras.admin.password", DEFAULT_ADMIN_PASSWD);

        } catch (IOException ex) {
            System.out.println("RasUtil: Failed to load config file, using default values: " + ex.getMessage());
            server = DEFAULT_SERVER;
            database = DEFAULT_BASE;
            port = DEFAULT_PORT;
            username = DEFAULT_USER;
            password = DEFAULT_PASSWD;
            adminUsername = DEFAULT_ADMIN_USER;
            adminPassword = DEFAULT_ADMIN_PASSWD;
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Executes an Hsql multidimensional array query.
     * @param selector Selector string with rasql expressions
     * @param rasArrayIds Varargs list of RasArrayIds to select from
     * @return String array with one element containing the path to the array file.
     * @throws HsqlException If any error occurs processing the query, this exception is thrown.
     */
    public static String executeHsqlArrayQuery(String selector, RasArrayId... rasArrayIds) throws HsqlException {
        return executeHsqlArrayQuery(selector, new HashSet<RasArrayId>(Arrays.asList(rasArrayIds)));
    }

    /**
     * Executes an Hsql multidimensional array query.
     * @param selector Selector string with rasql expressions
     * @param rasArrayIds Set of RasArrayIds to select from
     * @return String array with one element containing the path to the array file.
     * @throws HsqlException If any error occurs processing the query, this exception is thrown.
     */
    public static String executeHsqlArrayQuery(String selector, Set<RasArrayId> rasArrayIds) throws HsqlException {
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
    public static String executeHsqlArrayQuery(final String selector, final String extension, final Set<RasArrayId> rasArrayIds) throws HsqlException {
        final String query;
        if (rasArrayIds.isEmpty()) {
            query = String.format("SELECT "+ selector);
        } else {
            query = String.format("SELECT %s FROM %s WHERE %s", selector, RasArrayId.stringifyRasCollections(rasArrayIds), RasArrayId.stringifyOids(rasArrayIds));
        }
        if(printLog) queryOutputStream.println(query);
        DBag result = (DBag) executeRasqlQuery(query, username, password);

        final Iterator it = result.iterator();
        if (!(it.hasNext()))
            throw Error.error(ErrorCode.RAS_OIDNOTFOUND, query);

        final Object obj = it.next();

        //todo: allow returning scalar results
        if ((obj instanceof RasGMArray)) {
            RasGMArray arr = (RasGMArray) obj;
            final String filename = RasArrayId.stringifyIdentifier(rasArrayIds) + arr.spatialDomain() + extension;
            writeToFile(arr, filename);
            return System.getProperty("user.dir")+System.getProperty("file.separator")+filename;
        }
        //result is a scalar:

        return obj.toString();
    }

    private static void writeToFile(final RasGMArray arr, final String filename) throws HsqlException {
        byte dataToWrite[] = arr.getArray();
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(filename);
            out.write(dataToWrite);
        } catch (FileNotFoundException ex) {
            throw Error.error(ex, ErrorCode.RAS_IOERROR, filename);
        } catch (IOException ex) {
            throw Error.error(ex, ErrorCode.RAS_IOERROR, "write error");
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ex) {
                    //noinspection ThrowFromFinallyBlock
                    throw Error.error(ex, ErrorCode.RAS_IOERROR, "close error");
                }
            }
        }
    }

    /**
     * Execute a RasQL query with specified credentials.
     * @param query The rasql query string.
     * @param username rasql user
     * @param password rasql user's password
     * @return result object.
     * @throws org.hsqldb.HsqlException
     */
    public static Object executeRasqlQuery(String query, String username, String password) throws HsqlException {

        //todo: make this once per reqest
        RasImplementation impl = new RasImplementation("http://"+ server +":"+ port);
        impl.setUserIdentification(username, password);
        Database db = impl.newDatabase();
        int attempts = 0;

        //The result of the query will be assigned to ret
        //Should always return a result (empty result possible)
        //since a RasdamanException will be thrown in case of error
        Object ret=null;

        Transaction tr;

        //Try to connect until the maximum number of attempts is reached
        //This loop handles connection attempts to a saturated rasdaman
        //complex which will refuse the connection until a server becomes
        //available.
        boolean queryCompleted = false, dbOpened = false;

        while(!queryCompleted) {

            //Try to obtain a free rasdaman server
            try {
                if(printLog) log.finer("Opening database ...");
                db.open(database, username.equals(adminUsername)?Database.OPEN_READ_WRITE:Database.OPEN_READ_ONLY);
                dbOpened = true;

                if(printLog) log.finer("Starting transaction ...");
                tr = impl.newTransaction();
                tr.begin();

                if(printLog) log.finer("Instantiating query ...");
                OQLQuery q = impl.newOQLQuery();

                //A free rasdaman server was obtained, executing query
                try {
                    q.create(query);
                    if(printLog) log.finer("Executing query "+ query);
                    ret = q.execute();

                    if(printLog) log.finer("Committing transaction ...");
                    tr.commit();
                    queryCompleted = true;
                } catch (QueryException ex) {
                    //Executing a rasdaman query failed
                    tr.abort();
                    throw Error.error(ex, ErrorCode.RAS_QUERY, query);
                } catch (java.lang.Error ex) {
                    tr.abort();
                    throw Error.error(ErrorCode.RAS_OVERLOAD, query);
                } catch(NullPointerException ex) {
                    //there is a rasj bug that throws a NullPointerException for queries that retrieve scalars
                    tr.abort();
                    throw Error.error(ErrorCode.RAS_RASJ_BUG, query);
                } finally {

                    //Done connection with rasdaman, closing database.
                    try {
                        if(printLog) log.finer("Closing database ...");
                        db.close();
                    } catch (ODMGException ex) {
                        if(printLog) log.info("Error closing database connection: ", ex);
                    }
                }
            } catch(RasConnectionFailedException ex) {

                //A connection with a Rasdaman server could not be established
                //retry shortly unless connection attempts exceeded the maximum
                //possible connection attempts.
                attempts++;
                if(dbOpened)
                    try {
                        db.close();
                    } catch(ODMGException e) {
                        if(printLog) log.info("Error closing database connection: ", e);
                    }
                dbOpened = false;
                if(!(attempts < RAS_MAX_ATTEMPTS))
                    //Throw a RasConnectionFailedException if the connection
                    //attempts exceeds the maximum connection attempts.
                    throw Error.error(ex, ErrorCode.RAS_UNAVAILABLE, query);

                //Sleep before trying to open another connection
                try {
                    Thread.sleep(RAS_TIMEOUT);
                } catch(InterruptedException e) {
                    if(printLog) log.error("Thread " + Thread.currentThread().getName() +
                            " was interrupted while searching a free server.");
                    throw Error.error(ex, ErrorCode.RAS_UNAVAILABLE, query);
                }
            } catch(ODMGException ex) {

                //The maximum amount of connection attempts was exceeded
                //and a connection could not be established. Return
                //an exception indicating Rasdaman is unavailable.

                if(printLog) log.info("A Rasdaman request could not be fulfilled since no "+
                        "free Rasdaman server were available. Consider adjusting "+
                        "the values of rasdaman_retry_attempts and rasdaman_retry_timeout "+
                        "or adding more Rasdaman servers.",ex);
                throw Error.error(ex, ErrorCode.RAS_UNAVAILABLE, query);
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
}
