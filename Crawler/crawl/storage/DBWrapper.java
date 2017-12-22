package edu.upenn.cis.cis455.storage;

import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;

import java.io.File;
import java.io.IOException;

public class DBWrapper {
	
	private static String envDirectory = null;
	
	private static Environment myEnv;
	private static EntityStore store;
	
    private static boolean readOnly = false;
	
    public static void setup(String path) {
    	if (path == null) {
    		path = "../tmp/database/flyingmantis";
    	}
    	envDirectory = path; // envDir;
        File file = new File(envDirectory);
        file.mkdirs();
        
        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
        StoreConfig storeConfig = new StoreConfig();
        myEnvConfig.setAllowCreate(!readOnly);
        storeConfig.setAllowCreate(!readOnly);
        
        // Open the environment and entity store
        if (myEnv == null) myEnv = new Environment(file, myEnvConfig);
        if (store == null) store = new EntityStore(myEnv, "EntityStore", storeConfig);
    }
    
    public static Environment getEnv(String path) throws IOException {
    	if (myEnv == null) 
    		setup("export/bdbEnv");
    	return myEnv;
    }
    
    public static EntityStore getEntityStore(String path) {
    	if (store == null)
    		setup(path);
    	return store;
    }
    
    public static String getPath() {
    	return envDirectory;
    }

    public static void closeEnvDB() {
    	if (store != null) {
    		try {
    			store.close();
    		} catch(DatabaseException dbe) {
    			System.exit(-1);
    		} finally {
    			store = null;
    		}
		}
    	if (myEnv != null) {
    		try {
    			// Finally, close environment.
    			myEnv.close();
    		} catch(DatabaseException dbe) {
    			System.exit(-1);
    		} finally {
    			myEnv = null;
    		}
    	} 
    }
}
