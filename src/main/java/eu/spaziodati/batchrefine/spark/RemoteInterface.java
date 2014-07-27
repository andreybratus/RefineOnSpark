package eu.spaziodati.batchrefine.spark;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * RMI interface used to communicate with <i>RefineOnSpark</i> by submitting job
 * parameters in {@code String} array
 * 
 * @author andrey
 */

public interface RemoteInterface extends Remote {

	String submitJob(String[] args) throws RemoteException, Exception;

}
