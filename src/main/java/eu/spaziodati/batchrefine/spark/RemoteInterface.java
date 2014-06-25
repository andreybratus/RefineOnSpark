package eu.spaziodati.batchrefine.spark;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteInterface extends Remote {

	double doMain(String[] args) throws RemoteException, Exception;

}
